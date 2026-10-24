# -*- coding: utf-8 -*-
"""
Auto Clipper — backend (Flask)
Memotong video YouTube menjadi klip Shorts/Reels otomatis.
Pipeline: yt-dlp (info + heatmap + transkrip) -> kandidat momen
(heatmap / Gemini / fallback merata) -> ffmpeg potong + crop rasio.
"""
import json
import os
import re
import shutil
import glob
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request, send_from_directory

APP_VER = "1.7"
BASE_DIR = os.environ.get("CLIPJOBS_DIR", "/tmp/clipjobs")
# Proxy egress untuk YouTube (WARP via wireproxy, disiapkan start.sh).
# Set env CLIP_PROXY="" untuk mematikan.
PROXY_URL = os.environ.get("CLIP_PROXY", "socks5h://127.0.0.1:1080")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash:generateContent"
)

app = Flask(__name__)


@app.after_request
def _cors(resp):
    """Izin CORS untuk frontend (Cloudflare Pages) — tanpa header ini
    browser memblokir semua jawaban backend ("Failed to fetch"),
    walau backend-nya sendiri sehat."""
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp

JOBS = {}
JOBS_LOCK = threading.Lock()


# ---------------------------------------------------------------- util dasar
def new_job():
    job = {
        "id": uuid.uuid4().hex[:12],
        "status": "queued",
        "progress": 0,
        "log": [],
        "clips": [],
        "error": None,
        "created": time.time(),
    }
    with JOBS_LOCK:
        JOBS[job["id"]] = job
    return job


def log(job, msg):
    job["log"].append(msg)
    # batasi panjang log
    if len(job["log"]) > 200:
        job["log"] = job["log"][-200:]


def set_progress(job, pct):
    job["progress"] = max(0, min(100, int(pct)))


def cleanup_old_jobs():
    """Hapus folder job yang berumur > 2 jam (oportunistik)."""
    try:
        now = time.time()
        for name in os.listdir(BASE_DIR):
            path = os.path.join(BASE_DIR, name)
            if os.path.isdir(path) and now - os.path.getmtime(path) > 2 * 3600:
                shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def run_cmd(cmd, timeout=600):
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout
    )


# ---------------------------------------------------------------- yt-dlp
def _cookies_to_netscape(content):
    """YOUTUBE_COOKIES bisa berupa teks Netscape atau JSON (ekspor ekstensi).

    yt-dlp hanya menerima format Netscape, jadi JSON dikonversi di sini.
    """
    text = content.strip()
    if not text.startswith(("[", "{")):
        return text
    try:
        data = json.loads(text)
    except Exception:
        return text
    if isinstance(data, dict):
        data = data.get("cookies", [])
    lines = ["# Netscape HTTP Cookie File"]
    for c in data:
        if not isinstance(c, dict):
            continue
        domain = str(c.get("domain", ""))
        name = str(c.get("name", ""))
        if not domain or not name:
            continue
        flag = "TRUE" if domain.startswith(".") else "FALSE"
        path = str(c.get("path", "/"))
        secure = "TRUE" if c.get("secure") else "FALSE"
        exp = c.get("expirationDate", c.get("expiry", 0))
        try:
            exp = int(float(exp))
        except Exception:
            exp = 0
        value = str(c.get("value", ""))
        lines.append("\t".join([domain, flag, path, secure, str(exp),
                                name, value]))
    return "\n".join(lines) + "\n"


# Host Meta (Facebook/Instagram): kontennya menuntut sesi login, jadi
# jalur ini memakai cookies Facebook yang terpasang di server (env
# FB_COOKIES_FILE menunjuk ke file Netscape) — tanpa cookies, Meta
# menyodorkan halaman kosong (cobalt: fetch.empty; yt-dlp: parse error).
META_HOSTS = ("facebook.com", "fb.watch", "instagram.com")


def is_meta_url(url):
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        return False
    return any(host == h or host.endswith("." + h) for h in META_HOSTS)


# TikTok: video publik kebaca langsung tanpa login (terbukti 24 Okt 2026:
# yt-dlp -J polos lolos, 172 dtk 720x1280) — tanpa cookies, tanpa proxy.
TIKTOK_HOSTS = ("tiktok.com",)


def is_tiktok_url(url):
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        return False
    return any(host == h or host.endswith("." + h) for h in TIKTOK_HOSTS)


def cookies_file_for(url):
    """File cookies per platform Meta: Instagram pakai file sendiri
    (env IG_COOKIES_FILE), Facebook/fb.watch pakai FB_COOKIES_FILE.
    Mengembalikan (path, nama_platform)."""
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        host = ""
    if host == "instagram.com" or host.endswith(".instagram.com"):
        return (os.environ.get("IG_COOKIES_FILE", "").strip(),
                "Instagram")
    return os.environ.get("FB_COOKIES_FILE", "").strip(), "Facebook"


def ytdlp_base_args(job_dir, use_cookies=True, use_proxy=False,
                    cookies_path=None):
    """Argumen dasar yt-dlp.

    - Proxy SOCKS5 (WARP via wireproxy, disiapkan start.sh) hanya
      dipakai bila use_proxy=True — dipakai untuk UNDUHAN saja:
      tahap info justru lolos dari IP Render langsung, sementara
      unduhan googlevideo diharapkan lolos lewat IP WARP (IPv4
      dipaksa, mengikuti setup yang terbukti di laporan riset).
      Kosongkan env CLIP_PROXY untuk mematikan.
    - JS runtime (Deno/Node) + remote component EJS: WAJIB sejak
      yt-dlp modern — tanpa runtime JS, tantangan n-challenge YouTube
      tak terselesaikan dan format video jadi kosong.
    - Cookies dari env YOUTUBE_COOKIES hanya bila use_cookies=True.
    """
    args = [sys.executable, "-m", "yt_dlp", "--no-playlist",
            "--no-warnings",
            "--impersonate", "chrome",
            "--js-runtimes", "deno", "--js-runtimes", "node",
            "--remote-components", "ejs:github"]
    if use_proxy and PROXY_URL:
        args += ["--proxy", PROXY_URL, "--force-ipv4"]
    cookies_content = os.environ.get("YOUTUBE_COOKIES", "").strip()
    if use_cookies and cookies_content:
        cpath = os.path.join(job_dir, "cookies.txt")
        with open(cpath, "w", encoding="utf-8") as f:
            f.write(_cookies_to_netscape(cookies_content))
        args += ["--cookies", cpath]
    if cookies_path:
        args += ["--cookies", cookies_path]
    return args


# Strategi percobaan (klien, pakai_cookies) untuk IP datacenter,
# mengikuti PO Token Guide resmi yt-dlp:
# Fase 1 LOGGED-OUT dulu — mweb + PO Token bgutil adalah resep utama;
#   android_vr/visionos/web_safari dikenal memberi format tanpa PO
#   (web_safari berupa HLS yang tak butuh n-challenge).
# Fase 2 pakai cookies login — klien web & tv (format tv ter-DRM
#   tanpa cookies, jadi fase ini memang butuh cookies).
ATTEMPTS = [
    ("mweb", False),
    ("android_vr", False),
    ("visionos", False),
    ("web_safari", False),
    (None, True),
    ("web_safari", True),
    ("ios_downgraded", True),
    ("web_embedded", True),
    ("tv_downgraded", True),
    ("web_creator", True),
    ("mweb", True),
    ("tv", True),
]


# Jalur Meta (Facebook/Instagram): satu percobaan langsung, tanpa
# extractor-args YouTube dan tanpa proxy — resep jembatan yang terbukti
# (cookies dari file server: FB_COOKIES_FILE untuk Facebook,
# IG_COOKIES_FILE untuk Instagram).
META_ATTEMPTS = [(None, False)]


# Jalur TikTok: satu percobaan langsung, tanpa cookies, tanpa proxy,
# tanpa extractor-args YouTube (terbukti lolos polos 24 Okt 2026).
TIKTOK_ATTEMPTS = [(None, False)]


def _is_bot_error(text):
    t = (text or "").lower()
    return "sign in to confirm" in t or "not a bot" in t


def _is_retryable(text):
    """Error yang layak dicoba ulang dengan player client lain."""
    t = (text or "").lower()
    return (
        _is_bot_error(t)
        or "requested format is not available" in t
        or "page needs to be reloaded" in t
    )


def run_ytdlp(job, job_dir, tail_args, timeout, errlabel, verbose=False,
              attempts=None, use_proxy=False, meta=False, tiktok=False):
    """Jalankan yt-dlp melewati daftar percobaan (klien, pakai cookies).

    Error apa pun -> percobaan berikutnya. verbose=True menyaring
    baris terkait PO Token dari stderr ke log (diagnosis bgutil).
    Percobaan yang menang dicatat di job["win_attempt"].
    meta=True: jalur Meta (Facebook/Instagram) — satu percobaan
    langsung dengan cookies dari file server per platform, tanpa
    extractor-args YouTube dan tanpa proxy.
    tiktok=True: jalur TikTok — satu percobaan langsung, tanpa
    cookies, tanpa extractor-args YouTube, tanpa proxy.
    """
    last_tail = "kesalahan tidak diketahui"
    cookies_path = None
    if meta:
        url = tail_args[-1] if tail_args else ""
        cookies_path, platform = cookies_file_for(url)
        if not cookies_path or not os.path.exists(cookies_path):
            raise RuntimeError(
                "cookies {} belum terpasang di server".format(platform))
        attempts = META_ATTEMPTS
    if tiktok:
        attempts = TIKTOK_ATTEMPTS
    for client, use_cookies in (attempts or ATTEMPTS):
        if use_cookies and not os.environ.get(
                "YOUTUBE_COOKIES", "").strip():
            continue
        args = ytdlp_base_args(job_dir, use_cookies=use_cookies,
                               use_proxy=use_proxy,
                               cookies_path=cookies_path)
        if verbose:
            args += ["--verbose"]
        if client:
            args += ["--extractor-args",
                     "youtube:player_client=" + client]
        res = run_cmd(args + tail_args, timeout=timeout)
        label = (client or "web") + ("/cookies" if use_cookies
                                     else "/tanpa-cookies")
        if verbose:
            noise = ("script path doesn't exist", "no server_home",
                     "po token providers:", "po token cache")
            pot_lines = [
                ln.strip() for ln in (res.stderr or "").splitlines()
                if ("pot" in ln.lower() or "bgutil" in ln.lower())
                and not any(n in ln.lower() for n in noise)
            ]
            for ln in pot_lines[:12]:
                log(job, "[pot] " + ln[:200])
            if not pot_lines:
                log(job, "[pot] tidak ada baris PO Token di output "
                    "(klien " + label + ")")
        if res.returncode == 0:
            log(job, "Lolos via klien " + label + ".")
            job["win_attempt"] = (client, use_cookies)
            return res
        lines = (res.stderr or res.stdout or "").strip().splitlines()
        err_lines = [ln for ln in lines if "ERROR" in ln]
        last_tail = (err_lines[-1] if err_lines
                     else (lines[-1] if lines else last_tail))
        log(job, "Klien " + label + " gagal: "
            + (last_tail[:160] if last_tail else "?"))
    raise RuntimeError(errlabel + ": " + last_tail)


def fetch_info(job, url, job_dir):
    """Ambil metadata video (judul, durasi, heatmap) tanpa mengunduh."""
    res = run_ytdlp(job, job_dir, ["-J", url], timeout=180,
                    errlabel="yt-dlp gagal mengambil info video",
                    verbose=True, meta=is_meta_url(url),
                    tiktok=is_tiktok_url(url))
    info = json.loads(res.stdout)
    return info


def parse_vtt(text):
    """Parse WebVTT sederhana -> list segmen {start, end, text} (detik)."""
    segs = []
    cur = None
    for line in text.splitlines():
        line = line.strip()
        m = re.match(
            r"(\d+):(\d+):(\d+)[.,](\d+)\s*-->\s*(\d+):(\d+):(\d+)[.,](\d+)",
            line,
        )
        if m:
            g = [int(x) for x in m.groups()]
            start = g[0] * 3600 + g[1] * 60 + g[2] + g[3] / 1000.0
            end = g[4] * 3600 + g[5] * 60 + g[6] + g[7] / 1000.0
            cur = {"start": start, "end": end, "text": ""}
            segs.append(cur)
        elif line and cur is not None and "-->" not in line:
            clean = re.sub(r"<[^>]+>", "", line)
            if clean and clean not in cur["text"]:
                cur["text"] = (cur["text"] + " " + clean).strip()
    return [s for s in segs if s["text"]]


def parse_json3(text):
    """Parse subtitle json3 YouTube -> list segmen {start, end, text}."""
    try:
        data = json.loads(text)
    except Exception:
        return []
    segs = []
    for ev in data.get("events", []):
        parts = ev.get("segs") or []
        txt = "".join(p.get("utf8", "") for p in parts).strip()
        if not txt:
            continue
        start = ev.get("tStartMs", 0) / 1000.0
        dur = ev.get("dDurationMs", 2000) / 1000.0
        segs.append({"start": start, "end": start + dur, "text": txt})
    return segs


LANGS = ("auto", "id", "en", "ms", "ar", "zh", "hi", "ja", "ko",
         "th", "vi", "tl", "pt", "es", "fr", "de", "it")


def fetch_transcript(job, info, job_dir, lang_pref="auto"):
    """Unduh subtitle -> segmen ber-timestamp.

    Urutan bahasa: pilihan user dulu (bila bukan auto), lalu id, en."""
    subs = {}
    for key in ("subtitles", "automatic_captions"):
        for lang, tracks in (info.get(key) or {}).items():
            subs.setdefault(lang, tracks)
    wants = []
    if lang_pref and lang_pref != "auto":
        wants.append(lang_pref)
    wants += ["id", "en"]
    chosen = None
    for want in wants:
        for lang, tracks in subs.items():
            if lang == want or lang.startswith(want + "-"):
                chosen = tracks
                break
        if chosen:
            break
    if not chosen:
        return []
    track_url = None
    for t in chosen:
        if t.get("ext") == "json3":
            track_url = t.get("url")
            break
    if not track_url:
        for t in chosen:
            if t.get("ext") == "vtt":
                track_url = t.get("url")
                break
    if not track_url:
        return []
    try:
        r = requests.get(track_url, timeout=30)
        r.raise_for_status()
        if "json3" in track_url or r.text.lstrip().startswith("{"):
            return parse_json3(r.text)
        return parse_vtt(r.text)
    except Exception:
        return []


def _http_download(job, fmt_entry, dest, use_proxy=True):
    """Unduh satu format langsung dari URL hasil ekstraksi info.

    Format HLS (m3u8, khas klien TV/embedded) diambil pakai ffmpeg
    karena isinya playlist segmen, bukan satu berkas.
    """
    proto = fmt_entry.get("protocol") or ""
    if "m3u8" in proto:
        hdrs = "".join("{}: {}\r\n".format(k, v)
                       for k, v in (fmt_entry.get("http_headers")
                                    or {}).items())
        cmd = ["ffmpeg", "-y"]
        if hdrs:
            cmd += ["-headers", hdrs]
        cmd += ["-i", fmt_entry["url"], "-c", "copy", dest]
        res = run_cmd(cmd, timeout=1200)
        if res.returncode != 0 or not os.path.exists(dest):
            raise RuntimeError("ffmpeg gagal mengambil HLS.")
        return dest
    headers = dict(fmt_entry.get("http_headers") or {})
    headers.setdefault("User-Agent", "Mozilla/5.0")
    proxies = ({"http": PROXY_URL, "https": PROXY_URL}
               if (use_proxy and PROXY_URL) else None)
    try:
        from curl_cffi import requests as _cffi_requests
        def _get(**kw):
            return _cffi_requests.get(fmt_entry["url"],
                                      impersonate="chrome", **kw)
    except Exception:
        def _get(**kw):
            return requests.get(fmt_entry["url"], **kw)
    r = _get(headers=headers, stream=True,
             timeout=60, proxies=proxies)
    try:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    finally:
        try:
            r.close()
        except Exception:
            pass
    return dest


def _download_from_info(job, info, job_dir):
    """Unduh memakai URL format dari info yang SUDAH berhasil diambil.

    Ekstraksi ulang untuk unduh sering kena 403 googlevideo; URL dari
    info yang sukses membawa tanda tangan (dan token) yang sama dengan
    sesi ekstaksinya, jadi diunduh langsung lewat HTTP.
    """
    # Format HLS (m3u8) dikeluarkan dari unduh langsung: ffmpeg di sini
    # tidak bisa lewat proxy SOCKS, biar ditangani pengunduh HLS bawaan
    # yt-dlp pada jalur cadangan (yang menghormati --proxy).
    formats = [f for f in (info.get("formats") or [])
               if f.get("url") and "m3u8" not in (f.get("protocol")
                                                  or "")]

    def h(f):
        return f.get("height") or 0

    def _dl(entry, dest):
        # Sesi-konsisten dulu: URL dari info diekstrak langsung dari IP
        # ini, jadi unduhan langsung didahulukan; WARP jadi cadangan.
        try:
            return _http_download(job, entry, dest, use_proxy=False)
        except Exception:
            return _http_download(job, entry, dest, use_proxy=True)

    muxed = [f for f in formats
             if f.get("vcodec") not in (None, "none")
             and f.get("acodec") not in (None, "none")
             and h(f) <= 720]
    videos = [f for f in formats
              if f.get("vcodec") not in (None, "none")
              and f.get("acodec") in (None, "none") and h(f) <= 720]
    audios = [f for f in formats
              if f.get("acodec") not in (None, "none")
              and f.get("vcodec") in (None, "none")]
    muxed.sort(key=h, reverse=True)
    videos.sort(key=h, reverse=True)
    audios.sort(key=lambda f: f.get("abr") or 0, reverse=True)

    if muxed:
        f = muxed[0]
        log(job, "Unduh langsung format {} ({}p, {}).".format(
            f.get("format_id"), h(f), f.get("protocol")))
        dest = os.path.join(job_dir, "source.mp4")
        _dl(f, dest)
        return dest
    if videos and audios:
        v, a = videos[0], audios[0]
        log(job, "Unduh langsung format video {} ({}p, {}) + audio {}."
            .format(v.get("format_id"), h(v), v.get("protocol"),
                    a.get("format_id")))
        vpath = os.path.join(job_dir, "source_v.bin")
        apath = os.path.join(job_dir, "source_a.bin")
        _dl(v, vpath)
        _dl(a, apath)
        dest = os.path.join(job_dir, "source.mp4")
        res = run_cmd(["ffmpeg", "-y", "-i", vpath, "-i", apath,
                       "-c", "copy", dest], timeout=600)
        if res.returncode != 0 or not os.path.exists(dest):
            raise RuntimeError("Gagal menggabungkan video+audio.")
        return dest
    raise RuntimeError("Info video tidak membawa format yang bisa "
                       "diunduh langsung.")


def download_video(job, url, job_dir, info=None):
    meta = is_meta_url(url)
    tiktok = is_tiktok_url(url)
    # Cara utama: unduh langsung dari URL info yang sukses.
    if info:
        try:
            return _download_from_info(job, info, job_dir)
        except Exception as e:
            log(job, "Unduh langsung gagal ({}), coba yt-dlp…".format(
                str(e)[:120]))
    # Cadangan: yt-dlp, utamakan percobaan yang menang saat ambil info.
    attempts = None
    win = job.get("win_attempt")
    if win:
        attempts = [win] + [a for a in ATTEMPTS if a != win]
    out = os.path.join(job_dir, "source.%(ext)s")
    fmt = (
        "best[height<=720][ext=mp4]/best[height<=720]/"
        "bestvideo[height<=720]+bestaudio/best"
    )
    tail = [
        "-f", fmt,
        "--merge-output-format", "mp4",
        "-o", out,
        url,
    ]
    # Langsung (sesi-konsisten + impersonate) didahulukan; WARP cadangan
    # (bukan untuk Meta/TikTok: resep direct terbukti lolos).
    try:
        run_ytdlp(job, job_dir, tail, timeout=1200,
                  errlabel="yt-dlp gagal mengunduh video (langsung)",
                  attempts=attempts, use_proxy=False, meta=meta,
                  tiktok=tiktok)
    except RuntimeError:
        if meta or tiktok:
            raise
        log(job, "Jalur langsung gagal total, coba lewat WARP…")
        run_ytdlp(job, job_dir, tail, timeout=1200,
                  errlabel="yt-dlp gagal mengunduh video",
                  attempts=attempts, use_proxy=True)
    for name in sorted(os.listdir(job_dir)):
        if name.startswith("source.") and ".part" not in name \
                and not name.endswith(".ytdl"):
            return os.path.join(job_dir, name)
    raise RuntimeError("Berkas video hasil unduhan tidak ditemukan.")


# ---------------------------------------------------------------- kandidat
def heatmap_peaks(info, duration, min_dur, max_dur, count):
    """Ambil segmen di sekitar puncak heatmap (Most Replayed)."""
    heat = info.get("heatmap") or []
    if not heat:
        return []
    pts = []
    for h in heat:
        try:
            pts.append(
                (
                    float(h.get("start_time", 0)),
                    float(h.get("end_time", 0)),
                    float(h.get("value", 0)),
                )
            )
        except Exception:
            continue
    if not pts:
        return []
    pts.sort(key=lambda p: p[2], reverse=True)
    target = (min_dur + max_dur) / 2.0
    chosen = []
    for s, e, v in pts:
        center = (s + e) / 2.0
        start = max(0.0, center - target / 2.0)
        end = min(duration, start + target)
        start = max(0.0, end - target)
        if any(not (end <= c["start"] or start >= c["end"]) for c in chosen):
            continue  # tumpang tindih
        chosen.append(
            {"start": start, "end": end, "score": round(v * 100)}
        )
        if len(chosen) >= count:
            break
    return chosen


def transcript_summary(segs, limit_chars=12000):
    lines = []
    total = 0
    for s in segs:
        line = "[{:.0f}-{:.0f}] {}".format(s["start"], s["end"], s["text"])
        total += len(line)
        if total > limit_chars:
            break
        lines.append(line)
    return "\n".join(lines)


def gemini_candidates(api_key, title, duration, segs, heat_info,
                      count, min_dur, max_dur, audience):
    """Minta Gemini memilih momen terbaik. Balas list kandidat atau []."""
    prompt = (
        "Kamu adalah editor klip video viral. Video berjudul \"{title}\" "
        "berdurasi {dur} detik.\n"
        "Target penonton: {aud}.\n"
        "Pilih {n} momen terbaik untuk dijadikan klip viral (durasi tiap "
        "klip antara {mn} sampai {mx} detik). Utamakan bagian yang paling "
        "menarik, lucu, mengejutkan, atau penuh wawasan di awal kalimatnya.\n"
        "Info Most Replayed (heatmap, bila ada): {heat}\n"
        "Transkrip ber-timestamp (detik):\n{tr}\n\n"
        "Balas HANYA JSON murni berupa array objek tanpa teks lain, format:\n"
        '[{{"start": 12, "end": 40, "hook": "kalimat pancingan pendek", '
        '"score": 95, "title": "Judul pendek clickbait", '
        '"desc": "Teaser satu kalimat untuk caption postingan"}}]\n'
        "hook = kalimat pancingan pendek Bahasa Indonesia yang membuat "
        "penasaran. score = 0-100. title = judul pendek gaya clickbait "
        "untuk dibakar ke video. desc = teaser satu kalimat untuk "
        "caption saat posting klip."
    ).format(
        title=title, dur=int(duration), aud=audience or "umum",
        n=count, mn=min_dur, mx=max_dur,
        heat=heat_info or "tidak ada",
        tr=transcript_summary(segs) or "(tidak ada transkrip)",
    )
    try:
        r = requests.post(
            GEMINI_URL,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": api_key,
            },
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=120,
        )
        r.raise_for_status()
        data = r.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        text = text.strip()
        text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()
        arr = json.loads(text)
        out = []
        for item in arr:
            s = float(item["start"])
            e = float(item["end"])
            if e <= s:
                continue
            e = min(e, s + max_dur)
            if e - s < min_dur:
                e = s + min_dur
            out.append({
                "start": max(0.0, s),
                "end": min(duration, e),
                "hook": str(item.get("hook", "")).strip(),
                "score": int(item.get("score", 50)),
                "title": str(item.get("title", "")).strip(),
                "desc": str(item.get("desc", "")).strip(),
            })
        return out
    except Exception:
        return []


def even_fallback(duration, count, min_dur, max_dur):
    target = (min_dur + max_dur) / 2.0
    if duration <= target:
        return [{"start": 0.0, "end": duration, "score": 50}]
    out = []
    step = duration / count
    for i in range(count):
        center = step * i + step / 2.0
        start = max(0.0, center - target / 2.0)
        end = min(duration, start + target)
        out.append({"start": start, "end": end, "score": 50})
    return out


def hook_for_segment(segs, cand, title):
    if cand.get("hook"):
        return cand["hook"]
    for s in segs:
        if s["start"] >= cand["start"] and s["start"] < cand["end"]:
            txt = s["text"].strip()
            if txt:
                return (txt[:77] + "…") if len(txt) > 80 else txt
    return title


# ---------------------------------------------------------------- STT
def groq_transcribe(job, src, job_dir, groq_key, total_duration,
                     lang="auto"):
    """Transkrip audio pakai Groq Whisper (dipakai bila video tak
    punya transkrip YouTube dan caption aktif).

    Audio diekstrak mono 32kbps lalu dipotong per 20 menit (batas
    berkas Groq 25MB). Mengembalikan (segments, words) dengan
    timestamp detik; words = [{start, end, text}] per kata.
    Key hanya dipakai di header Authorization, tidak pernah dicatat.
    """
    words = []
    chunk_len = 1200.0
    n_chunks = max(1, int(total_duration // chunk_len) + 1)
    for ci in range(n_chunks):
        start = ci * chunk_len
        if start >= total_duration:
            break
        audio = os.path.join(job_dir, "stt_{}.mp3".format(ci))
        cmd = ["ffmpeg", "-y", "-ss", "{:.1f}".format(start),
               "-t", "{:.1f}".format(chunk_len), "-i", src,
               "-vn", "-ac", "1", "-ar", "16000", "-b:a", "32k", audio]
        res = run_cmd(cmd, timeout=600)
        if res.returncode != 0 or not os.path.exists(audio):
            raise RuntimeError("ekstrak audio gagal")
        try:
            with open(audio, "rb") as fh:
                r = requests.post(
                    "https://api.groq.com/openai/v1/audio/transcriptions",
                    headers={"Authorization": "Bearer " + groq_key},
                    files={"file": ("audio.mp3", fh, "audio/mpeg")},
                    data={"model": "whisper-large-v3-turbo",
                          "response_format": "verbose_json",
                          "timestamp_granularities[]": ["word", "segment"],
                          **({"language": lang} if lang and lang != "auto"
                             else {})},
                    timeout=600,
                )
        finally:
            try:
                os.remove(audio)
            except OSError:
                pass
        if r.status_code != 200:
            raise RuntimeError("Groq HTTP {}".format(r.status_code))
        data = r.json()
        for w in data.get("words") or []:
            txt = (w.get("word") or "").strip()
            if not txt:
                continue
            words.append({
                "start": start + float(w.get("start", 0)),
                "end": start + float(w.get("end", 0)),
                "text": txt,
            })
    # kelompokkan kata menjadi segmen untuk hook/pemilih momen
    segs = []
    cur = None
    for w in words:
        if (cur is None or w["start"] - cur["end"] > 0.8
                or w["end"] - cur["start"] > 4.0
                or len(cur["text"].split()) >= 12):
            cur = {"start": w["start"], "end": w["end"], "text": w["text"]}
            segs.append(cur)
        else:
            cur["end"] = w["end"]
            cur["text"] += " " + w["text"]
    return segs, words


# ---------------------------------------------------------------- ffmpeg
def sanitize_drawtext(text):
    """Bersihkan teks agar aman untuk filter drawtext ffmpeg."""
    text = text.replace("\\", " ").replace(":", " ").replace("'", " ")
    text = text.replace('"', " ").replace("%", " ").replace("[", " ")
    text = text.replace("]", " ").replace(";", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", text).strip()[:90]


def _ass_color(hexstr):
    """Hex RRGGBB -> warna ASS &H00BBGGRR; None bila tak valid."""
    try:
        h = (hexstr or "").strip().lstrip("#")
        if len(h) != 6:
            return None
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return "&H00{:02X}{:02X}{:02X}".format(b, g, r)
    except Exception:
        return None


def probe_dims(src):
    """Dimensi video sumber (lebar, tinggi) via ffprobe; (None, None)
    bila gagal."""
    try:
        r = run_cmd(["ffprobe", "-v", "error", "-select_streams", "v:0",
                     "-show_entries", "stream=width,height",
                     "-of", "csv=p=0", src], timeout=60)
        out = (r.stdout or "").strip().split(",")
        return int(out[0]), int(out[1])
    except Exception:
        return None, None


def face_trajectory(src, start, end, job_dir, idx):
    """Jejak posisi wajah untuk Smart Face Tracking.

    Sampel 1 fps dari segmen klip, deteksi wajah terbesar (Haar),
    kosong diisi tetangga terdekat, lalu smoothing rata-gerak.
    Mengembalikan list (t_rel_detik, x_fraksi_lebar) atau None bila
    OpenCV tak tersedia / tak ada wajah terdeteksi."""
    try:
        import cv2
    except Exception:
        return None
    tmp = os.path.join(job_dir, "face{}".format(idx))
    try:
        os.makedirs(tmp, exist_ok=True)
        dur = max(1.0, end - start)
        cmd = ["ffmpeg", "-y", "-ss", "{:.2f}".format(start),
               "-t", "{:.2f}".format(dur), "-i", src,
               "-vf", "fps=1,scale=640:-2",
               os.path.join(tmp, "f%03d.jpg")]
        res = run_cmd(cmd, timeout=300)
        if res.returncode != 0:
            return None
        cascade = cv2.CascadeClassifier(
            cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
        if cascade.empty():
            return None
        pts = []
        for fn in sorted(os.listdir(tmp)):
            if not fn.endswith(".jpg"):
                continue
            try:
                n = int(fn[1:4])
            except Exception:
                continue
            img = cv2.imread(os.path.join(tmp, fn))
            if img is None:
                continue
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            faces = cascade.detectMultiScale(gray, 1.2, 5,
                                             minSize=(60, 60))
            if len(faces) == 0:
                pts.append((float(n - 1), None))
                continue
            x, y, w, h = max(faces, key=lambda f: f[2] * f[3])
            pts.append((float(n - 1), (x + w / 2.0) / img.shape[1]))
        vals = [p[1] for p in pts]
        last = None
        for i, v in enumerate(vals):
            if v is None:
                vals[i] = last
            else:
                last = v
        if all(v is None for v in vals):
            return None
        first = next(v for v in vals if v is not None)
        vals = [first if v is None else v for v in vals]
        sm = []
        for i in range(len(vals)):
            lo = max(0, i - 2)
            hi = min(len(vals), i + 3)
            sm.append(sum(vals[lo:hi]) / (hi - lo))
        return [(t, v) for (t, _), v in zip(pts, sm)]
    except Exception:
        return None
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _crop_px(x_frac, crop_w_expr):
    """Ekspresi piksel x crop dari fraksi lebar, dijepit ke dalam
    frame: x = x_frac*iw - crop_w/2, 0 <= x <= iw-crop_w."""
    return "min(max({:.4f}*iw-({})/2,0),iw-({}))".format(
        x_frac, crop_w_expr, crop_w_expr)


def _x_expr_from_points(points, crop_w_expr):
    """Ekspresi x crop bertingkat (step per detik) dari jejak wajah
    [(t, x_frac)]. Titik berlaku untuk [t, t+1)."""
    expr = None
    for t, xf in reversed(points):
        px = _crop_px(xf, crop_w_expr)
        if expr is None:
            expr = px
        else:
            expr = "if(lt(t,{:.2f}),{}, {})".format(t + 1.0, px, expr)
    return expr


def _x_expr_lerp(points, crop_w_expr):
    """Ekspresi x crop interpolasi linear dari keyframe [(t, x_frac)].
    Di luar rentang: nilai ujung terdekat."""
    def px(xf):
        return _crop_px(xf, crop_w_expr)
    expr = px(points[-1][1])
    for i in range(len(points) - 2, -1, -1):
        t0, x0 = points[i]
        t1, x1 = points[i + 1]
        if t1 <= t0:
            continue
        lerp = ("({:.4f}+({:.4f}-{:.4f})*(t-{:.2f})/{:.2f})*iw"
                "-({})/2".format(x0, x1, x0, t0, t1 - t0, crop_w_expr))
        lerp = "min(max({},0),iw-({}))".format(lerp, crop_w_expr)
        expr = "if(lt(t,{:.2f}),{}, {})".format(t1, lerp, expr)
    expr = "if(lt(t,{:.2f}),{}, {})".format(points[0][0],
                                            px(points[0][1]), expr)
    return expr


def _overlay_filters(settings, teaser_text, teaser_seconds, title_text):
    """Filter drawtext untuk judul, teaser, dan watermark teks."""
    parts = []
    if title_text and settings.get("titleMode", "off") != "off":
        safe = sanitize_drawtext(title_text)
        if safe:
            en = ":enable='lt(t,3)'" if settings["titleMode"] == "awal" \
                else ""
            if settings.get("titleStyle") == "teks":
                parts.append(
                    "drawtext=font='DejaVu Sans':text='{}':fontsize=44:"
                    "fontcolor=white:borderw=2:bordercolor=black:"
                    "x=(w-text_w)/2:y=40{}".format(safe, en))
            else:  # bar: teks di atas bilah hitam transparan
                parts.append(
                    "drawtext=font='DejaVu Sans':text='{}':fontsize=40:"
                    "fontcolor=white:box=1:boxcolor=black@0.6:"
                    "boxborderw=24:x=(w-text_w)/2:y=30{}".format(safe, en))
    if teaser_text:
        safe = sanitize_drawtext(teaser_text)
        if safe:
            parts.append(
                "drawtext=font='DejaVu Sans':text='{}':fontsize=42:"
                "fontcolor=white:borderw=2:bordercolor=black:"
                "x=(w-text_w)/2:y=h*0.08:enable='lt(t,{:.1f})'".format(
                    safe, float(teaser_seconds)
                )
            )
    wm = (settings.get("watermarkText") or "").strip()
    if wm:
        safe = sanitize_drawtext(wm)[:40]
        if safe:
            xy = {
                "kanan-bawah": "x=w-text_w-20:y=h-text_h-20",
                "kiri-bawah": "x=20:y=h-text_h-20",
                "kanan-atas": "x=w-text_w-20:y=20",
                "kiri-atas": "x=20:y=20",
            }.get(settings.get("watermarkPos", "kanan-bawah"),
                  "x=w-text_w-20:y=h-text_h-20")
            op = max(10, min(100,
                             int(settings.get("watermarkOpacity", 60)))) / 100.0
            fs = max(16, min(64, int(settings.get("watermarkSize", 28))))
            parts.append(
                "drawtext=font='DejaVu Sans':text='{}':fontsize={}:"
                "fontcolor=white@{:.2f}:borderw=1:bordercolor=black@0.5:"
                "{}".format(safe, fs, op, xy))
    return parts


def _box_pct(d):
    """Validasi kotak persen {x,y,w,h}; None bila tak valid."""
    try:
        x, y, w, h = (float(d["x"]), float(d["y"]),
                      float(d["w"]), float(d["h"]))
    except Exception:
        return None
    if not (0 <= x < 100 and 0 <= y < 100 and 0 < w <= 100
            and 0 < h <= 100):
        return None
    if x + w > 100 or y + h > 100:
        return None
    return (x, y, w, h)


def build_filter(settings, teaser_text=None, teaser_seconds=3,
                 title_text=None, x_expr=None, crop_box=None):
    """Susun -vf untuk crop rasio + skala + overlay.

    settings: dict settings job. crop_box: (x%, y%, w%, h%) persen
    dimensi sumber (crop manual, ganti crop tengah). x_expr:
    ekspresi x crop (face tracking / keyframe, dikutip agar koma di
    dalamnya tak memecah filter). Judul + watermark teks ikut di sini;
    watermark gambar & gaming split memakai filter_complex di cut_clip.
    """
    ratio = settings["ratio"]
    height = int(settings["height"])
    parts = []
    if ratio == "9:16-blur":
        # latar blur: satu cabang diburamkan memenuhi frame, satu cabang
        # video utuh di tengah (fit tinggi)
        w = height
        h = int(w * 16 / 9)
        h -= h % 2
        parts.append(
            "split[a][b];[a]scale={0}:{1}:force_original_aspect_ratio"
            "=increase,crop={0}:{1},gblur=sigma=20[bg];[b]scale=-2:{1}"
            "[fg];[bg][fg]overlay=(W-w)/2:(H-h)/2".format(w, h))
    elif ratio == "9:16":
        if crop_box:
            xp, yp, wp, hp = crop_box
            parts.append("crop=iw*{:.4f}:ih*{:.4f}:iw*{:.4f}:ih*{:.4f}"
                         .format(wp / 100.0, hp / 100.0,
                                 xp / 100.0, yp / 100.0))
        elif x_expr:
            parts.append("crop=ih*9/16:ih:'{}'".format(x_expr))
        else:
            parts.append("crop=ih*9/16:ih")
        # "height" = lebar target untuk video vertikal: 720 -> 720x1280
        w = height
        h = int(w * 16 / 9)
        h -= h % 2
        parts.append("scale={}:{}".format(w, h))
    elif ratio == "1:1":
        if crop_box:
            xp, yp, wp, hp = crop_box
            parts.append("crop=iw*{:.4f}:ih*{:.4f}:iw*{:.4f}:ih*{:.4f}"
                         .format(wp / 100.0, hp / 100.0,
                                 xp / 100.0, yp / 100.0))
        elif x_expr:
            parts.append("crop=ih:ih:'{}'".format(x_expr))
        else:
            parts.append("crop=ih:ih")
        parts.append("scale={}:{}".format(height, height))
    else:  # 16:9 tanpa crop, skala berdasarkan tinggi
        parts.append("scale=-2:{}".format(height))
    parts.extend(_overlay_filters(settings, teaser_text, teaser_seconds,
                                  title_text))
    return ",".join(parts)


def output_dims(ratio, height):
    """Dimensi piksel keluaran, sama seperti yang dihasilkan
    build_filter."""
    height = int(height)
    if ratio in ("9:16", "9:16-blur"):
        w = height
        h = int(w * 16 / 9)
        return w, h - h % 2
    if ratio == "1:1":
        return height, height
    return 1280, 720  # 16:9: perkiraan untuk PlayRes ASS


def _ass_time(sec):
    sec = max(0.0, float(sec))
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    return "{}:{:02d}:{:05.2f}".format(h, m, s)


def build_clip_ass(segs, cand, settings, job_dir, idx, words=None):
    """Buat berkas .ass caption viral per-kata untuk satu klip.

    Kata-kata dari segmen transkrip yang tumpang tindih dengan jendela
    klip dikelompokkan 3-4 kata per baris; tiap kata diberi tag karaoke
    \\kf sehingga kata yang sedang diucapkan menyala (gaya klip viral).
    Waktu per kata ditimbang dari panjang katanya di dalam segmen.
    Bila `words` diberikan (hasil STT Groq, timestamp per kata asli),
    kata-kata itu dipakai langsung tanpa ditimbang ulang.
    Mengembalikan path berkas, atau None bila tak ada teks.
    """
    clip_words = []  # (start_rel, end_rel, text)
    if words:
        for w in words:
            if w["end"] <= cand["start"] or w["start"] >= cand["end"]:
                continue
            txt = w["text"].replace("{", "(").replace("}", ")")
            clip_words.append((max(0.0, w["start"] - cand["start"]),
                               w["end"] - cand["start"], txt))
    for s in ([] if words else segs):
        if s["end"] <= cand["start"] or s["start"] >= cand["end"]:
            continue
        parts = [w for w in re.split(r"\s+", s["text"].strip()) if w]
        parts = [w.replace("{", "(").replace("}", ")") for w in parts]
        if not parts:
            continue
        total = sum(len(w) for w in parts) or 1
        t = max(s["start"], cand["start"])
        seg_end = min(s["end"], cand["end"])
        span = max(0.1, seg_end - t)
        for w in parts:
            d = max(0.06, span * len(w) / total)
            clip_words.append((t - cand["start"],
                               t - cand["start"] + d, w))
            t += d
    if not clip_words:
        return None

    style_name = settings.get("captionStyle", "pop")
    w_px, h_px = output_dims(settings["ratio"], settings["height"])
    fsize = max(28, round(h_px * 0.047))
    margin_v = round(h_px * 0.17)
    if style_name == "klasik":
        primary, secondary, border, back = (
            "&H00FFFFFF", "&H00FFFFFF", 1, "&H00000000")
        outline = 5
    elif style_name == "kotak":
        primary, secondary, border, back = (
            "&H0000FFFF", "&H00FFFFFF", 3, "&H96000000")
        outline = 12
    else:  # pop
        primary, secondary, border, back = (
            "&H0000FFFF", "&H00FFFFFF", 1, "&H00000000")
        outline = 5
    upper = style_name in ("pop", "kotak")
    # --- editor caption (v1.5): warna, ukuran, posisi, glow, kotak ---
    size_pct = max(70, min(150, int(settings.get("capSizePct", 100))))
    fsize = max(24, round(fsize * size_pct / 100.0))
    pos_frac = {"bawah": 0.17, "tengah": 0.45, "atas": 0.78}.get(
        settings.get("capPos", "bawah"), 0.17)
    margin_v = round(h_px * pos_frac)
    custom = _ass_color(settings.get("capColor", ""))
    if custom:
        primary = custom
    if settings.get("capBox"):
        border, back = 3, "&H96000000"
    shadow = 2 if settings.get("capGlow") else 1
    cap_chunk = max(1, min(8, int(settings.get("capChunk", 4))))

    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        "PlayResX: {}".format(w_px),
        "PlayResY: {}".format(h_px),
        "",
        "[V4+ Styles]",
        ("Format: Name, Fontname, Fontsize, PrimaryColour, "
         "SecondaryColour, OutlineColour, BackColour, Bold, Italic, "
         "Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, "
         "BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, "
         "MarginV, Encoding"),
        ("Style: Viral,DejaVu Sans,{},{},{},&H00000000,{},1,0,0,0,100,"
         "100,0,0,{},{},{},2,24,24,{},1").format(
             fsize, primary, secondary, back, border, outline, shadow,
             margin_v),
        "",
        "[Events]",
        ("Format: Layer, Start, End, Style, Name, MarginL, MarginR, "
         "MarginV, Effect, Text"),
    ]
    chunk = []
    chunk_start = None

    def flush():
        if not chunk:
            return
        start = chunk[0][0]
        end = chunk[-1][1] + 0.05
        pieces = []
        for ws, we, wt in chunk:
            cs = max(1, round((we - ws) * 100))
            txt = wt.upper() if upper else wt
            pieces.append("{{\\kf{}}}{}".format(cs, txt))
        lines.append("Dialogue: 0,{},{},Viral,,0,0,0,,{}".format(
            _ass_time(start), _ass_time(end), " ".join(pieces)))

    for item in clip_words:
        if chunk and (len(chunk) >= cap_chunk
                      or item[1] - chunk_start > 2.4):
            flush()
            chunk = []
        if not chunk:
            chunk_start = item[0]
        chunk.append(item)
    flush()

    path = os.path.join(job_dir, "cap{}.ass".format(idx))
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def _subtitles_filter(ass_path):
    esc = (ass_path.replace("\\", "\\\\").replace(":", "\\:")
           .replace("'", "\\'"))
    return "subtitles='{}'".format(esc)


def cut_clip(src, dst, cand, settings, ass_path=None, job_dir=None,
             idx=1):
    """Potong satu segmen dengan ffmpeg. True bila sukses.

    Jalur filter_complex dipakai bila gaming split / watermark gambar
    aktif; selebihnya -vf biasa dengan varian cadangan (penuh, tanpa
    overlay, polos) sehingga satu overlay gagal tak menggagalkan klip.
    """
    teaser = cand.get("hook") if settings.get("teaser") else None
    teaser_sec = settings.get("teaserSeconds", 3)
    title = cand.get("title") if settings.get("titleMode", "off") != "off" \
        else None
    ratio = settings["ratio"]
    use_split = bool(settings.get("splitMode")) and ratio == "9:16"
    wm_path = settings.get("watermarkImagePath")

    # --- kotak crop manual / ekspresi x (face tracking / keyframe) ---
    crop_box = None
    cb = _box_pct(settings.get("cropBox") or {}) if settings.get("cropBox") \
        else None
    if cb and ratio in ("9:16", "1:1"):
        crop_box = cb
    x_expr = None
    if not use_split and not crop_box and ratio in ("9:16", "1:1"):
        cw = "ih*9/16" if ratio == "9:16" else "ih"
        kf = settings.get("keyframes") or []
        pts = []
        for k in kf:
            try:
                pts.append((float(k["t"]), float(k["x"]) / 100.0))
            except Exception:
                continue
        pts = sorted(set(pts))
        if len(pts) >= 2:
            x_expr = _x_expr_lerp(pts, cw)
        elif settings.get("faceTrack") and job_dir:
            traj = face_trajectory(src, cand["start"], cand["end"],
                                   job_dir, idx)
            if traj:
                x_expr = _x_expr_from_points(traj, cw)

    attempts = []

    # --- percobaan filter_complex (split / watermark gambar) ---
    if (use_split or wm_path) and not (
            not use_split and ratio == "9:16-blur"):
        graph = ""
        if use_split:
            W = int(settings["height"])
            H = int(W * 16 / 9)
            H -= H % 2
            face_h = max(100, H * int(settings.get("splitFaceH", 35))
                         // 100)
            face_h -= face_h % 2
            game_h = H - face_h
            fb = _box_pct(settings.get("splitFaceBox") or {}) or \
                (62, 68, 36, 30)
            gb = _box_pct(settings.get("splitGameBox") or {}) or \
                (0, 0, 100, 100)
            fx, fy, fw, fh = fb
            gx, gy, gw, gh = gb
            graph += (
                "[0:v]split=2[fs][gs];"
                "[fs]crop=iw*{:.4f}:ih*{:.4f}:iw*{:.4f}:ih*{:.4f},"
                "scale={}:{}:force_original_aspect_ratio=increase,"
                "crop={}:{}[face];"
                "[gs]crop=iw*{:.4f}:ih*{:.4f}:iw*{:.4f}:ih*{:.4f},"
                "scale={}:{}:force_original_aspect_ratio=increase,"
                "crop={}:{}[game];"
            ).format(fw / 100, fh / 100, fx / 100, fy / 100,
                     W, face_h, W, face_h,
                     gw / 100, gh / 100, gx / 100, gy / 100,
                     W, game_h, W, game_h)
            if settings.get("splitFace", "bawah") == "atas":
                graph += "[face][game]vstack[v]"
            else:
                graph += "[game][face]vstack[v]"
            vid = "[v]"
            ov = _overlay_filters(settings, teaser, teaser_sec, title)
            if ov:
                graph += ";[v]" + ",".join(ov) + "[v2]"
                vid = "[v2]"
        else:
            vf0 = build_filter(settings, teaser, teaser_sec, title,
                               x_expr, crop_box)
            graph += "[0:v]" + vf0 + "[vbase]"
            vid = "[vbase]"
        if ass_path:
            graph += ";{0}{1}[vout]".format(vid, _subtitles_filter(ass_path))
            vid = "[vout]"
        inputs = ["ffmpeg", "-y",
                  "-ss", "{:.2f}".format(cand["start"]),
                  "-to", "{:.2f}".format(cand["end"]),
                  "-i", src]
        if wm_path:
            inputs += ["-i", wm_path]
            op = max(10, min(100,
                             int(settings.get("watermarkOpacity", 60)))) / 100.0
            xy = {
                "kanan-bawah": "x=W-w-20:y=H-h-20",
                "kiri-bawah": "x=20:y=H-h-20",
                "kanan-atas": "x=W-w-20:y=20",
                "kiri-atas": "x=20:y=20",
            }.get(settings.get("watermarkPos", "kanan-bawah"),
                  "x=W-w-20:y=H-h-20")
            graph += (";[1:v]format=rgba,colorchannelmixer=aa={op:.2f}[wm];"
                      "{vid}[wm]overlay={xy}:format=yuv420[vfin]"
                      ).format(op=op, vid=vid, xy=xy)
            vid = "[vfin]"
        attempts.append(inputs + [
            "-filter_complex", graph,
            "-map", vid, "-map", "0:a",
            "-c:v", "libx264", "-preset", "veryfast",
            "-c:a", "aac", "-movflags", "+faststart", dst,
        ])

    # --- percobaan -vf biasa: penuh, tanpa overlay, polos ---
    vf_full = build_filter(settings, teaser, teaser_sec, title,
                           x_expr, crop_box)
    if ass_path:
        vf_full += "," + _subtitles_filter(ass_path)
    attempts.append(["ffmpeg", "-y",
                     "-ss", "{:.2f}".format(cand["start"]),
                     "-to", "{:.2f}".format(cand["end"]),
                     "-i", src, "-vf", vf_full,
                     "-c:v", "libx264", "-preset", "veryfast",
                     "-c:a", "aac", "-movflags", "+faststart", dst])
    vf_sub = build_filter(settings, None, 0, None, x_expr, crop_box)
    if ass_path:
        vf_sub += "," + _subtitles_filter(ass_path)
    attempts.append(["ffmpeg", "-y",
                     "-ss", "{:.2f}".format(cand["start"]),
                     "-to", "{:.2f}".format(cand["end"]),
                     "-i", src, "-vf", vf_sub,
                     "-c:v", "libx264", "-preset", "veryfast",
                     "-c:a", "aac", "-movflags", "+faststart", dst])
    vf_plain = build_filter(settings, None, 0, None, None, None)
    attempts.append(["ffmpeg", "-y",
                     "-ss", "{:.2f}".format(cand["start"]),
                     "-to", "{:.2f}".format(cand["end"]),
                     "-i", src, "-vf", vf_plain,
                     "-c:v", "libx264", "-preset", "veryfast",
                     "-c:a", "aac", "-movflags", "+faststart", dst])

    for cmd in attempts:
        res = run_cmd(cmd, timeout=900)
        if res.returncode == 0 and os.path.exists(dst):
            return True
    return False


# ---------------------------------------------------------------- pipeline
def process_job(job, url, gemini_key, settings, groq_key=None):
    job_dir = os.path.join(BASE_DIR, job["id"])
    os.makedirs(job_dir, exist_ok=True)
    try:
        job["status"] = "running"
        log(job, "Memulai proses…")
        set_progress(job, 5)

        log(job, "Mengambil info video…")
        info = fetch_info(job, url, job_dir)
        title = info.get("title") or "Video"
        duration = float(info.get("duration") or 0)
        if duration <= 0:
            raise RuntimeError("Durasi video tidak diketahui.")
        log(job, "Judul: {}".format(title))
        log(job, "Durasi: {:.0f} detik".format(duration))
        set_progress(job, 15)

        log(job, "Mengunduh video…")
        src = download_video(job, url, job_dir, info=info)
        set_progress(job, 40)

        log(job, "Membaca transkrip…")
        segs = fetch_transcript(job, info, job_dir,
                                settings.get("sttLang", "auto"))
        stt_words = None
        if segs:
            log(job, "Transkrip ditemukan ({} segmen).".format(len(segs)))
        elif settings.get("captions") and groq_key:
            log(job, "Transkrip kosong — memakai AI speech-to-text "
                     "(Groq)…")
            try:
                segs, stt_words = groq_transcribe(
                    job, src, job_dir, groq_key, duration,
                    settings.get("sttLang", "auto"))
                if segs:
                    log(job, "STT Groq selesai ({} kata).".format(
                        len(stt_words)))
                else:
                    log(job, "STT Groq tidak menghasilkan teks.")
            except Exception as e:  # noqa: BLE001
                segs, stt_words = [], None
                log(job, "STT Groq gagal ({}); klip tanpa "
                         "caption.".format(e))
        else:
            log(job, "Transkrip tidak tersedia.")
        set_progress(job, 50)

        count = settings["clipCount"]
        min_dur = settings["minDur"]
        max_dur = settings["maxDur"]

        # --- mode manual: pakai pilihan user, lewati seleksi otomatis ---
        manual_picked = []
        manual_clips = settings.get("clips") or []
        if settings.get("mode") in ("manual", "manual_ai") and manual_clips:
            for mc in manual_clips:
                try:
                    s = max(0.0, float(mc.get("start", 0)))
                    e = min(duration, float(mc.get("end", 0)))
                except Exception:
                    continue
                if e - s < 3:
                    continue
                manual_picked.append({
                    "start": s, "end": e,
                    "hook": str(mc.get("hook", "") or ""),
                    "title": str(mc.get("title", "") or ""),
                    "desc": str(mc.get("desc", "") or ""),
                    "score": int(mc.get("score", 50) or 50),
                })
            manual_picked.sort(key=lambda c: c["start"])
        skip_auto = bool(manual_picked)

        heat = heatmap_peaks(info, duration, min_dur, max_dur, count)
        heat_info = ""
        if heat:
            heat_info = ", ".join(
                "{:.0f}-{:.0f} (skor {})".format(
                    h["start"], h["end"], h["score"])
                for h in heat
            )
            log(job, "Data Most Replayed ditemukan.")
        else:
            log(job, "Data Most Replayed tidak tersedia.")

        candidates = []
        if gemini_key and not skip_auto:
            log(job, "AI menganalisis momen…")
            candidates = gemini_candidates(
                gemini_key, title, duration, segs, heat_info,
                count, min_dur, max_dur, settings.get("audience", ""),
            )
            if candidates:
                log(job, "AI memilih {} momen.".format(len(candidates)))
            else:
                log(job, "AI tidak memberi hasil, memakai cadangan.")
        if not skip_auto and not candidates and heat:
            candidates = heat
            log(job, "Memakai puncak Most Replayed.")
        if not skip_auto and not candidates:
            candidates = even_fallback(duration, count, min_dur, max_dur)
            log(job, "Memakai segmen merata sepanjang video.")

        # rapikan: batasi jumlah, hindari tumpang tindih, urutkan skor
        candidates.sort(key=lambda c: c.get("score", 0), reverse=True)
        picked = []
        for c in candidates:
            if any(not (c["end"] <= p["start"] or c["start"] >= p["end"])
                   for p in picked):
                continue
            picked.append(c)
            if len(picked) >= count:
                break
        if not skip_auto and settings.get("onlyHighScore"):
            picked = [c for c in picked if c.get("score", 0) >= 70] or picked[:1]
        picked.sort(key=lambda c: c["start"])
        if skip_auto:
            picked = manual_picked
            log(job, "Mode manual: {} klip dipilih.".format(len(picked)))
        if not picked:
            raise RuntimeError("Tidak ada momen yang bisa dipotong.")
        set_progress(job, 60)

        if settings.get("captions"):
            if segs:
                log(job, "Caption viral akan dibakar ke klip.")
            else:
                log(job, "Caption dilewati: transkrip tidak tersedia.")

        total = len(picked)
        for i, cand in enumerate(picked, start=1):
            cand["hook"] = hook_for_segment(segs, cand, title)
            log(job, "Memotong klip {}/{}…".format(i, total))
            fname = "clip{}.mp4".format(i)
            dst = os.path.join(job_dir, fname)
            ass_path = None
            if settings.get("captions") and segs:
                try:
                    ass_path = build_clip_ass(segs, cand, settings,
                                              job_dir, i,
                                              words=stt_words)
                except Exception:
                    ass_path = None
            if not cut_clip(src, dst, cand, settings, ass_path=ass_path,
                             job_dir=job_dir, idx=i):
                log(job, "Klip {} gagal dipotong, dilewati.".format(i))
                continue
            job["clips"].append({
                "file": fname,
                "url": "/files/{}/{}".format(job["id"], fname),
                "start": round(cand["start"], 1),
                "end": round(cand["end"], 1),
                "hook": cand["hook"],
                "title": cand.get("title", ""),
                "desc": cand.get("desc", ""),
                "score": cand.get("score", 50),
            })
            set_progress(job, 60 + int(40 * i / total))

        if not job["clips"]:
            raise RuntimeError("Semua klip gagal dipotong.")
        wm_path = settings.get("watermarkImagePath")
        if wm_path:
            try:
                os.remove(wm_path)
            except OSError:
                pass
        job["status"] = "done"
        set_progress(job, 100)
        log(job, "Selesai. {} klip dibuat.".format(len(job["clips"])))
    except Exception as e:  # noqa: BLE001
        wm_path = settings.get("watermarkImagePath")
        if wm_path:
            try:
                os.remove(wm_path)
            except OSError:
                pass
        job["status"] = "error"
        job["error"] = str(e)
        log(job, "Error: {}".format(e))


# ---------------------------------------------------------------- routes
@app.get("/")
def index():
    return jsonify({"status": "ok", "service": "auto-clipper",
                    "version": APP_VER})


@app.get("/api/debug/bgutil")
def debug_bgutil():
    """Cek server bgutil (generator PO Token) hidup di 127.0.0.1:4416."""
    try:
        r = requests.get("http://127.0.0.1:4416/ping", timeout=4)
        return jsonify({"reachable": True, "status_code": r.status_code,
                        "body": (r.text or "")[:200]})
    except Exception as e:
        return jsonify({"reachable": False, "error": str(e)[:200]})


@app.get("/api/debug/egress")
def debug_egress():
    """Bandingkan egress langsung vs lewat proxy (WARP).

    Memakai jejak Cloudflare: warp=on berarti keluar lewat WARP.
    """
    out = {"proxy_configured": PROXY_URL or None}
    url = "https://www.cloudflare.com/cdn-cgi/trace"
    for label, proxies in (("direct", None),
                           ("via_proxy", {"http": PROXY_URL,
                                          "https": PROXY_URL}
                            if PROXY_URL else None)):
        if label == "via_proxy" and not PROXY_URL:
            continue
        try:
            r = requests.get(url, timeout=10, proxies=proxies)
            info = {}
            for ln in (r.text or "").splitlines():
                if "=" in ln:
                    k, v = ln.split("=", 1)
                    if k in ("ip", "warp", "loc"):
                        info[k] = v
            out[label] = info
        except Exception as e:
            out[label] = {"error": str(e)[:160]}
    return jsonify(out)


@app.post("/api/debug/probe")
def debug_probe():
    """Probe bedah yt-dlp satu percobaan (tanpa tangga, tanpa job).

    Header wajib: X-Probe-Key == env PROBE_KEY.
    Body: {url, client, cookies: bool, mode: "info"|"bytes",
           format_id?, proto?: "hls"|"https"}
    - info: ekstrak -J sekali, rangkum format (total, maks, HLS).
    - bytes: ekstrak lalu unduh <=1MB dari format terpilih memakai
      curl_cffi impersonate; untuk HLS, ambil manifest + segmen
      pertama. Melaporkan status HTTP saja.
    """
    key = os.environ.get("PROBE_KEY", "")
    if not key or request.headers.get("X-Probe-Key") != key:
        return jsonify({"error": "unauthorized"}), 403
    data = request.get_json(force=True, silent=True) or {}
    url = data.get("url") or ""
    if "youtube.com/watch" not in url and "youtu.be/" not in url:
        return jsonify({"error": "url youtube tidak valid"}), 400
    client = (data.get("client") or "web").strip()
    use_cookies = bool(data.get("cookies", True))
    mode = data.get("mode") or "info"
    tmp = tempfile.mkdtemp(prefix="probe-")
    try:
        if mode == "instance":
            inst = (data.get("instance") or "").rstrip("/")
            if not inst.startswith("https://"):
                return jsonify({"ok": False,
                                "error": "instance tidak valid"})
            try:
                r = requests.post(
                    inst + "/",
                    json={"url": url, "videoQuality": "720"},
                    headers={"Accept": "application/json",
                             "Content-Type": "application/json"},
                    timeout=60)
                body = {}
                try:
                    body = r.json()
                except Exception:
                    pass
                out = {"ok": False, "instance_status": r.status_code,
                       "instance_resp": str(body)[:300]}
                tun = body.get("url") if isinstance(body, dict) \
                    else None
                if body.get("status") in ("tunnel", "redirect") and tun:
                    r2 = requests.get(
                        tun, headers={"Range": "bytes=0-524287"},
                        timeout=60, stream=True)
                    out["tunnel_status"] = r2.status_code
                    out["tunnel_bytes_head"] = len(
                        next(r2.iter_content(65536), b""))
                    out["ok"] = r2.status_code in (200, 206)
                return jsonify(out)
            except Exception as e:  # noqa: BLE001
                return jsonify({"ok": False,
                                "error": str(e)[:300]})
        if mode == "shen":
            args = ytdlp_base_args(tmp, use_cookies=True,
                                   use_proxy=True)
            args += ["--force-ipv4", "--extractor-args",
                     "youtube:player_client=web,mweb,android"]
            dst = os.path.join(tmp, "shen_out.mp4")
            res = run_cmd(args + [
                "-f", "best[height<=720]/best",
                "--download-sections", "*0-20",
                "--force-keyframes-at-cuts",
                "-o", dst, url,
            ], timeout=240)
            size = (os.path.getsize(dst)
                    if os.path.exists(dst) else 0)
            tail = (res.stderr or res.stdout or "").strip()
            return jsonify({
                "ok": res.returncode == 0 and size > 100000,
                "stage": "shen-download",
                "bytes_written": size,
                "tail": tail[-400:],
            })
        if mode == "sabr":
            args = ytdlp_base_args(tmp, use_cookies=True,
                                   use_proxy=False)
            dst = os.path.join(tmp, "sabr_out.mp4")
            res = run_cmd(args + [
                "--extractor-args", "youtube:formats=sabr",
                "-f", "best[height<=720]/best",
                "--download-sections", "*0-20",
                "--force-keyframes-at-cuts",
                "-o", dst, url,
            ], timeout=240)
            size = (os.path.getsize(dst)
                    if os.path.exists(dst) else 0)
            tail = (res.stderr or res.stdout or "").strip()
            return jsonify({
                "ok": res.returncode == 0 and size > 100000,
                "stage": "sabr-download",
                "bytes_written": size,
                "tail": tail[-400:],
            })
        args = ytdlp_base_args(tmp, use_cookies=use_cookies,
                               use_proxy=False)
        if client != "default":
            args += ["--extractor-args",
                     "youtube:player_client=" + client]
        res = run_cmd(args + ["-J", "--skip-download", url],
                      timeout=120)
        if res.returncode != 0:
            tail = (res.stderr or res.stdout or "").strip()
            return jsonify({"ok": False, "stage": "extract",
                            "error": tail[-400:]})
        info = json.loads(res.stdout)
        fmts = info.get("formats") or []

        def _h(f):
            return f.get("height") or 0

        hls = [{"format_id": f.get("format_id"), "height": _h(f),
                "protocol": f.get("protocol")}
               for f in fmts if "m3u8" in (f.get("protocol") or "")]
        summary = {
            "ok": True,
            "title": (info.get("title") or "")[:100],
            "n_formats": len(fmts),
            "max_h": max((_h(f) for f in fmts), default=0),
            "hls": sorted(hls, key=lambda x: x["height"],
                          reverse=True)[:8],
        }
        if mode == "info":
            return jsonify(summary)
        # ---- mode bytes ----
        fid = data.get("format_id")
        proto = data.get("proto") or "https"
        cand = None
        for f in fmts:
            if fid and f.get("format_id") == fid:
                cand = f
                break
        if cand is None:
            pool = [f for f in fmts if f.get("url")]
            if proto == "hls":
                pool = [f for f in pool
                        if "m3u8" in (f.get("protocol") or "")]
            else:
                pool = [f for f in pool
                        if "m3u8" not in (f.get("protocol") or "")]
            pool = [f for f in pool if _h(f) <= 720] or pool
            pool.sort(key=_h, reverse=True)
            cand = pool[0] if pool else None
        if cand is None:
            return jsonify({**summary, "ok": False,
                            "stage": "pick",
                            "error": "tidak ada format cocok"})
        out = {**summary, "picked": {
            "format_id": cand.get("format_id"), "height": _h(cand),
            "protocol": cand.get("protocol")}}
        try:
            from curl_cffi import requests as creq
            getter = creq.get
            kw = {"impersonate": "chrome"}
        except Exception:
            getter = requests.get
            kw = {}
        hdrs = dict(cand.get("http_headers") or {})
        u = cand["url"]
        if "m3u8" in (cand.get("protocol") or ""):
            r = getter(u, headers=hdrs, timeout=30, **kw)
            out["manifest_status"] = r.status_code
            seg = None
            for ln in (r.text or "").splitlines():
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    seg = ln
                    break
            if seg:
                seg_url = (seg if seg.startswith("http")
                           else u.rsplit("/", 1)[0] + "/" + seg)
                r2 = getter(seg_url, headers=hdrs, timeout=30, **kw)
                out["segment_status"] = r2.status_code
                out["segment_bytes_head"] = len(r2.content[:1024])
            out["ok"] = (out.get("manifest_status") == 200
                         and out.get("segment_status") == 200)
            return jsonify(out)
        hdrs["Range"] = "bytes=0-1048575"
        r = getter(u, headers=hdrs, timeout=30, **kw)
        out["byte_status"] = r.status_code
        out["bytes_head"] = len(r.content[:1024])
        out["ok"] = r.status_code in (200, 206)
        return jsonify(out)
    except Exception as e:  # noqa: BLE001
        return jsonify({"ok": False, "stage": "exception",
                        "error": str(e)[:300]})
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ------------------------------------------------------- jembatan downloader
# Unduh YouTube PENUH untuk web Downloader: resep yang terbukti di box
# InstaBox (web_safari + cookies, HLS) dengan tangga kualitas otomatis
# (diminta -> 720 -> 480 -> 360). Di Render endpoint ini ikut terpasang
# tapi akan gagal oleh gerbang IP di sana; rumahnya memang di box.
YTDL_JOBS = {}
YTDL_LOCK = threading.Lock()
YTDL_DIR = "/tmp/ytdljobs"


_DL_PCT = re.compile(r"(\d+(?:\.\d+)?)% of")
_DL_FRAG = re.compile(r"[Ff]ragment\s+(\d+)\s+of\s+(\d+)")
_DL_SPEED = re.compile(r"at\s+([0-9.]+\s*[KMG]iB/s)")
_DL_ETA = re.compile(r"ETA\s+(?:(\d+):)?(\d+):(\d+)")
_FF_TIME = re.compile(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)")


def _run_dl_stream(args, timeout, job):
    """Jalankan yt-dlp sambil membaca progresnya baris demi baris.

    Mengisi job['progress'] (0-100), job['speed'], job['eta'] (detik)
    secara langsung agar endpoint status bisa melaporkannya ke web —
    inilah "klu" lamanya unduhan: persen + perkiraan sisa dari yt-dlp
    sendiri. Mengembalikan (returncode, tail_output).
    """
    proc = subprocess.Popen(args + ["--newline"], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True,
                            bufsize=1)
    tail = []
    t0 = time.time()
    try:
        for line in proc.stdout:
            tail.append(line)
            if len(tail) > 40:
                tail.pop(0)
            s = line.strip()
            m = _DL_PCT.search(s)
            if m:
                try:
                    job["progress"] = round(float(m.group(1)), 1)
                except Exception:
                    pass
                ms = _DL_SPEED.search(s)
                if ms:
                    job["speed"] = ms.group(1).replace(" ", "")
                me = _DL_ETA.search(s)
                if me:
                    job["eta"] = int(me.group(1) or 0) * 3600 \
                        + int(me.group(2)) * 60 + int(me.group(3))
            else:
                mf = _DL_FRAG.search(s)
                if mf and int(mf.group(2)) > 0:
                    job["progress"] = round(
                        100.0 * int(mf.group(1)) / int(mf.group(2)), 1)
                elif job.get("duration"):
                    mt = _FF_TIME.search(s)
                    if mt:
                        secs = int(mt.group(1)) * 3600 \
                            + int(mt.group(2)) * 60 + float(mt.group(3))
                        job["progress"] = round(min(
                            99.0, 100.0 * secs / job["duration"]), 1)
            if timeout and time.time() - t0 > timeout:
                proc.kill()
                return 124, "".join(tail)
    finally:
        try:
            proc.stdout.close()
        except Exception:
            pass
    return proc.wait(), "".join(tail)


def _ytdl_worker(job):
    jid = job["id"]
    jdir = os.path.join(YTDL_DIR, jid)
    os.makedirs(jdir, exist_ok=True)
    url = job["url"]
    try:
        job["stage"] = "mengambil info video"
        meta = is_meta_url(url)
        if meta:
            # Meta: tanpa extractor-args YouTube, tanpa proxy (tes
            # terbukti lolos direct), cookies dari file server
            # per platform (FB_COOKIES_FILE / IG_COOKIES_FILE).
            cookie_file, platform = cookies_file_for(url)
            if not cookie_file or not os.path.exists(cookie_file):
                raise RuntimeError(
                    "cookies {} belum terpasang di server".format(
                        platform))
            base = ytdlp_base_args(jdir, use_cookies=False,
                                   use_proxy=False,
                                   cookies_path=cookie_file)
            client = []
        else:
            base = ytdlp_base_args(jdir, use_cookies=True, use_proxy=True)
            client = ["--extractor-args",
                      "youtube:player_client=web_safari"]
        res = run_cmd(base + client + ["-J", "--skip-download", url],
                      timeout=120)
        if res.returncode == 0:
            try:
                info = json.loads(res.stdout)
                job["title"] = (info.get("title") or "")[:120]
                job["duration"] = info.get("duration") or 0
            except Exception:
                pass
        if job["mode"] == "audio":
            ladder = [(0, "bestaudio/best")]
        elif meta:
            # Meta: tangga tinggi YouTube malah menurunkan reel
            # vertikal (terbukti: cap 1080 -> 960, tanpa cap -> 1920);
            # ambil format terbaik, resolusi nyata dibaca ffprobe.
            ladder = [(0, "bv+ba/b")]
        else:
            heights = []
            for h in (job["quality"], 720, 480, 360):
                if h and h <= job["quality"] and h not in heights:
                    heights.append(h)
            ladder = [(h, "best[height<={}]/best".format(h))
                      for h in heights]
        last_err = ""
        for h, fsel in ladder:
            if job["mode"] == "audio":
                job["stage"] = "mengunduh audio"
            elif meta:
                job["stage"] = "mengunduh video terbaik"
            else:
                job["stage"] = "mengunduh {}p".format(h)
            job["progress"] = None
            job["speed"] = ""
            job["eta"] = None
            args = base + client + [
                "-f", fsel, "-o", os.path.join(jdir, "media.%(ext)s")]
            if job["mode"] == "audio":
                args += ["-x", "--audio-format", "mp3"]
            args += [url]
            rc, tail = _run_dl_stream(args, 1800, job)
            files = [f for f in glob.glob(os.path.join(jdir, "media.*"))
                     if ".part" not in f]
            if rc == 0 and files:
                job["file"] = os.path.basename(files[0])
                if job["mode"] == "video":
                    pr = run_cmd(
                        ["ffprobe", "-v", "error", "-select_streams",
                         "v:0", "-show_entries", "stream=width,height",
                         "-of", "csv=p=0", files[0]], timeout=30)
                    try:
                        parts = (pr.stdout or "").strip().split(",")
                        w = int(parts[0]); hh = int(parts[1])
                        # Label "p" = sisi pendek: reel vertikal
                        # 1080x1920 -> 1080p, landscape 1920x1080 -> 1080p.
                        job["height"] = min(w, hh) if meta else hh
                    except Exception:
                        job["height"] = h or 0
                job["status"] = "done"
                return
            last_err = tail[-300:]
        raise RuntimeError("semua tangga kualitas gagal: " + last_err)
    except Exception as e:  # noqa: BLE001
        job["status"] = "error"
        job["error"] = str(e)[:300]


@app.post("/api/ytdl")
def ytdl_create():
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if ("youtube.com/watch" not in url and "youtu.be/" not in url
            and "youtube.com/shorts/" not in url
            and not is_meta_url(url)):
        return jsonify({"error": "url youtube/facebook tidak valid"}), 400
    try:
        q = int(data.get("quality") or 720)
    except Exception:
        q = 720
    jid = uuid.uuid4().hex[:12]
    job = {"id": jid, "url": url, "quality": q,
           "mode": "audio" if data.get("mode") == "audio" else "video",
           "status": "running", "title": "", "duration": 0,
           "height": 0, "file": None, "error": None,
           "stage": "antre", "progress": None, "speed": "",
           "eta": None}
    with YTDL_LOCK:
        YTDL_JOBS[jid] = job
    threading.Thread(target=_ytdl_worker, args=(job,),
                     daemon=True).start()
    return jsonify({"id": jid, "status": "running"})


@app.get("/api/ytdl/<jid>")
def ytdl_status(jid):
    with YTDL_LOCK:
        job = YTDL_JOBS.get(jid)
    if not job:
        return jsonify({"error": "tidak ditemukan"}), 404
    out = {k: job[k] for k in ("id", "status", "title", "duration",
                               "height", "mode", "error", "stage",
                               "progress", "speed", "eta")}
    if job["status"] == "done":
        out["file_url"] = "/api/ytdl/file/" + jid
    return jsonify(out)


@app.get("/api/ytdl/file/<jid>")
def ytdl_file(jid):
    with YTDL_LOCK:
        job = YTDL_JOBS.get(jid)
    if not job or job["status"] != "done" or not job.get("file"):
        return jsonify({"error": "belum siap"}), 404
    fname = job["file"]
    ext = os.path.splitext(fname)[1]
    title = re.sub(r"[^\w\-. ]+", "", job["title"] or "youtube")
    title = title.strip()[:80] or "youtube"
    if job["mode"] == "audio":
        dl = title + " - audio.mp3"
    else:
        dl = title + " - {}p{}".format(job["height"] or job["quality"],
                                        ext)
    return send_from_directory(os.path.join(YTDL_DIR, jid), fname,
                               as_attachment=True, download_name=dl)


# ------------------------------------------------------------- cek gerbang
# Tes byte RINGAN untuk pemantau berkala: unduh 5 detik pertama video
# pendek tetap via resep web_safari+cookies. Hasil di-cache 10 menit
# agar endpoint publik ini tidak bisa dipakai menguras kuota.
GATECHECK = {"at": 0, "ok": None, "detail": ""}
GATECHECK_URL = "https://www.youtube.com/watch?v=jNQXAC9IVRw"


@app.get("/api/gatecheck")
def gatecheck():
    now = time.time()
    if GATECHECK["ok"] is not None and now - GATECHECK["at"] < 600:
        return jsonify({"ok": GATECHECK["ok"], "cached": True,
                        "detail": GATECHECK["detail"]})
    tmp = tempfile.mkdtemp(prefix="gate-")
    try:
        base = ytdlp_base_args(tmp, use_cookies=True, use_proxy=True)
        args = base + [
            "--extractor-args", "youtube:player_client=web_safari",
            "-f", "best[height<=360]/best",
            "--download-sections", "*0-5",
            "--force-keyframes-at-cuts",
            "-o", os.path.join(tmp, "g.mp4"), GATECHECK_URL]
        res = run_cmd(args, timeout=120)
        size = 0
        p = os.path.join(tmp, "g.mp4")
        if os.path.exists(p):
            size = os.path.getsize(p)
        ok = res.returncode == 0 and size > 50000
        tail = (res.stderr or "").strip()[-160:]
        GATECHECK.update({"at": now, "ok": ok,
                          "detail": "bytes={} {}".format(size, tail)})
        return jsonify({"ok": ok, "cached": False,
                        "detail": GATECHECK["detail"]})
    except Exception as e:  # noqa: BLE001
        return jsonify({"ok": False, "cached": False,
                        "detail": str(e)[:160]})
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@app.post("/api/detect")
def detect_moments():
    """Deteksi momen (Manual+AI): info + transkrip + heatmap + Gemini
    tanpa merender. Mengembalikan kandidat untuk dipilih user di
    frontend; render dilakukan lewat /api/job mode manual_ai."""
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL video wajib diisi."}), 400
    s = data.get("settings") or {}
    try:
        count = max(1, min(20, int(s.get("clipCount", 10))))
        min_dur = max(3, int(s.get("minDur", 15)))
        max_dur = max(5, int(s.get("maxDur", 30)))
    except Exception:
        return jsonify({"error": "Pengaturan durasi tidak valid."}), 400
    audience = str(s.get("audience", ""))[:120]
    lang = s.get("sttLang") if s.get("sttLang") in LANGS else "auto"
    gemini_key = (data.get("geminiKey") or "").strip() or None
    groq_key = (data.get("groqKey") or "").strip() or None
    job = {"id": "detect", "log": [], "progress": 0,
           "status": "running", "clips": [], "error": None}
    try:
        tmpdir = os.path.join(BASE_DIR, "detect_tmp")
        os.makedirs(tmpdir, exist_ok=True)
        info = fetch_info(job, url, tmpdir)
        title = info.get("title") or "Video"
        duration = float(info.get("duration") or 0)
        if duration <= 0:
            return jsonify({"error": "Durasi video tidak diketahui."}), 400
        segs = fetch_transcript(job, info, tmpdir, lang)
        if not segs and groq_key:
            # tanpa subtitle: STT butuh berkas video, unduh sekali
            src = download_video(job, url, tmpdir, info=info)
            try:
                segs, _w = groq_transcribe(job, src, tmpdir, groq_key,
                                           duration, lang)
            except Exception:
                segs = []
        heat = heatmap_peaks(info, duration, min_dur, max_dur, count)
        heat_info = ""
        if heat:
            heat_info = ", ".join(
                "{:.0f}-{:.0f} (skor {})".format(
                    h["start"], h["end"], h["score"]) for h in heat)
        candidates = []
        if gemini_key:
            candidates = gemini_candidates(
                gemini_key, title, duration, segs, heat_info,
                count, min_dur, max_dur, audience)
        if not candidates and heat:
            candidates = heat
        if not candidates:
            candidates = even_fallback(duration, count, min_dur, max_dur)
        candidates.sort(key=lambda c: c.get("score", 0), reverse=True)
        picked = []
        for c in candidates:
            if any(not (c["end"] <= p["start"] or c["start"] >= p["end"])
                   for p in picked):
                continue
            picked.append(c)
            if len(picked) >= count:
                break
        picked.sort(key=lambda c: c["start"])
        out = []
        for c in picked:
            out.append({
                "start": round(c["start"], 1),
                "end": round(c["end"], 1),
                "hook": c.get("hook", "") or hook_for_segment(
                    segs, c, title),
                "title": c.get("title", ""),
                "desc": c.get("desc", ""),
                "score": c.get("score", 50),
            })
        return jsonify({"title": title, "duration": duration,
                        "candidates": out})
    except Exception as e:  # noqa: BLE001
        return jsonify({"error": str(e)}), 500


@app.post("/api/job")
def create_job():
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL video wajib diisi."}), 400
    s = data.get("settings") or {}

    def _clamp_int(v, lo, hi, default):
        try:
            return max(lo, min(hi, int(v)))
        except Exception:
            return default

    def _pct_box(d):
        if not isinstance(d, dict):
            return None
        return _box_pct(d)

    clips = []
    if isinstance(s.get("clips"), list):
        for mc in s["clips"][:20]:
            if not isinstance(mc, dict):
                continue
            try:
                clips.append({
                    "start": float(mc.get("start", 0)),
                    "end": float(mc.get("end", 0)),
                    "hook": str(mc.get("hook", "") or "")[:200],
                    "title": str(mc.get("title", "") or "")[:120],
                    "desc": str(mc.get("desc", "") or "")[:300],
                    "score": _clamp_int(mc.get("score", 50), 0, 100, 50),
                })
            except Exception:
                continue
    keyframes = []
    if isinstance(s.get("keyframes"), list):
        for k in s["keyframes"][:24]:
            if not isinstance(k, dict):
                continue
            try:
                keyframes.append({"t": float(k.get("t", 0)),
                                  "x": float(k.get("x", 50))})
            except Exception:
                continue
    settings = {
        "clipCount": max(1, min(20, int(s.get("clipCount", 5)))),
        "minDur": max(3, int(s.get("minDur", 15))),
        "maxDur": max(5, int(s.get("maxDur", 30))),
        "ratio": s.get("ratio") if s.get("ratio") in
                 ("9:16", "16:9", "1:1", "9:16-blur") else "9:16",
        "height": int(s.get("height", 720)) or 720,
        "audience": str(s.get("audience", ""))[:120],
        "teaser": bool(s.get("teaser", True)),
        "teaserSeconds": max(1, min(10, int(s.get("teaserSeconds", 3)))),
        "onlyHighScore": bool(s.get("onlyHighScore", False)),
        "captions": bool(s.get("captions", True)),
        "captionStyle": s.get("captionStyle") if s.get("captionStyle") in
                        ("pop", "klasik", "kotak") else "pop",
        # --- v1.5: mode & pilihan klip ---
        "mode": s.get("mode") if s.get("mode") in
                ("manual", "manual_ai") else "auto",
        "clips": clips,
        "keyframes": keyframes,
        # --- v1.5: bahasa transkripsi ---
        "sttLang": s.get("sttLang") if s.get("sttLang") in LANGS
                    else "auto",
        # --- v1.5: caption editor ---
        "capColor": str(s.get("capColor", ""))[:7],
        "capSizePct": _clamp_int(s.get("capSizePct", 100), 70, 150, 100),
        "capChunk": _clamp_int(s.get("capChunk", 4), 1, 8, 4),
        "capPos": s.get("capPos") if s.get("capPos") in
                  ("bawah", "tengah", "atas") else "bawah",
        "capGlow": bool(s.get("capGlow", False)),
        "capBox": bool(s.get("capBox", False)),
        # --- v1.5: judul ---
        "titleMode": s.get("titleMode") if s.get("titleMode") in
                      ("off", "awal", "penuh") else "off",
        "titleStyle": s.get("titleStyle") if s.get("titleStyle") in
                      ("bar", "teks") else "bar",
        # --- v1.5: watermark ---
        "watermarkText": str(s.get("watermarkText", ""))[:40],
        "watermarkPos": s.get("watermarkPos") if s.get("watermarkPos") in
                        ("kanan-bawah", "kiri-bawah", "kanan-atas",
                         "kiri-atas") else "kanan-bawah",
        "watermarkOpacity": _clamp_int(s.get("watermarkOpacity", 60),
                                       10, 100, 60),
        "watermarkSize": _clamp_int(s.get("watermarkSize", 28),
                                    16, 64, 28),
        "watermarkImagePath": None,
        # --- v1.5: tracking / split / crop ---
        "faceTrack": bool(s.get("faceTrack", False)),
        "splitMode": bool(s.get("splitMode", False)),
        "splitFace": s.get("splitFace") if s.get("splitFace") in
                     ("atas", "bawah") else "bawah",
        "splitFaceH": _clamp_int(s.get("splitFaceH", 35), 20, 50, 35),
        "splitFaceBox": _pct_box(s.get("splitFaceBox") or {}),
        "splitGameBox": _pct_box(s.get("splitGameBox") or {}),
        "cropBox": _pct_box(s.get("cropBox") or {}),
    }
    if settings["maxDur"] < settings["minDur"]:
        settings["maxDur"] = settings["minDur"]
    gemini_key = (data.get("geminiKey") or "").strip() or None
    groq_key = (data.get("groqKey") or "").strip() or None

    cleanup_old_jobs()
    job = new_job()
    # watermark gambar: simpan sekali ke /tmp, dihapus saat job selesai
    wm_data = data.get("watermarkImage") or ""
    if isinstance(wm_data, str) and wm_data.startswith("data:image/"):
        try:
            import base64
            header, b64 = wm_data.split(",", 1)
            raw = base64.b64decode(b64, validate=True)
            ext = "png"
            if "jpeg" in header or "jpg" in header:
                ext = "jpg"
            elif "webp" in header:
                ext = "webp"
            if len(raw) <= 5 * 1024 * 1024 and raw[:4] in (
                    b"\x89PNG", b"\xff\xd8\xff\xe0", b"\xff\xd8\xff\xe1",
                    b"RIFF"):
                path = os.path.join(
                    tempfile.gettempdir(),
                    "wm_{}.{}".format(job["id"], ext))
                with open(path, "wb") as fh:
                    fh.write(raw)
                settings["watermarkImagePath"] = path
        except Exception:
            settings["watermarkImagePath"] = None
    t = threading.Thread(
        target=process_job, args=(job, url, gemini_key, settings, groq_key),
        daemon=True,
    )
    t.start()
    return jsonify({"jobId": job["id"]})


@app.get("/api/job/<job_id>")
def job_status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        # job mungkin dari proses lama; cek folder
        return jsonify({"error": "Job tidak ditemukan."}), 404
    return jsonify({
        "status": job["status"],
        "progress": job["progress"],
        "log": job["log"],
        "clips": job["clips"],
        "error": job["error"],
    })


@app.get("/files/<job_id>/<path:filename>")
def serve_file(job_id, filename):
    job_dir = os.path.join(BASE_DIR, job_id)
    return send_from_directory(job_dir, filename)


if __name__ == "__main__":
    os.makedirs(BASE_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", "5001"))
    app.run(host="0.0.0.0", port=port)
