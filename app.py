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
import subprocess
import sys
import threading
import time
import uuid

import requests
from flask import Flask, jsonify, request, send_from_directory

APP_VER = "1.1"
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


def ytdlp_base_args(job_dir, use_cookies=True, use_proxy=False):
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
    ("web_embedded", True),
    ("tv_downgraded", True),
    ("web_creator", True),
    ("mweb", True),
    ("tv", True),
]


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
              attempts=None, use_proxy=False):
    """Jalankan yt-dlp melewati daftar percobaan (klien, pakai cookies).

    Error apa pun -> percobaan berikutnya. verbose=True menyaring
    baris terkait PO Token dari stderr ke log (diagnosis bgutil).
    Percobaan yang menang dicatat di job["win_attempt"].
    """
    last_tail = "kesalahan tidak diketahui"
    for client, use_cookies in (attempts or ATTEMPTS):
        if use_cookies and not os.environ.get(
                "YOUTUBE_COOKIES", "").strip():
            continue
        args = ytdlp_base_args(job_dir, use_cookies=use_cookies,
                               use_proxy=use_proxy)
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
                    verbose=True)
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


def fetch_transcript(job, info, job_dir):
    """Unduh subtitle (id diutamakan, lalu en) -> segmen ber-timestamp."""
    subs = {}
    for key in ("subtitles", "automatic_captions"):
        for lang, tracks in (info.get(key) or {}).items():
            subs.setdefault(lang, tracks)
    chosen = None
    for want in ("id", "en"):
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


def _http_download(job, fmt_entry, dest):
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
               if PROXY_URL else None)
    with requests.get(fmt_entry["url"], headers=headers, stream=True,
                      timeout=60, proxies=proxies) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
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
        _http_download(job, f, dest)
        return dest
    if videos and audios:
        v, a = videos[0], audios[0]
        log(job, "Unduh langsung format video {} ({}p, {}) + audio {}."
            .format(v.get("format_id"), h(v), v.get("protocol"),
                    a.get("format_id")))
        vpath = os.path.join(job_dir, "source_v.bin")
        apath = os.path.join(job_dir, "source_a.bin")
        _http_download(job, v, vpath)
        _http_download(job, a, apath)
        dest = os.path.join(job_dir, "source.mp4")
        res = run_cmd(["ffmpeg", "-y", "-i", vpath, "-i", apath,
                       "-c", "copy", dest], timeout=600)
        if res.returncode != 0 or not os.path.exists(dest):
            raise RuntimeError("Gagal menggabungkan video+audio.")
        return dest
    raise RuntimeError("Info video tidak membawa format yang bisa "
                       "diunduh langsung.")


def download_video(job, url, job_dir, info=None):
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
    run_ytdlp(job, job_dir, [
        "-f", fmt,
        "--merge-output-format", "mp4",
        "-o", out,
        url,
    ], timeout=1200, errlabel="yt-dlp gagal mengunduh video",
        attempts=attempts, use_proxy=True)
    for name in sorted(os.listdir(job_dir)):
        if name.startswith("source."):
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
        '"score": 95}}]\n'
        "hook = kalimat pancingan pendek Bahasa Indonesia yang membuat "
        "penasaran. score = 0-100."
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


# ---------------------------------------------------------------- ffmpeg
def sanitize_drawtext(text):
    """Bersihkan teks agar aman untuk filter drawtext ffmpeg."""
    text = text.replace("\\", " ").replace(":", " ").replace("'", " ")
    text = text.replace('"', " ").replace("%", " ").replace("[", " ")
    text = text.replace("]", " ").replace(";", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", text).strip()[:90]


def build_filter(ratio, height, teaser_text, teaser_seconds):
    """Susun -vf untuk crop rasio + skala + overlay teaser opsional."""
    parts = []
    if ratio == "9:16":
        parts.append("crop=ih*9/16:ih")
        # "height" = lebar target untuk video vertikal: 720 -> 720x1280
        w = int(height)
        h = int(w * 16 / 9)
        h -= h % 2
        parts.append("scale={}:{}".format(w, h))
    elif ratio == "1:1":
        parts.append("crop=ih:ih")
        parts.append("scale={}:{}".format(height, height))
    else:  # 16:9 tanpa crop, skala berdasarkan tinggi
        parts.append("scale=-2:{}".format(height))
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
    return ",".join(parts)


def output_dims(ratio, height):
    """Dimensi piksel keluaran, sama seperti yang dihasilkan
    build_filter."""
    height = int(height)
    if ratio == "9:16":
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


def build_clip_ass(segs, cand, settings, job_dir, idx):
    """Buat berkas .ass caption viral per-kata untuk satu klip.

    Kata-kata dari segmen transkrip yang tumpang tindih dengan jendela
    klip dikelompokkan 3-4 kata per baris; tiap kata diberi tag karaoke
    \\kf sehingga kata yang sedang diucapkan menyala (gaya klip viral).
    Waktu per kata ditimbang dari panjang katanya di dalam segmen.
    Mengembalikan path berkas, atau None bila tak ada teks.
    """
    words = []  # (start_rel, end_rel, text)
    for s in segs:
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
            words.append((t - cand["start"], t - cand["start"] + d, w))
            t += d
    if not words:
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
         "100,0,0,{},{},1,2,24,24,{},1").format(
             fsize, primary, secondary, back, border, outline, margin_v),
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

    for item in words:
        if chunk and (len(chunk) >= 4 or item[1] - chunk_start > 2.4):
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


def cut_clip(src, dst, cand, settings, ass_path=None):
    """Potong satu segmen dengan ffmpeg. True bila sukses."""
    teaser = cand.get("hook") if settings.get("teaser") else None
    variants = []
    vf = build_filter(settings["ratio"], settings["height"],
                      teaser, settings.get("teaserSeconds", 3))
    if ass_path:
        vf += "," + _subtitles_filter(ass_path)
    variants.append(vf)
    if teaser:  # cadangan: tanpa teaser, caption tetap
        vf2 = build_filter(settings["ratio"], settings["height"], None, 0)
        if ass_path:
            vf2 += "," + _subtitles_filter(ass_path)
        variants.append(vf2)
    variants.append(build_filter(settings["ratio"], settings["height"],
                                 None, 0))
    for vf in variants:
        cmd = [
            "ffmpeg", "-y",
            "-ss", "{:.2f}".format(cand["start"]),
            "-to", "{:.2f}".format(cand["end"]),
            "-i", src,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast",
            "-c:a", "aac", "-movflags", "+faststart",
            dst,
        ]
        res = run_cmd(cmd, timeout=900)
        if res.returncode == 0 and os.path.exists(dst):
            return True
    return False


# ---------------------------------------------------------------- pipeline
def process_job(job, url, gemini_key, settings):
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
        segs = fetch_transcript(job, info, job_dir)
        if segs:
            log(job, "Transkrip ditemukan ({} segmen).".format(len(segs)))
        else:
            log(job, "Transkrip tidak tersedia.")
        set_progress(job, 50)

        count = settings["clipCount"]
        min_dur = settings["minDur"]
        max_dur = settings["maxDur"]

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
        if gemini_key:
            log(job, "AI menganalisis momen…")
            candidates = gemini_candidates(
                gemini_key, title, duration, segs, heat_info,
                count, min_dur, max_dur, settings.get("audience", ""),
            )
            if candidates:
                log(job, "AI memilih {} momen.".format(len(candidates)))
            else:
                log(job, "AI tidak memberi hasil, memakai cadangan.")
        if not candidates and heat:
            candidates = heat
            log(job, "Memakai puncak Most Replayed.")
        if not candidates:
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
        if settings.get("onlyHighScore"):
            picked = [c for c in picked if c.get("score", 0) >= 70] or picked[:1]
        picked.sort(key=lambda c: c["start"])
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
                                              job_dir, i)
                except Exception:
                    ass_path = None
            if not cut_clip(src, dst, cand, settings, ass_path=ass_path):
                log(job, "Klip {} gagal dipotong, dilewati.".format(i))
                continue
            job["clips"].append({
                "file": fname,
                "url": "/files/{}/{}".format(job["id"], fname),
                "start": round(cand["start"], 1),
                "end": round(cand["end"], 1),
                "hook": cand["hook"],
                "score": cand.get("score", 50),
            })
            set_progress(job, 60 + int(40 * i / total))

        if not job["clips"]:
            raise RuntimeError("Semua klip gagal dipotong.")
        job["status"] = "done"
        set_progress(job, 100)
        log(job, "Selesai. {} klip dibuat.".format(len(job["clips"])))
    except Exception as e:  # noqa: BLE001
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


@app.post("/api/job")
def create_job():
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL video wajib diisi."}), 400
    s = data.get("settings") or {}
    settings = {
        "clipCount": max(1, min(20, int(s.get("clipCount", 5)))),
        "minDur": max(3, int(s.get("minDur", 15))),
        "maxDur": max(5, int(s.get("maxDur", 30))),
        "ratio": s.get("ratio") if s.get("ratio") in
                 ("9:16", "16:9", "1:1") else "9:16",
        "height": int(s.get("height", 720)) or 720,
        "audience": str(s.get("audience", ""))[:120],
        "teaser": bool(s.get("teaser", True)),
        "teaserSeconds": max(1, min(10, int(s.get("teaserSeconds", 3)))),
        "onlyHighScore": bool(s.get("onlyHighScore", False)),
        "captions": bool(s.get("captions", True)),
        "captionStyle": s.get("captionStyle") if s.get("captionStyle") in
                        ("pop", "klasik", "kotak") else "pop",
    }
    if settings["maxDur"] < settings["minDur"]:
        settings["maxDur"] = settings["minDur"]
    gemini_key = (data.get("geminiKey") or "").strip() or None

    cleanup_old_jobs()
    job = new_job()
    t = threading.Thread(
        target=process_job, args=(job, url, gemini_key, settings),
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
