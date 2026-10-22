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

APP_VER = "1.0"
BASE_DIR = os.environ.get("CLIPJOBS_DIR", "/tmp/clipjobs")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash:generateContent"
)

app = Flask(__name__)

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


def ytdlp_base_args(job_dir):
    """Argumen dasar yt-dlp, termasuk cookies dari env YOUTUBE_COOKIES."""
    args = [sys.executable, "-m", "yt_dlp", "--no-playlist", "--no-warnings"]
    cookies_content = os.environ.get("YOUTUBE_COOKIES", "").strip()
    if cookies_content:
        cpath = os.path.join(job_dir, "cookies.txt")
        with open(cpath, "w", encoding="utf-8") as f:
            f.write(_cookies_to_netscape(cookies_content))
        args += ["--cookies", cpath]
    return args


# Urutan strategi klien YouTube untuk mengakali pembatasan di IP
# datacenter: None = bawaan yt-dlp, sisanya dicoba bergantian setiap
# error. android_vr & mediaconnect dikenal sering memberi format tanpa
# token PO; tv butuh cookies login.
PLAYER_CLIENTS = [None, "tv", "android_vr", "mediaconnect", "ios",
                  "mweb", "web_safari"]


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


def run_ytdlp(job, job_dir, tail_args, timeout, errlabel, verbose=False):
    """Jalankan yt-dlp; error apa pun -> coba player client berikutnya.

    YouTube di IP datacenter rewelnya beda-beda per klien (bot-check,
    format hilang, page reload, player response kosong) dan berubah-ubah,
    jadi semua kegagalan dianggap layak dicoba ulang dengan klien lain.
    verbose=True menyaring baris terkait PO Token dari stderr ke log
    (diagnosis bgutil).
    """
    last_tail = "kesalahan tidak diketahui"
    for client in PLAYER_CLIENTS:
        args = ytdlp_base_args(job_dir)
        if verbose:
            args += ["--verbose"]
        if client:
            args += ["--extractor-args",
                     "youtube:player_client=" + client]
        res = run_cmd(args + tail_args, timeout=timeout)
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
                    "(klien " + (client or "bawaan") + ")")
        if res.returncode == 0:
            if client:
                log(job, "Lolos via klien " + client + ".")
            return res
        tail = (res.stderr or res.stdout or "").strip().splitlines()
        last_tail = tail[-1] if tail else last_tail
        label = client or "bawaan"
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


def download_video(job, url, job_dir):
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
    ], timeout=1200, errlabel="yt-dlp gagal mengunduh video")
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


def cut_clip(src, dst, cand, settings):
    """Potong satu segmen dengan ffmpeg. True bila sukses."""
    vf = build_filter(
        settings["ratio"], settings["height"],
        cand.get("hook") if settings.get("teaser") else None,
        settings.get("teaserSeconds", 3),
    )
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
    # bila gagal (mis. drawtext), coba lagi tanpa overlay teks
    if settings.get("teaser"):
        vf2 = build_filter(settings["ratio"], settings["height"], None, 0)
        cmd2 = [
            "ffmpeg", "-y",
            "-ss", "{:.2f}".format(cand["start"]),
            "-to", "{:.2f}".format(cand["end"]),
            "-i", src,
            "-vf", vf2,
            "-c:v", "libx264", "-preset", "veryfast",
            "-c:a", "aac", "-movflags", "+faststart",
            dst,
        ]
        res2 = run_cmd(cmd2, timeout=900)
        return res2.returncode == 0 and os.path.exists(dst)
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
        src = download_video(job, url, job_dir)
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

        total = len(picked)
        for i, cand in enumerate(picked, start=1):
            cand["hook"] = hook_for_segment(segs, cand, title)
            log(job, "Memotong klip {}/{}…".format(i, total))
            fname = "clip{}.mp4".format(i)
            dst = os.path.join(job_dir, fname)
            if not cut_clip(src, dst, cand, settings):
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
