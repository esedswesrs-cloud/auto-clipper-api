# Auto Clipper — Backend

API Flask yang memotong video YouTube menjadi klip pendek (Shorts/Reels)
secara otomatis.

## Cara kerja

1. Frontend mengirim `POST /api/job` berisi URL video + pengaturan.
2. Backend mengambil info video dengan **yt-dlp** (termasuk data
   *Most Replayed*/heatmap dan transkrip bila ada).
3. Kandidat momen dipilih dengan urutan prioritas:
   - **Gemini** (bila API key dikirim): memilih momen + menulis hook.
   - Puncak **heatmap** Most Replayed.
   - Fallback: segmen merata sepanjang video.
4. **ffmpeg** memotong tiap segmen, crop ke rasio yang dipilih
   (9:16 / 16:9 / 1:1), skala ke resolusi target, dan (opsional)
   membubuhkan teks teaser/hook di detik-detik pertama.
5. Hasil dilayani lewat `GET /files/<jobId>/clipN.mp4`.

## Endpoint

| Method | Path | Keterangan |
|---|---|---|
| GET | `/` | status + versi |
| POST | `/api/job` | buat job baru, balas `{jobId}` |
| GET | `/api/job/<id>` | status, progress, log, daftar klip |
| GET | `/files/<jobId>/<file>` | unduh/putar klip hasil |

## Menjalankan lokal

```bash
pip install -r requirements.txt
python app.py          # http://localhost:5001
```

## Deploy (Render)

Repo ini berisi `Dockerfile` + `render.yaml` (web service Docker, plan
free). Variabel lingkungan opsional:

- `YOUTUBE_COOKIES` — isi file cookies Netscape untuk YouTube, dipakai
  bila unduhan dari IP datacenter kena bot-check.

## Catatan

- Folder kerja job: `/tmp/clipjobs/<jobId>`; job lama (>2 jam) dibersihkan
  secara oportunistik. Di Render free tier filesystem bersifat sementara.
- API key Gemini dikirim per-job dari frontend (disimpan di browser
  pengguna), tidak disimpan di server.
