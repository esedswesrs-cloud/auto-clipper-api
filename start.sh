#!/bin/sh
# Skrip mulai container Auto Clipper:
# 1) Siapkan egress Cloudflare WARP via wireproxy (SOCKS5 lokal).
#    Profil WARP dibuat sekali (wgcf) lalu dipakai ulang selama
#    instance hidup; registrasi ulang hanya saat deploy baru.
# 2) Jalankan server bgutil (generator PO Token) di port 4416.
# 3) Jalankan gunicorn (aplikasi utama).
set -e

WARP_DIR=/tmp/warpdata
mkdir -p "$WARP_DIR"
if [ ! -f "$WARP_DIR/wireproxy.conf" ]; then
  cd "$WARP_DIR"
  wgcf register --accept-tos > wgcf.log 2>&1 || true
  wgcf generate >> wgcf.log 2>&1 || true
  if [ -f wgcf-profile.conf ]; then
    cat wgcf-profile.conf > wireproxy.conf
    printf '\n[Socks5]\nBindAddress = 127.0.0.1:1080\n' >> wireproxy.conf
  fi
fi
if [ -f "$WARP_DIR/wireproxy.conf" ]; then
  wireproxy -c "$WARP_DIR/wireproxy.conf" > /tmp/wireproxy.log 2>&1 &
fi

node /opt/bgutil/server/build/main.js --port 4416 > /tmp/bgutil.log 2>&1 &

sleep 4
cd /app
exec gunicorn -w 1 -t 1800 --bind "0.0.0.0:${PORT:-10000}" app:app
