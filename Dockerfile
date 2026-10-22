FROM python:3.12-slim

# ffmpeg untuk potong video; Node.js + git untuk server bgutil
# (generator PO Token YouTube — token bukti penonton asli yang
# membuka format video untuk yt-dlp di IP datacenter).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg fonts-dejavu-core git curl xz-utils ca-certificates unzip \
    && curl -fsSL https://nodejs.org/dist/v24.20.0/node-v24.20.0-linux-x64.tar.xz -o /tmp/node.tar.xz \
    && tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1 \
    && rm /tmp/node.tar.xz \
    && rm -rf /var/lib/apt/lists/*

# Deno: JS runtime untuk yt-dlp (penyelesai tantangan n-challenge
# YouTube; tanpa ini format video bisa kosong di server).
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh \
    && deno --version

# Server bgutil: dibangun sekali saat build, jalan di port 4416
# berdampingan dengan gunicorn; plugin yt-dlp menemukannya otomatis.
RUN git clone --depth 1 https://github.com/Brainicism/bgutil-ytdlp-pot-provider /opt/bgutil \
    && cd /opt/bgutil/server \
    && npm install --no-audit --no-fund \
    && npx tsc

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV PORT=10000
EXPOSE 10000
CMD ["sh", "-c", "node /opt/bgutil/server/build/main.js --port 4416 > /tmp/bgutil.log 2>&1 & sleep 3; gunicorn -w 1 -t 1800 --bind 0.0.0.0:${PORT:-10000} app:app"]
