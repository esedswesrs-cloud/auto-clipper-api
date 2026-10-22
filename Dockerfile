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

# wireproxy (SOCKS5 userspace dari tunnel WireGuard) + wgcf
# (pembuat profil Cloudflare WARP) — egress bersih untuk YouTube.
RUN curl -fsSL https://github.com/windtf/wireproxy/releases/download/v1.1.3/wireproxy_linux_amd64.tar.gz -o /tmp/wireproxy.tar.gz \
    && tar -xzf /tmp/wireproxy.tar.gz -C /usr/local/bin wireproxy \
    && rm /tmp/wireproxy.tar.gz \
    && curl -fsSL https://github.com/ViRb3/wgcf/releases/download/v2.3.1/wgcf_2.3.1_linux_amd64 -o /usr/local/bin/wgcf \
    && chmod +x /usr/local/bin/wgcf /usr/local/bin/wireproxy

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY start.sh .

ENV PORT=10000
EXPOSE 10000
CMD ["sh", "/app/start.sh"]
