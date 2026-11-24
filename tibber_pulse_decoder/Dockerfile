# https://developers.home-assistant.io/docs/apps/configuration#app-dockerfile
ARG BUILD_FROM=ghcr.io/home-assistant/base-python:3.12-alpine3.24
FROM ${BUILD_FROM}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pulse_decoder.py .
COPY run.sh /
RUN chmod a+x /run.sh

# unbuffered so the app Log tab shows output promptly
ENV PYTHONUNBUFFERED=1

ARG BUILD_VERSION
LABEL \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.type="app" \
  io.hass.arch="aarch64|amd64" \
  org.opencontainers.image.title="Home Assistant App: Tibber Pulse HAN decoder" \
  org.opencontainers.image.source="https://github.com/taraldb/tibber-pulse-decoder"

CMD [ "/run.sh" ]
