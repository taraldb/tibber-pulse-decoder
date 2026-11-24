# Plain image for the Unraid deploy (scripts/deploy.sh). The HA app builds from
# tibber_pulse_decoder/Dockerfile instead; both use the same pulse_decoder.py.
FROM python:3.12-slim

WORKDIR /app

COPY tibber_pulse_decoder/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tibber_pulse_decoder/pulse_decoder.py .

# unbuffered so `docker logs` shows output promptly
ENV PYTHONUNBUFFERED=1

CMD ["python", "pulse_decoder.py"]
