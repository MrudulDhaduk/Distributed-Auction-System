# GroundHog, the distributed auction app: the web dashboard, the LLM server and the three Raft nodes,
# all in one container. The dashboard starts the others as child processes, so
# its Kill -9 / Suspend / Resume / Restart buttons work as on the host
# (SIGKILL / SIGSTOP / SIGCONT). The model itself runs in the ollama container
# (docker-compose.yml); the LLM server reaches it through OLLAMA_HOST.
#
#   docker compose up --build        then open http://localhost:8765

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first, so editing the code does not reinstall them.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8765

# Ctrl+C semantics on `docker stop`: the dashboard stops the nodes it started.
STOPSIGNAL SIGINT

CMD ["python", "-m", "dashboard", "--host", "0.0.0.0", "--port", "8765", "--llm", "real"]
