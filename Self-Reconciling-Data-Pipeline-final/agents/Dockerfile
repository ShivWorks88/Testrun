# Private Cloud Run service "reconcile-diagnosis": ADK API server with TWO apps, both without tools —
# reconcile_agents (Diagnosis, stage 3b) and reconcile_assistant (read-only console assistant, Phase 6).
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY reconcile_agents ./agents/reconcile_agents
COPY reconcile_assistant ./agents/reconcile_assistant
ENV GOOGLE_GENAI_USE_VERTEXAI=TRUE GOOGLE_CLOUD_LOCATION=asia-south1 PORT=8080
# no web UI: only the API (/apps/.../sessions, /run); Cloud Run requires an ID token (no public access)
CMD exec adk api_server --host 0.0.0.0 --port ${PORT} /app/agents
