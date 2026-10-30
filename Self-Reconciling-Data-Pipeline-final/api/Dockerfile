# Console + API image for Cloud Run. Build from the repo root:
#   docker build -f api/Dockerfile .
# Stage 1 builds the React console; stage 2 is the FastAPI service that also serves it.

FROM node:22-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8080
WORKDIR /app
COPY api/pyproject.toml ./
COPY api/app ./app
RUN pip install --no-cache-dir . && useradd --uid 10001 --no-create-home appuser
COPY policy/policy.yml /app/policy/policy.yml
COPY agents/registry.yml /app/agents/registry.yml
COPY --from=web /web/dist /app/static
ENV POLICY_FILE=/app/policy/policy.yml REGISTRY_FILE=/app/agents/registry.yml STATIC_DIR=/app/static
USER appuser
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*'
