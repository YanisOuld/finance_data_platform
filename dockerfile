# --- Stage 1: build the React/Vite frontend ---------------------------------
# Kept separate so the final image doesn't need Node at all -- only the
# built static output (frontend/dist) is copied over below.
FROM node:20-slim AS frontend-build

WORKDIR /app/frontend

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

COPY frontend ./
RUN npm run build

# --- Stage 2: the actual API image -------------------------------------------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
	&& pip install --no-cache-dir -r requirements.txt

COPY alembic.ini .
COPY alembic ./alembic
COPY src ./src
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

EXPOSE 8000

# Migrations run on every container start instead of being a manual step
# someone has to remember before deploying -- alembic upgrade head is a
# no-op if the schema is already current.
CMD ["sh", "-c", "alembic upgrade head && uvicorn src.main:app --host 0.0.0.0 --port 8000"]
