# Multi-stage build for crawler/API services
FROM python:3.13-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install uv
RUN pip install uv

# Dependencies stage
FROM base AS deps
WORKDIR /app

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Development stage (with dev dependencies)
FROM deps AS development
RUN uv sync --frozen

# Production stage
FROM deps AS production
WORKDIR /app

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash --uid 1001 appuser && \
    chown -R appuser:appuser /app && \
    mkdir -p /app/data /app/output && \
    chown -R appuser:appuser /app/data /app/output

USER appuser

# Expose port (used by API service)
EXPOSE 8000

# Default command (will be overridden in docker-compose.yml)
CMD ["uv", "run", "--help"] 