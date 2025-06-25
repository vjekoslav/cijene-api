# Multi-stage build for crawler/API services
FROM python:3.13-slim AS base

# Add labels for better container management
LABEL org.opencontainers.image.title="Cijene API"
LABEL org.opencontainers.image.description="Croatian grocery price tracking service"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.authors="Cijene API Team"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Europe/Zagreb \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    wget \
    unzip \
    ca-certificates \
    tzdata \
    cron \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install uv (pinned version for reproducibility)
RUN pip install uv==0.5.8

# Dependencies stage
FROM base AS deps
WORKDIR /app

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock ./

# Install production dependencies
RUN uv sync --frozen --no-dev

# Development stage (with dev dependencies)
FROM deps AS development
# Install all dependencies including dev
RUN uv sync --frozen

# Add development user for consistency
RUN useradd --create-home --shell /bin/bash --uid 1001 appuser && \
    chown -R appuser:appuser /app && \
    mkdir -p /app/data /app/output && \
    chown -R appuser:appuser /app/data /app/output

USER appuser

# Copy application code for development
COPY --chown=appuser:appuser . .

# Expose port
EXPOSE 8000

# Default development command
CMD ["uv", "run", "-m", "service.main", "--reload"]

# Production stage
FROM deps AS production
WORKDIR /app

# Copy application code
COPY . .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash --uid 1001 --no-log-init appuser && \
    chown -R appuser:appuser /app && \
    mkdir -p /app/data /app/output && \
    chown -R appuser:appuser /app/data /app/output

# Switch to non-root user
USER appuser

# Expose port (used by API service)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command (will be overridden in docker-compose.yml)
CMD ["uv", "run", "-m", "service.main"]