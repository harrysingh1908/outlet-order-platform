# ==========================================
# Stage 1: Build & Dependency Stage
# ==========================================
FROM python:3.13-slim AS builder

WORKDIR /build

# Copy requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install dependencies into user directory
RUN pip install --no-cache-dir --user -r requirements.txt

# ==========================================
# Stage 2: Final Minimal Runtime Stage
# ==========================================
FROM python:3.13-slim AS runner

WORKDIR /app

# Create a dedicated non-root user and group for security (Hadolint rule DL3002)
RUN groupadd -r appgroup && useradd -r -g appgroup -u 1001 appuser

# Copy installed Python packages from builder stage
COPY --from=builder /root/.local /home/appuser/.local

# Copy application source code
COPY src/ /app/src/

# Set environment paths and ensure unbuffered logging
ENV PATH=/home/appuser/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Ensure ownership belongs to non-root user
RUN chown -R appuser:appgroup /app /home/appuser

# Switch to non-root user
USER appuser

# Start Kitchen Worker consumer loop
CMD ["python", "-m", "src.worker.worker"]
