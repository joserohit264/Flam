# 🐍 Lightweight base image
FROM python:3.11-alpine

# Set working directory inside the container
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies (none external, but we’ll include sqlite3 tools)
RUN apk add --no-cache bash sqlite

# Ensure our main script is executable
RUN chmod +x queuectl.py demo.sh

# Create data directory for persistence
RUN mkdir -p /root/.queuectl

# Default command: show help
ENTRYPOINT ["./queuectl.py"]
CMD ["--help"]
