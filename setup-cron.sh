#!/bin/bash
# Setup script for installing crawler cron jobs
# Run with: ./setup-cron.sh

set -e

# Get the absolute path to the project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find docker-compose binary
DOCKER_COMPOSE_PATH=$(which docker-compose || which docker compose || echo "")

if [ -z "$DOCKER_COMPOSE_PATH" ]; then
    echo "Error: docker-compose not found in PATH"
    echo "Please install docker-compose first"
    exit 1
fi

# If using 'docker compose' (newer syntax), adjust the command
if [[ "$DOCKER_COMPOSE_PATH" == *"docker"* ]] && ! [[ "$DOCKER_COMPOSE_PATH" == *"docker-compose"* ]]; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    DOCKER_COMPOSE_CMD="$DOCKER_COMPOSE_PATH"
fi

echo "Project directory: $PROJECT_DIR"
echo "Docker Compose command: $DOCKER_COMPOSE_CMD"

# Create the cron entries
CRON_ENTRY_MORNING="0 9 * * * cd $PROJECT_DIR && $DOCKER_COMPOSE_CMD run --rm crawler >> /var/log/cijene-crawler.log 2>&1"
CRON_ENTRY_EVENING="0 18 * * * cd $PROJECT_DIR && $DOCKER_COMPOSE_CMD run --rm crawler >> /var/log/cijene-crawler.log 2>&1"

echo ""
echo "The following cron entries will be added:"
echo "$CRON_ENTRY_MORNING"
echo "$CRON_ENTRY_EVENING"
echo ""

# Check if running as root or if sudo is available
if [ "$EUID" -eq 0 ]; then
    CRON_USER="root"
    CRON_CMD="crontab"
elif command -v sudo >/dev/null 2>&1; then
    CRON_USER="root (via sudo)"
    CRON_CMD="sudo crontab"
else
    CRON_USER="current user"
    CRON_CMD="crontab"
fi

echo "Installing cron jobs for: $CRON_USER"
echo ""

# Get existing crontab (if any) and add new entries
TEMP_CRON=$(mktemp)
($CRON_CMD -l 2>/dev/null || true) | grep -v "cijene-crawler" > "$TEMP_CRON" || true

# Add new entries
echo "# Cijene API Crawler Jobs" >> "$TEMP_CRON"
echo "$CRON_ENTRY_MORNING" >> "$TEMP_CRON"
echo "$CRON_ENTRY_EVENING" >> "$TEMP_CRON"

# Install the new crontab
$CRON_CMD "$TEMP_CRON"

# Cleanup
rm "$TEMP_CRON"

echo "✅ Cron jobs installed successfully!"
echo ""
echo "To verify installation:"
echo "  $CRON_CMD -l"
echo ""
echo "To view logs:"
echo "  tail -f /var/log/cijene-crawler.log"
echo ""
echo "To remove these cron jobs later:"
echo "  $CRON_CMD -e"
echo "  (then delete the lines containing 'cijene-crawler')"