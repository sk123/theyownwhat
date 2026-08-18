#!/bin/bash
# =============================================================================
# Antigravity IDE Nightly Auto-Updater Script
# =============================================================================
# Runs nightly via cron to check, log, and install updates for:
#   1. Antigravity IDE (direct tarball from antigravity.google)
#   2. @google/gemini-cli (npm)
#   3. google-antigravity Python SDK (pip)
# =============================================================================

set -uo pipefail

export PATH="/home/sk/.nvm/versions/node/v22.22.0/bin:$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin"

LOG_DIR="/home/sk/dev/theyownwhat/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/antigravity_update.log"

INSTALL_DIR="/home/sk/apps/Antigravity-IDE"
BACKUP_DIR="/home/sk/apps/Antigravity-IDE-backup"
DOWNLOAD_BASE="https://edgedl.me.gvt1.com/edgedl/release2/j0qc3/antigravity/stable"
VERSION_CHECK_URL="https://antigravity.google/download"
TMP_DIR="/tmp/antigravity-update-$$"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"
}

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "==================================================" >> "$LOG_FILE"
log "Starting Nightly Antigravity IDE Check"

# ─────────────────────────────────────────────────────────────────────────────
# 1. Antigravity IDE — Direct tarball download
# ─────────────────────────────────────────────────────────────────────────────

# Get locally installed version from package.json
LOCAL_VERSION="none"
if [ -f "$INSTALL_DIR/resources/app/package.json" ]; then
  LOCAL_VERSION=$(python3 -c "import json; print(json.load(open('$INSTALL_DIR/resources/app/package.json'))['version'])" 2>/dev/null || echo "none")
fi

# Scrape the download page for the latest tarball URL
# The page contains a link like: https://edgedl.me.gvt1.com/edgedl/release2/.../Antigravity%20IDE.tar.gz
mkdir -p "$TMP_DIR"
LATEST_URL=""
REMOTE_VERSION=""

PAGE_CONTENT=$(curl -fsSL --max-time 30 "$VERSION_CHECK_URL" 2>/dev/null || echo "")
if [ -n "$PAGE_CONTENT" ]; then
  # Extract the Linux tar.gz download URL from the page
  LATEST_URL=$(echo "$PAGE_CONTENT" | grep -oP 'https://edgedl[^"]*linux-x64/Antigravity[^"]*\.tar\.gz' | head -1)
  
  if [ -z "$LATEST_URL" ]; then
    # Fallback: try a broader pattern
    LATEST_URL=$(echo "$PAGE_CONTENT" | grep -oP 'https://[^"]*linux[^"]*\.tar\.gz' | head -1)
  fi
fi

if [ -n "$LATEST_URL" ]; then
  # Download to temp to check version before installing
  TARBALL="$TMP_DIR/antigravity-latest.tar.gz"
  log "IDE: Downloading from $LATEST_URL"
  
  if curl -fSL --max-time 300 -o "$TARBALL" "$LATEST_URL" 2>>"$LOG_FILE"; then
    # Extract just the package.json to check version
    EXTRACT_DIR="$TMP_DIR/extract"
    mkdir -p "$EXTRACT_DIR"
    tar -xzf "$TARBALL" -C "$EXTRACT_DIR" 2>>"$LOG_FILE"
    
    # Handle the "Antigravity IDE" directory name from tarball
    EXTRACTED_DIR=$(find "$EXTRACT_DIR" -maxdepth 1 -type d -name "Antigravity*" | head -1)
    
    if [ -n "$EXTRACTED_DIR" ] && [ -f "$EXTRACTED_DIR/resources/app/package.json" ]; then
      REMOTE_VERSION=$(python3 -c "import json; print(json.load(open('$EXTRACTED_DIR/resources/app/package.json'))['version'])" 2>/dev/null || echo "unknown")
      
      log "IDE: Installed=$LOCAL_VERSION | Remote=$REMOTE_VERSION"
      
      if [ "$LOCAL_VERSION" != "$REMOTE_VERSION" ] && [ "$REMOTE_VERSION" != "unknown" ]; then
        log "IDE: Upgrading $LOCAL_VERSION → $REMOTE_VERSION"
        
        # Backup current installation
        mkdir -p "$BACKUP_DIR"
        if [ -d "$INSTALL_DIR" ]; then
          BACKUP_NAME="v${LOCAL_VERSION}-$(date '+%Y%m%d')"
          rm -rf "$BACKUP_DIR/$BACKUP_NAME"
          cp -a "$INSTALL_DIR" "$BACKUP_DIR/$BACKUP_NAME"
          log "IDE: Backed up to $BACKUP_DIR/$BACKUP_NAME"
        fi
        
        # Swap in the new version
        rm -rf "$INSTALL_DIR"
        mv "$EXTRACTED_DIR" "$INSTALL_DIR"
        
        # Verify the symlink still works
        if [ -x "$INSTALL_DIR/antigravity-ide" ]; then
          log "IDE: SUCCESS — upgraded to $REMOTE_VERSION"
          
          # Desktop notification
          if command -v notify-send &>/dev/null && [ -n "${DISPLAY:-}" ]; then
            notify-send -i system-software-update \
              "Antigravity IDE Updated" \
              "Upgraded to version $REMOTE_VERSION. Restart to apply." \
              2>/dev/null || true
          fi
        else
          log "IDE: WARNING — binary not found after extraction, restoring backup"
          rm -rf "$INSTALL_DIR"
          cp -a "$BACKUP_DIR/$BACKUP_NAME" "$INSTALL_DIR"
          log "IDE: Restored from backup"
        fi
        
        # Prune old backups (keep last 3)
        ls -dt "$BACKUP_DIR"/v* 2>/dev/null | tail -n +4 | xargs rm -rf 2>/dev/null || true
      else
        log "IDE: Already up to date ($LOCAL_VERSION)"
      fi
    else
      log "IDE: ERROR — could not find package.json in extracted tarball"
    fi
  else
    log "IDE: ERROR — download failed"
  fi
else
  log "IDE: WARNING — could not find download URL on page"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2. NPM CLI / Agent Engine Package (@google/gemini-cli)
# ─────────────────────────────────────────────────────────────────────────────

CURRENT_CLI=$(npm list -g @google/gemini-cli --depth=0 2>/dev/null | grep @google/gemini-cli | awk -F'@' '{print $3}')
LATEST_CLI=$(npm view @google/gemini-cli version 2>/dev/null)

log "CLI: Installed=${CURRENT_CLI:-none} | Latest NPM=${LATEST_CLI:-unknown}"

if [ -n "$LATEST_CLI" ] && [ "$CURRENT_CLI" != "$LATEST_CLI" ]; then
    log "CLI: Upgrading to $LATEST_CLI..."
    npm install -g @google/gemini-cli@latest >> "$LOG_FILE" 2>&1
    log "CLI: Upgrade complete."
else
    log "CLI: Up to date ($CURRENT_CLI)."
fi

# ─────────────────────────────────────────────────────────────────────────────
# 3. Python Antigravity SDK
# ─────────────────────────────────────────────────────────────────────────────

if command -v pip3 &>/dev/null; then
    log "SDK: Checking Python google-antigravity..."
    pip3 install --upgrade --break-system-packages google-antigravity >> "$LOG_FILE" 2>&1 || true
fi

log "Nightly Antigravity check complete."
echo "==================================================" >> "$LOG_FILE"
