#!/usr/bin/env bash
set -euo pipefail

# One-time host provisioning for the self-hosted Docker stack.
# Run on a fresh Ubuntu/Debian host from a checkout of this repository:
#
#   sudo ./deploy/scripts/bootstrap-server.sh
#
# Environment overrides:
#   TARGET=/opt/stacks/strava
#   DEPLOY_USER=<user that will run GitHub deploys; defaults to SUDO_USER>
#   SSH_PORT=22
#   START_STACK=1

if [[ "${EUID}" -ne 0 ]]; then
    echo "Run this script as root (sudo)." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET="${TARGET:-/opt/stacks/strava}"
SSH_PORT="${SSH_PORT:-22}"
DEPLOY_USER="${DEPLOY_USER:-${SUDO_USER:-root}}"

if [[ ! -r /etc/os-release ]]; then
    echo "Unsupported host: /etc/os-release is missing." >&2
    exit 1
fi

# shellcheck disable=SC1091
source /etc/os-release
case "${ID}" in
    ubuntu|debian) ;;
    *)
        echo "This script supports Ubuntu and Debian. Detected: ${ID}" >&2
        exit 1
        ;;
esac

echo "==> Installing base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    fail2ban \
    gnupg \
    jq \
    make \
    openssl \
    rsync \
    ufw \
    unattended-upgrades

echo "==> Installing Docker Engine and Compose plugin"
install -m 0755 -d /etc/apt/keyrings
curl -fsSL "https://download.docker.com/linux/${ID}/gpg" -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

ARCH="$(dpkg --print-architecture)"
cat >/etc/apt/sources.list.d/docker.list <<EOF
deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable
EOF

apt-get update
apt-get install -y --no-install-recommends \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin

systemctl enable --now docker

if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    usermod -aG docker "${SUDO_USER}" || true
    echo "Added ${SUDO_USER} to the docker group (log out and back in to use it)."
fi

echo "==> Applying host tuning"
cat >/etc/sysctl.d/99-peakshunters.conf <<'EOF'
vm.swappiness=10
fs.inotify.max_user_watches=524288
EOF
sysctl --system >/dev/null

if [[ ! -f /etc/docker/daemon.json ]]; then
    cat >/etc/docker/daemon.json <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "20m",
    "max-file": "5"
  }
}
EOF
    systemctl restart docker
else
    echo "Keeping existing /etc/docker/daemon.json; verify it has log rotation configured."
fi

echo "==> Configuring firewall"
ufw allow "${SSH_PORT}"/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 443/udp
ufw --force enable

echo "==> Enabling unattended security updates"
dpkg-reconfigure -f noninteractive unattended-upgrades || true
systemctl enable --now unattended-upgrades || true
systemctl enable --now fail2ban || true

echo "==> Installing deployment files into ${TARGET}"
mkdir -p "${TARGET}" "${TARGET}/backups"
if [[ "${SOURCE_DIR}" != "${TARGET}" ]]; then
    rsync -a --delete \
        --exclude '.env' \
        --exclude 'backups' \
        "${SOURCE_DIR}/" "${TARGET}/"
fi

if [[ ! -f "${TARGET}/.env" ]]; then
    echo "==> Creating ${TARGET}/.env from .env.example"
    "${TARGET}/scripts/init-env.sh"
else
    chmod 600 "${TARGET}/.env"
fi

echo "==> Setting deployment directory ownership to ${DEPLOY_USER}"
chown -R "${DEPLOY_USER}" "${TARGET}"
chmod 600 "${TARGET}/.env"

echo "==> Installing backup timer"
install -m 0644 "${TARGET}/systemd/peakshunters-backup.service" /etc/systemd/system/
install -m 0644 "${TARGET}/systemd/peakshunters-backup.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now peakshunters-backup.timer

echo
echo "Bootstrap complete."
echo "  1. Edit ${TARGET}/.env"
echo "  2. Log in to GHCR once if the images are private:"
echo "       docker login ghcr.io"
echo "  3. Start the stack:"
echo "       cd ${TARGET} && docker compose up -d --wait"
echo
echo "Workers are disabled by default. See deploy/README.md for the rollout order."

if [[ "${START_STACK:-0}" == "1" ]]; then
    cd "${TARGET}"
    docker compose up -d --remove-orphans --wait
fi
