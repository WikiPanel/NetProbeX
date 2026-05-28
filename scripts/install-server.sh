#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Run as root: sudo scripts/install-server.sh"
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install -d /opt/netprobex /var/log/netprobex

if [[ -f "$ROOT/dist/netprobex-server-linux-amd64" ]]; then
  install -m 0755 "$ROOT/dist/netprobex-server-linux-amd64" /opt/netprobex/server-agent
elif [[ -f "$ROOT/server-agent" ]]; then
  install -m 0755 "$ROOT/server-agent" /opt/netprobex/server-agent
else
  echo "Build the server first with scripts/build-all.sh"
  exit 1
fi

install -m 0644 "$ROOT/configs/server.example.json" /opt/netprobex/server.json
install -m 0644 "$ROOT/systemd/netprobex-server.service" /etc/systemd/system/netprobex-server.service
systemctl daemon-reload
systemctl enable netprobex-server.service
echo "Installed. Edit /opt/netprobex/server.json, then run: systemctl start netprobex-server"
