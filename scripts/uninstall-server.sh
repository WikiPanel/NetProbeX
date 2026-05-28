#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Run as root: sudo scripts/uninstall-server.sh"
  exit 1
fi

systemctl stop netprobex-server.service 2>/dev/null || true
systemctl disable netprobex-server.service 2>/dev/null || true
rm -f /etc/systemd/system/netprobex-server.service
systemctl daemon-reload
rm -rf /opt/netprobex
echo "Uninstalled. Logs remain in /var/log/netprobex unless removed manually."
