#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/dist"
mkdir -p "$DIST"

build() {
  local goos="$1"
  local goarch="$2"
  local package="$3"
  local output="$4"
  echo "building $output"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath -ldflags="-s -w" -o "$DIST/$output" "$package"
}

cd "$ROOT"
build linux amd64 ./cmd/server-agent netprobex-server-linux-amd64
build linux arm64 ./cmd/server-agent netprobex-server-linux-arm64
build darwin arm64 ./cmd/client-probe netprobex-client-darwin-arm64
build darwin amd64 ./cmd/client-probe netprobex-client-darwin-amd64
build windows amd64 ./cmd/client-probe netprobex-client-windows-amd64.exe
build linux amd64 ./cmd/client-probe netprobex-client-linux-amd64

echo "done: $DIST"
