#!/usr/bin/env bash
# 合成したTLSの発行者と、実際に生成したIcebergのGoldを照合する。
set -Eeuo pipefail

docker_cmd=(docker)
if [[ "${1:-}" == --sudo ]]; then
  docker_cmd=(sudo -n docker)
  shift
fi
check_python="${1:?署名検査用のPython実行ファイルを指定してください}"
umask 077
stage=$(mktemp -d)
project="foundation-r-edge-$(openssl rand -hex 4)"
site="$stage/site.env"
landing="$stage/landing"
fixture="$stage/keys"
suite=("${docker_cmd[@]}" compose -p "$project" --env-file "$site"
  -f compose.yaml -f compose.cloudflare.yaml
  -f tests/integration/edge/compose.fixture.yaml)

cleanup() {
  if [[ -f "$site" ]]; then
    "${suite[@]}" down --volumes --remove-orphans --rmi local || return
  fi
  rm -rf -- "$stage"
}
trap cleanup EXIT

wait_for_edge() {
  local container health attempt
  container=$("${suite[@]}" ps -q edge-gold)
  # restartはヘルスチェック完了を待たず、公開ポートも再割当てされうる。
  for ((attempt = 0; attempt < 25; attempt++)); do
    health=$("${docker_cmd[@]}" inspect --format '{{.State.Health.Status}}' "$container")
    if [[ "$health" == healthy ]]; then
      "${suite[@]}" port edge-gold 8080
      return
    fi
    sleep 2
  done
  "${suite[@]}" logs --tail 40 edge-gold >&2
  return 1
}

mkdir -m 700 "$fixture"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj '/CN=Foundation R test CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -keyout "$stage/ca-key.pem" -out "$stage/ca.pem" 2>"$stage/ca.log"
openssl req -newkey rsa:2048 -nodes \
  -subj '/CN=fixture-team.cloudflareaccess.com' \
  -keyout "$fixture/server-key.pem" -out "$stage/server.csr" \
  2>"$stage/leaf.log"
printf '%s\n' 'subjectAltName=DNS:fixture-team.cloudflareaccess.com' \
  'extendedKeyUsage=serverAuth' > "$stage/extensions"
openssl x509 -req -in "$stage/server.csr" -CA "$stage/ca.pem" \
  -CAkey "$stage/ca-key.pem" -CAcreateserial -days 1 \
  -extfile "$stage/extensions" -out "$fixture/server.pem" \
  2>"$stage/sign.log"
cp tests/integration/edge/jwks_fixture.py "$fixture/jwks_fixture.py"

python3 native/setup.py --output "$site" --landing "$landing"
sed -n 's/^READER_TOKEN=//p' "$site" > "$stage/edge-reader"
# トンネルの設定展開だけに使う空ファイル。cloudflaredは起動対象外。
: > "$stage/synthetic-tunnel"
printf '%s\n' LAKEHOUSE_API_PORT=0 CF_EDGE_HTTPS_PROXY= \
  "CF_JWKS_FIXTURE_DIR=$fixture" "CORPORATE_CA_FILE=$stage/ca.pem" \
  "CORPORATE_CA_SHA256=$(sha256sum "$stage/ca.pem" | cut -d " " -f 1)" \
  "CF_EDGE_READER_FILE=$stage/edge-reader" "CF_EDGE_POLICY_FILE=$fixture/policy.json" \
  "CF_TUNNEL_TOKEN_FILE=$stage/synthetic-tunnel" >> "$site"

build_images() {
  # v5ではBakeがファイルごとの許可を求める。公開CAだけを明示的に渡す。
  "${suite[@]}" build --print "$@" |
    "${docker_cmd[@]}" buildx bake --file - --load --allow "fs.read=$stage/ca.pem"
}

"${suite[@]}" config --quiet
build_images lakehouse
"${suite[@]}" up -d --no-build --pull missing --wait --wait-timeout 180 lakehouse
python3 scripts/generate-demo.py --output "$landing" --seed 42 --orders 20 --days 7 --scenario edge
"${suite[@]}" exec -T lakehouse python -m native.api medallion
"${suite[@]}" exec -T lakehouse python -m native.api gold factory_daily_production --limit 20 > "$fixture/gold.json"
"$check_python" tests/integration/edge/live_acceptance.py "$fixture" --prepare
build_images edge-gold read-api
"${suite[@]}" up -d --no-build --pull missing --wait --wait-timeout 90 edge-gold
binding=$(wait_for_edge)
"$check_python" tests/integration/edge/live_acceptance.py "$fixture" --port "${binding##*:}"

# ゲートを再起動して鍵キャッシュを空にし、署名の検証を省略しないことを確認する。
"${suite[@]}" stop fixture-jwks
"${suite[@]}" restart edge-gold
binding=$(wait_for_edge)
"${suite[@]}" exec -T lakehouse python -m native.api medallion > "$stage/outage.json"
python3 - "$stage/outage.json" <<'PY'
import json
import sys
from pathlib import Path

status = json.loads(Path(sys.argv[1]).read_text())
assert status["unchanged"] and status["imported"] == 0
PY
"${suite[@]}" exec -T lakehouse python -m native.api gold factory_daily_production --limit 20 > "$stage/outage-gold.json"
cmp "$fixture/gold.json" "$stage/outage-gold.json"
"$check_python" tests/integration/edge/live_acceptance.py "$fixture" --port "${binding##*:}" --outage
"${suite[@]}" up -d --wait --wait-timeout 60 fixture-jwks
"$check_python" tests/integration/edge/live_acceptance.py "$fixture" --port "${binding##*:}"
