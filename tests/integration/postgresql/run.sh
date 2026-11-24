#!/usr/bin/env bash
# 一時証明書・秘密・名前付きComposeだけを使って、実SQL Goldまで確認する。
set -Eeuo pipefail

use_sudo=false
if [[ "${1:-}" == --sudo ]]; then
  use_sudo=true
  shift
fi
check_python="${1:?Psycopgを導入済みのPython実行ファイルを指定してください}"
fixture=tests/integration/postgresql
umask 077
stage=$(mktemp -d)
stage=$(cd -- "$stage" && pwd -P)
project="foundation-r-postgresql-$(openssl rand -hex 8)"
site="$stage/site.env"
input="$stage/source-input"
docker_cmd=(docker)
if "$use_sudo"; then
  mkdir -m 700 "$stage/docker-config"
  docker_cmd=(sudo -n env -i PATH=/usr/local/bin:/usr/bin:/bin
    "DOCKER_CONFIG=$stage/docker-config" docker)
fi
suite=("${docker_cmd[@]}" compose -p "$project" --env-file "$site"
  -f compose.yaml -f "$fixture/compose.fixture.yaml")

cleanup() {
  local status=$?
  trap - EXIT
  if [[ -f "$site" ]]; then
    if ((status)); then
      "${suite[@]}" ps --all >&2 || true
    fi
    "${suite[@]}" down --volumes --remove-orphans || status=1
    "${docker_cmd[@]}" image rm "$project-lakehouse" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$stage" || status=1
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -m 700 "$input"
openssl rand -hex 24 > "$input/admin-password"
for ca in ca wrong-ca; do
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -subj "/CN=Foundation R ephemeral $ca" \
    -addext basicConstraints=critical,CA:TRUE \
    -addext keyUsage=critical,keyCertSign,cRLSign \
    -keyout "$stage/$ca-key.pem" -out "$stage/$ca.pem" 2>/dev/null
done
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
  -keyout "$input/server-key.pem" -out "$stage/server.csr" 2>/dev/null
printf '%s\n' 'subjectAltName=DNS:localhost' 'extendedKeyUsage=serverAuth' \
  > "$stage/extensions"
openssl x509 -req -in "$stage/server.csr" -CA "$stage/ca.pem" \
  -CAkey "$stage/ca-key.pem" -CAcreateserial -days 1 \
  -extfile "$stage/extensions" -out "$input/server.pem" 2>/dev/null

python3 native/setup.py --output "$site" --landing "$stage/landing"
printf 'POSTGRES_FIXTURE_INPUT=%s\n' "$input" >> "$site"
"${suite[@]}" config --quiet
"${suite[@]}" up -d --build --wait --wait-timeout 180 postgres-source lakehouse

binding=$("${suite[@]}" port postgres-source 5432)
if [[ ! "$binding" =~ ^127\.0\.0\.1:([1-9][0-9]*)$ ]]; then
  printf '%s\n' '合成PostgreSQLのループバックポートを確認できません。' >&2
  exit 1
fi
# Lakehouseの書込APIはホストに公開せず、既存コンテナ内から操作する。
env -i PATH="$PATH" HOME="$stage" \
  "$check_python" "$fixture/check_source.py" "$stage" "${BASH_REMATCH[1]}"
"${suite[@]}" exec -T lakehouse python - < "$fixture/check_apply.py"
