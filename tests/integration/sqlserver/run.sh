#!/usr/bin/env bash
# 一時CAとループバックのみを使い、読み取りの一貫性を実SQL Serverで照合する。
set -Eeuo pipefail

dock=(docker)
if [[ "${1:-}" == --sudo ]]; then
  dock=(sudo -n docker)
  shift
fi
mode=source
if [[ "${1:-}" == --cdc-contract ]]; then
  mode=cdc-contract
  shift
fi
check_python="${1:?pymssqlを導入済みのPython実行ファイルを指定してください}"
fixture=tests/integration/sqlserver
image=mcr.microsoft.com/mssql/server@sha256:05644dad1266ca3d445d1e8f6d2ebb9a6a03df9a39376e0f7b48917356dca9d9
if [[ "$mode" == cdc-contract ]]; then
  # 独立CDCの対象版は SQL Server 2022 (16.0.4325.4) のみ。
  # MCR 2022-latest の検証済み linux/amd64 manifest。既存source試験は変更しない。
  image=mcr.microsoft.com/mssql/server@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f
fi
umask 077
stage=$(mktemp -d)
stage=$(cd -- "$stage" && pwd -P)
site="foundation-r-sqlserver-$(openssl rand -hex 8)"
certs="$site-tls"

cleanup() {
  local status=$?
  trap - EXIT
  if "${dock[@]}" inspect --type container "$site" >/dev/null 2>&1; then
    if ((status)); then
      # コンテナ環境変数に入れた合成パスワードをログに展開しない。
      "${dock[@]}" inspect --format '{{.State.Status}} {{.State.ExitCode}}' "$site" >&2 || true
    fi
    "${dock[@]}" rm -f -v "$site" >/dev/null || status=1
  fi
  if "${dock[@]}" volume inspect "$certs" >/dev/null 2>&1; then
    "${dock[@]}" volume rm "$certs" >/dev/null || status=1
  fi
  rm -rf -- "$stage" || status=1
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for ca in ca wrong-ca; do
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -subj "/CN=Foundation R SQL fixture $ca" \
    -addext basicConstraints=critical,CA:TRUE \
    -addext keyUsage=critical,keyCertSign,cRLSign \
    -keyout "$stage/$ca-key.pem" -out "$stage/$ca.pem" 2>/dev/null
done
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
  -keyout "$stage/server-key.pem" -out "$stage/server.csr" 2>/dev/null
printf '%s\n' 'subjectAltName=DNS:localhost' 'extendedKeyUsage=serverAuth' \
  > "$stage/extensions"
openssl x509 -req -in "$stage/server.csr" -CA "$stage/ca.pem" \
  -CAkey "$stage/ca-key.pem" -CAcreateserial -days 1 \
  -extfile "$stage/extensions" -out "$stage/server.pem" 2>/dev/null

{
  printf 'MSSQL_SA_PASSWORD=Aa9!%s\n' "$(openssl rand -hex 24)"
  printf 'MSSQL_PID=Developer\nMSSQL_MEMORY_LIMIT_MB=2048\n'
  if [[ "$mode" == cdc-contract ]]; then
    printf 'MSSQL_AGENT_ENABLED=true\n'
  fi
} > "$stage/fixture.env"
cat > "$stage/mssql.conf" <<'CONFIG'
[network]
forceencryption = 1
tlscert = /fixture-tls/server.pem
tlskey = /fixture-tls/server-key.pem
CONFIG
if [[ "$mode" == cdc-contract ]]; then
  printf '\n[sqlagent]\nenabled = true\n' >> "$stage/mssql.conf"
fi
chmod 0444 "$stage/mssql.conf"
"${dock[@]}" pull "$image" >/dev/null
"${dock[@]}" volume create "$certs" >/dev/null
"${dock[@]}" run --rm --pull never --network none --user 0 --entrypoint /bin/sh \
  --mount "type=bind,src=$stage/server-key.pem,dst=/incoming.key,readonly" \
  --mount "type=bind,src=$stage/server.pem,dst=/incoming.pem,readonly" \
  --mount "type=volume,src=$certs,dst=/fixture-tls" "$image" -ec \
  'install -m 0400 -o mssql /incoming.key /fixture-tls/server-key.pem
   install -m 0400 -o mssql /incoming.pem /fixture-tls/server.pem'

# MicrosoftのDeveloper版を閉じた合成試験に使う。通常の配備には含めない。
"${dock[@]}" run -d --pull never --init --name "$site" \
  --publish 127.0.0.1::1433 --memory 3g --cpus 2 \
  --env-file "$stage/fixture.env" --env ACCEPT_EULA=Y \
  --mount "type=bind,src=$stage/mssql.conf,dst=/var/opt/mssql/mssql.conf,readonly" \
  --mount "type=volume,src=$certs,dst=/fixture-tls,readonly" "$image" >/dev/null

binding=$("${dock[@]}" port "$site" 1433)
if [[ ! "$binding" =~ ^127\.0\.0\.1:([1-9][0-9]*)$ ]]; then
  printf '%s\n' '合成SQL Serverのループバックポートを確認できません。' >&2
  exit 1
fi
entry=acceptance.py
if [[ "$mode" == cdc-contract ]]; then
  entry=cdc_contract_acceptance.py
fi
env -i PATH="$PATH" HOME="$stage" \
  "$check_python" "$fixture/$entry" "$stage" "${BASH_REMATCH[1]}"
