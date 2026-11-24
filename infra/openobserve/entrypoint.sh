#!/bin/sh
# データvolumeの所有者や権限が異なるときに、データへ変更を加えず停止する。
set -eu
expected="$(/bin/busybox id -u):$(/bin/busybox id -g):700"
actual="$(/bin/busybox stat -c '%u:%g:%a' /data 2>/dev/null || true)"
if [ "$(/bin/busybox id -u)" = 0 ] || [ -L /data ] || \
   [ ! -d /data ] || [ ! -w /data ] || [ "$actual" != "$expected" ]; then
  printf '%s\n' 'OpenObserve のデータvolumeの所有者と権限を確認してください。' >&2
  exit 78
fi
if [ -z "${ZO_ROOT_USER_EMAIL:-}" ] || [ -z "${ZO_ROOT_USER_PASSWORD:-}" ]; then
  printf '%s\n' 'OpenObserve の初期管理者を設定してください。' >&2
  exit 78
fi
# 取込用の認証情報は転送サービス側だけで使用する。
exec /openobserve
