#!/bin/sh
# HTTPで応答することを確認する。取込と保持の確認は受入試験で行う。
set -eu
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
exec /bin/busybox wget -q -T 4 -O /dev/null http://127.0.0.1:5080/healthz
