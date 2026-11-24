#!/usr/bin/env bash
# GitHubホストランナー用。生成した合成データと専用の一時資源だけを使う。
set -Eeuo pipefail
umask 077

stage=$(mktemp -d "${RUNNER_TEMP:?GitHub Actionsの一時ディレクトリが必要です}/native-recovery.XXXXXXXX")
stamp=${stage##*.}
project="native-recovery-${stamp,,}"
target="$project-restored"
export RECOVERY_APP_IMAGE="$project-lakehouse"
archive="$stage/archive"
output="$stage/restore"
fixture=tests/integration/recovery/compose.fixture.yaml
site="$stage/source.env"
target_site="$stage/target.env"
source_dc=(docker compose -p "$project" --env-file "$site" -f compose.yaml -f "$fixture")
target_dc=(docker compose -p "$target" --env-file "$target_site"
  -f compose.yaml -f "$fixture" -f "$output/compose.restore.yaml")
helper=$(PYTHONPATH=scripts python3 -c \
  'import runpy; print(runpy.run_path("scripts/backup-native.py", run_name="constants")["PYTHON"])')

cleanup() {
  # 試験専用ラベルの資源だけを削除し、原本や資格情報を成果物へ送らない。
  if [[ -f "$output/compose.restore.yaml" ]]; then
    "${target_dc[@]}" down --volumes --remove-orphans >/dev/null || true
  fi
  if [[ -f "$site" ]]; then
    "${source_dc[@]}" down --volumes --remove-orphans >/dev/null || true
  fi
  docker volume ls --quiet --filter "label=foundation-r.restore-project=$target" |
    while IFS= read -r volume; do
      docker volume rm "$volume" >/dev/null || true
    done
  rm -rf -- "$stage"
}
trap cleanup EXIT

postgres_ref() {
  # 検証する版の公開Digestはバックアップ実装と同じ固定値から取る。
  PYTHONPATH=scripts python3 -c \
    'import runpy,sys; print(runpy.run_path("scripts/backup-native.py", run_name="constants")[sys.argv[1]])' "$1"
}

variant="${RECOVERY_POSTGRES_VARIANT:-current}"
current_pg=$(postgres_ref POSTGRES)
case "$variant" in
  current) source_pg="$current_pg" ;;
  previous)
    source_pg=$(postgres_ref LEGACY_POSTGRES)
    # 更新前に取得した台帳に相当する旧カタログ・旧S3も使う。
    PYTHONPATH=scripts python3 - "$source_pg" > "$stage/source-previous.yaml" <<'PYCODE'
import json
import runpy
import sys

constants = runpy.run_path("scripts/backup-native.py", run_name="constants")
images = {**constants["LEGACY_CORE_IMAGES"], "catalog-db": sys.argv[1]}
images["catalog-migrate"] = images["catalog"]
json.dump({"services": {role: {"image": ref} for role, ref in images.items()}}, sys.stdout)
PYCODE
    source_dc+=(-f "$stage/source-previous.yaml")
    ;;
  *) echo "試験用PostgreSQLの指定が不正です。" >&2; exit 1 ;;
esac


python3 native/setup.py --output "$site" --landing "$stage/source-landing"
mkdir -m 0750 "$stage/target-landing"
sed "s|^LANDING_DIR=.*$|LANDING_DIR='$stage/target-landing'|" "$site" > "$target_site"
"${source_dc[@]}" config --quiet
"${source_dc[@]}" pull --policy missing storage catalog-db catalog catalog-migrate
docker pull "$helper"
"${source_dc[@]}" build lakehouse
"${source_dc[@]}" up -d --no-build --pull never --wait --wait-timeout 160 lakehouse
python3 scripts/generate-demo.py --output "$stage/source-landing" --seed 42 --orders 20 --days 7 --scenario edge
"${source_dc[@]}" exec -T lakehouse python -m native.api medallion
"${source_dc[@]}" exec -T lakehouse python - --physical \
  < tests/integration/native/snapshot_evidence.py > "$stage/before.json"

# データを書き込むアプリを停止してから、カタログと保存先を同じ停止点にする。
"${source_dc[@]}" stop --timeout 30 lakehouse catalog
"${source_dc[@]}" stop --timeout 30 storage catalog-db
printf '%s\n' '採取先を読み取り、同じ停止点の2領域を記録します。'
python3 scripts/backup-native.py capture --project "$project" "$archive" > "$stage/capture.json"
manifest_hash=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["manifest_sha256"])' < "$stage/capture.json")
if python3 scripts/backup-native.py restore --project "$project" \
  --output "$stage/rejected" --expected-manifest-sha256 "$manifest_hash" "$archive"; then
  echo "採取側と同じApp Nameを拒否できませんでした。" >&2
  exit 1
fi
test ! -e "$stage/rejected"
printf '%s\n' '別のApp Nameと新規ボリュームに復元します。'
restore=(python3 scripts/backup-native.py restore --project "$target"
  --output "$output" --expected-manifest-sha256 "$manifest_hash" "$archive")
"${restore[@]}"
python3 - "$source_pg" "$archive/manifest.json" "$output/compose.restore.yaml" <<'PYCODE'
import json
import sys
from pathlib import Path

image, manifest, overlay = sys.argv[1:]
recorded = json.loads(Path(manifest).read_text(encoding="utf-8"))
effective = json.loads(Path(overlay).read_text(encoding="utf-8"))
assert recorded["native_core"]["images"]["catalog-db"] == image
for role, ref in recorded["native_core"]["images"].items():
    assert effective["services"][role]["image"] == ref
assert effective["services"]["catalog-migrate"]["image"] == recorded["native_core"]["images"]["catalog"]
PYCODE
if "${restore[@]}"; then
  echo "同じ出力先の再利用を検出できませんでした。" >&2
  exit 1
fi

# カタログ移行、Bootstrap、原本投入をせず、復元済みの記録だけを読み取る。
"${target_dc[@]}" config --quiet
"${target_dc[@]}" up -d --no-build --pull never --no-deps --wait --wait-timeout 160 storage catalog-db
"${target_dc[@]}" up -d --no-build --pull never --no-deps --wait --wait-timeout 160 catalog
"${target_dc[@]}" run -T --rm --no-deps --pull never --entrypoint python lakehouse - --physical \
  < tests/integration/native/snapshot_evidence.py > "$stage/after.json"
cmp -s "$stage/before.json" "$stage/after.json"
python3 scripts/backup-native.py verify "$archive" --expected-manifest-sha256 "$manifest_hash"
cat "$stage/after.json"

if [[ "$variant" == previous ]]; then
  # 18.6で読めることを確認後、復元先だけを18.7へ更新する。
  docker pull "$current_pg"
  printf '{"services":{"catalog-db":{"image":"%s"}}}\n' "$current_pg" > "$stage/upgrade.yaml"
  upgraded_dc=("${target_dc[@]}" -f "$stage/upgrade.yaml")
  collation_sql='SELECT datname, datlocprovider, datcollate, datctype,
                        datcollversion, pg_database_collation_actual_version(oid)
                 FROM pg_database WHERE datallowconn AND NOT datistemplate
                 ORDER BY datname'
  test "$("${target_dc[@]}" exec -T catalog-db psql -XAt -v ON_ERROR_STOP=1 \
    -U lakekeeper -d lakekeeper -c 'SHOW server_version_num')" = 180006
  "${target_dc[@]}" exec -T catalog-db psql -XAt -v ON_ERROR_STOP=1 \
    -U lakekeeper -d lakekeeper -c "$collation_sql" > "$stage/collation-before"
  test -s "$stage/collation-before"
  "${target_dc[@]}" stop --timeout 30 catalog catalog-db
  "${upgraded_dc[@]}" up -d --no-build --pull never --no-deps --wait --wait-timeout 160 catalog-db
  "${upgraded_dc[@]}" up -d --no-build --pull never --no-deps --wait --wait-timeout 160 catalog
  test "$("${upgraded_dc[@]}" exec -T catalog-db psql -XAt -v ON_ERROR_STOP=1 \
    -U lakekeeper -d lakekeeper -c 'SHOW server_version_num')" = 180007
  "${upgraded_dc[@]}" exec -T catalog-db psql -XAt -v ON_ERROR_STOP=1 \
    -U lakekeeper -d lakekeeper -c "$collation_sql" > "$stage/collation-after"
  cmp -s "$stage/collation-before" "$stage/collation-after"
  "${upgraded_dc[@]}" run -T --rm --no-deps --pull never --entrypoint python lakehouse - --physical \
    < tests/integration/native/snapshot_evidence.py > "$stage/upgraded.json"
  cmp -s "$stage/before.json" "$stage/upgraded.json"
  python3 scripts/backup-native.py verify "$archive" --expected-manifest-sha256 "$manifest_hash"
  printf '%s\n' '18.6から18.7へ更新した隔離復元先で、照合順序・Raw・Goldが一致しました。'
fi
