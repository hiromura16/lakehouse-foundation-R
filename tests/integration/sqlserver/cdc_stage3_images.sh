#!/usr/bin/env bash
# 使い捨てrunnerのみに実行イメージを組み立てて保存・照合する。
set -Eeuo pipefail
umask 077
repository="${GITHUB_REPOSITORY:-}"
if [[ "${GITHUB_ACTIONS:-}" != true || "${RUNNER_ENVIRONMENT:-}" != github-hosted ||
  "${RUNNER_OS:-}" != Linux || "${repository,,}" != hiromura16/lakehouse-foundation-r ||
  "$(uname -m)" != x86_64 ]]; then
  printf '%s\n' 'STAGE3_HOSTED_AMD64_ONLY' >&2
  exit 1
fi

out=$(mktemp -d "${RUNNER_TEMP:?}/stage3-oci.XXXXXX")
printf 'STAGE3_ARCHIVES_DIR=%s\n' "$out" >> "${GITHUB_ENV:?}"
python3 scripts/cdc_stage3_images.py registry --connect-receipt "$out/connect-source.json"
builder="foundation-r-cdc-stage3-$(openssl rand -hex 8)"
builder_image=$(python3 scripts/cdc_stage3_images.py builder-ref)
docker --context default buildx create --name "$builder" --driver docker-container \
  --driver-opt "image=$builder_image" >/dev/null
trap 'docker --context default buildx rm "$builder" >/dev/null' EXIT
for kind in kafka connect; do
  # Dockerfileに記載した公式参照でビルドし、pushやregistry loginは使わない。
  tag=$(python3 scripts/cdc_stage3_images.py local-tag "$kind" archive)
  archive="$out/$kind.tar"
  if ! docker --context default buildx build --builder "$builder" --platform linux/amd64 \
    --provenance=false --sbom=false --no-cache --pull \
    --output "type=oci,compression=gzip,name=$tag,dest=$archive" \
    --file "infra/cdc-stage3/$kind.Dockerfile" infra/cdc-stage3 > "$out/build.log" 2>&1; then
    printf 'STAGE3_BUILD_FAIL=%s\n' "$kind" >&2
    exit 1
  fi
  rm -- "$out/build.log"
  hash=$(sha256sum -- "$archive")
  verify_options=()
  if [[ "$kind" == connect ]]; then
    # タグの照合からビルドまでに別のレイヤーへ切り替わった場合は停止する。
    verify_options+=(--source-receipt "$out/connect-source.json")
  fi
  python3 scripts/cdc_stage3_images.py archive-info "$archive" --sha256 "${hash%% *}" \
    "${verify_options[@]}" \
    > "$out/$kind.json"
  if ! docker --context default image load --input "$archive" >/dev/null 2>&1; then
    printf '%s\n' 'STAGE3_OCI_LOAD_FAILED_CONTAINERD_REQUIRED' >&2
    exit 1
  fi
  config=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"])' "$out/$kind.json")
  manifest=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifest"])' "$out/$kind.json")
  if ! python3 scripts/cdc_stage3_images.py inspect-local "$tag" --manifest "$manifest"; then
    printf 'STAGE3_LOADED_MANIFEST_MISMATCH=%s\n' "$kind" >&2
    exit 1
  fi
  # 出力は上流由来バイトのハッシュだけ。tagはこのrunnerだけの参照。
  printf 'STAGE3_ARCHIVE_%s=' "$kind"
  cat -- "$out/$kind.json"
  ci_tag=$(python3 scripts/cdc_stage3_images.py local-tag "$kind" ci --config "$config")
  docker --context default image tag "$tag" "$ci_tag"
done
