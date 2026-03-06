#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="plz-out/gen"

declare -A ACTIVE_FILES
declare -A DEST_DIRS

targets=$(plz query alltargets --hidden --include copy_generated_code*,codegen 2>/dev/null)

if [[ -z "$targets" ]]; then
  echo "No targets found, skipping"
  exit 0
fi

declare -A TARGET_LABELS
while IFS=$'\t' read -r target label; do
  [[ -n "$label" ]] && TARGET_LABELS["$target"]="$label"
done < <(
  for target in $targets; do
    echo "$target"
  done | xargs -P8 -I{} bash -c 'label=$(plz query print "{}" --label=copy_generated_code: 2>/dev/null); [[ -n "$label" ]] && printf "%s\t%s\n" "{}" "$label"'
)

labeled_targets=("${!TARGET_LABELS[@]}")
if [[ ${#labeled_targets[@]} -eq 0 ]]; then
  echo "No labeled targets, skipping"
  exit 0
fi

build_output=$(plz build "${labeled_targets[@]}" 2>&1 | grep "plz-out/gen/" || true)

IFS=$'\n' sorted_targets=($(for t in "${labeled_targets[@]}"; do pkg="${t%%:*}"; pkg="${pkg#//}"; printf '%d\t%s\n' "${#pkg}" "$t"; done | sort -rn | cut -f2))
unset IFS
declare -A CLAIMED_FILES

for target in "${sorted_targets[@]}"; do
  label="${TARGET_LABELS[$target]}"
  IFS=':' read -r strip_prefix dest_dir <<< "$label"
  dest_dir="${dest_dir:-.}"
  DEST_DIRS["$dest_dir"]=1

  pkg="${target%%:*}"
  pkg="${pkg#//}"

  for file in $build_output; do
    [[ "$file" != plz-out/gen/${pkg}/* ]] && continue
    [[ -v CLAIMED_FILES["$file"] ]] && continue
    CLAIMED_FILES["$file"]=1

    rel_path="${file#plz-out/gen/}"
    [[ -n "$strip_prefix" ]] && rel_path="${rel_path#$strip_prefix/}"

    dir=$(dirname "$rel_path")
    filename=$(basename "$file")
    dest_file="$dest_dir/$dir/$filename"

    mkdir -p "$dest_dir/$dir"
    cp -f "$file" "$dest_file"
    echo "✓ Copied $rel_path -> $dest_file"
    ACTIVE_FILES["$dest_file"]=1
  done
done

for dest_dir in "${!DEST_DIRS[@]}"; do
  [[ ! -d "$dest_dir" || "$dest_dir" == "." ]] && continue

  while read -r f; do
    [[ ! -v ACTIVE_FILES["$f"] ]] && rm -f "$f" && echo "🗑 Removed stale $f"
  done < <(find "$dest_dir" -type f ! -name "BUILD.plz")

  while read -r build_file; do
    dir=$(dirname "$build_file")
    has_active=false
    for f in "${!ACTIVE_FILES[@]}"; do
      [[ $(dirname "$f") == "$dir" ]] && has_active=true && break
    done
    [[ "$has_active" == false ]] && rm -f "$build_file" && echo "🗑 Removed unused $build_file"
  done < <(find "$dest_dir" -type f -name "BUILD.plz")

  find "$dest_dir" -type d -empty -delete 2>/dev/null || true
done

echo "✅ Regenerated all files!"
