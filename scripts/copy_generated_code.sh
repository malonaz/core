#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="plz-out/gen"

declare -A ACTIVE_FILES
declare -A DEST_DIRS

targets=$(plz query alltargets --hidden --include copy_generated_code*,codegen,go 2>/dev/null)

if [[ -z "$targets" ]]; then
  echo "No targets found, skipping"
  exit 0
fi

for target in $targets; do
  label=$(plz query print "$target" --label=copy_generated_code: 2>/dev/null || true)
  [[ -z "$label" ]] && continue

  IFS=':' read -r strip_prefix dest_dir <<< "$label"
  dest_dir="${dest_dir:-.}"

  DEST_DIRS["$dest_dir"]=1

  output=$(plz build "$target" 2>&1 | grep "plz-out/gen/" || true)

  for file in $output; do
    [[ "$file" != plz-out/gen/* ]] && continue

    rel_path="${file#plz-out/gen/}"

    if [[ -n "$strip_prefix" ]]; then
      rel_path="${rel_path#$strip_prefix/}"
    fi

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
  [[ ! -d "$dest_dir" ]] && continue

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
