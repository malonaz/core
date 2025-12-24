#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="plz-out/gen"
DEST_DIR="genproto"

declare -A ACTIVE_FILES

targets=$(plz query alltargets --hidden --include codegen,proto,go 2>/dev/null)

for target in $targets; do
  output=$(plz build "$target" 2>&1 | grep "plz-out/gen/" || true)

  for file in $output; do
    [[ "$file" != plz-out/gen/* ]] && continue

    rel_path="${file#plz-out/gen/malonaz/}"
    dest_dir=$(dirname "$rel_path")
    filename=$(basename "$file")
    dest_file="$DEST_DIR/$dest_dir/$filename"

    mkdir -p "$DEST_DIR/$dest_dir"
    cp -f "$file" "$dest_file"
    echo "✓ Copied $rel_path"

    ACTIVE_FILES["$dest_file"]=1
  done
done

if [[ -d "$DEST_DIR" ]]; then
  while read -r f; do
    [[ ! -v ACTIVE_FILES["$f"] ]] && rm -f "$f" && echo "🗑 Removed stale $f"
  done < <(find "$DEST_DIR" -type f ! -name "BUILD.plz")

  # Remove BUILD.plz files in directories with no active files
  while read -r build_file; do
    dir=$(dirname "$build_file")
    has_active=false
    for f in "${!ACTIVE_FILES[@]}"; do
      [[ $(dirname "$f") == "$dir" ]] && has_active=true && break
    done
    [[ "$has_active" == false ]] && rm -f "$build_file" && echo "🗑 Removed unused $build_file"
  done < <(find "$DEST_DIR" -type f -name "BUILD.plz")

  find "$DEST_DIR" -type d -empty -delete 2>/dev/null || true
fi

echo "Linting files..."
plz lint > /dev/null 2>&1 || true

echo "✅ Regenerated all files!"
