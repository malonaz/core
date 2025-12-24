#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="plz-out/gen"
DEST_DIR="genproto"

declare -A ACTIVE_DIRS

targets=$(plz query alltargets --hidden --include codegen,proto,go 2>/dev/null)

for target in $targets; do
  output=$(plz build "$target" 2>&1 | grep "plz-out/gen/" || true)

  for file in $output; do
    [[ "$file" != plz-out/gen/* ]] && continue

    rel_path="${file#plz-out/gen/malonaz/}"
    dest_dir=$(dirname "$rel_path")
    filename=$(basename "$file")

    mkdir -p "$DEST_DIR/$dest_dir"
    cp -f "$file" "$DEST_DIR/$dest_dir/$filename"
    echo "✓ Copied $rel_path"

    ACTIVE_DIRS["$DEST_DIR/$dest_dir"]=1
  done
done

if [[ -d "$DEST_DIR" ]]; then
  find "$DEST_DIR" -type f ! -name "BUILD.plz" | while read -r f; do
    dir=$(dirname "$f")
    [[ ! -v ACTIVE_DIRS["$dir"] ]] && rm -f "$f" && echo "🗑 Removed stale $f"
  done

  while IFS= read -r build_file; do
    dir=$(dirname "$build_file")
    [[ ! -v ACTIVE_DIRS["$dir"] ]] && rm -f "$build_file" && echo "🗑 Removed unused $build_file"
  done < <(find "$DEST_DIR" -type f -name "BUILD.plz")

  find "$DEST_DIR" -type d -empty -delete 2>/dev/null || true
fi

echo "Linting files..."
plz lint > /dev/null 2>&1 || true

echo "✅ Regenerated all files!"
