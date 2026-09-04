#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="plz-out/gen"

declare -A ACTIVE_FILES
declare -A DEST_DIRS
declare -A TOTAL_COUNT
declare -A UPDATED_COUNT
declare -A REMOVED_COUNT

# Copy src -> dest, skipping identical files so only real changes are reported.
copy_file() {
  local src="$1" dest="$2" dest_dir="$3"
  ACTIVE_FILES["$dest"]=1
  TOTAL_COUNT["$dest_dir"]=$((${TOTAL_COUNT["$dest_dir"]:-0} + 1))
  if cmp -s "$src" "$dest"; then
    return
  fi
  mkdir -p "$(dirname "$dest")"
  cp -f "$src" "$dest"
  UPDATED_COUNT["$dest_dir"]=$((${UPDATED_COUNT["$dest_dir"]:-0} + 1))
}

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
    if [[ -n "$strip_prefix" ]]; then
      rel_path="${rel_path#$strip_prefix/}"
      # output path may equal the prefix exactly (directory outputs)
      [[ "$rel_path" == "$strip_prefix" ]] && rel_path=""
    fi

    # build outputs can be directories; copy them recursively
    if [[ -d "$file" ]]; then
      target_dir="$dest_dir${rel_path:+/$rel_path}"
      mkdir -p "$target_dir"
      while read -r copied; do
        copy_file "$copied" "$target_dir/${copied#$file/}" "$dest_dir"
      done < <(find "$file" -type f)
      continue
    fi

    copy_file "$file" "$dest_dir/$(dirname "$rel_path")/$(basename "$file")" "$dest_dir"
  done
done

# Directories that received a file this run; a BUILD.plz anywhere else is stale.
declare -A ACTIVE_DIRS
for f in "${!ACTIVE_FILES[@]}"; do ACTIVE_DIRS["${f%/*}"]=1; done

for dest_dir in "${!DEST_DIRS[@]}"; do
  [[ ! -d "$dest_dir" || "$dest_dir" == "." ]] && continue

  while read -r f; do
    [[ ! -v ACTIVE_FILES["$f"] ]] && rm -f "$f" && REMOVED_COUNT["$dest_dir"]=$((${REMOVED_COUNT["$dest_dir"]:-0} + 1))
  done < <(find "$dest_dir" -type f ! -name "BUILD.plz")

  while read -r build_file; do
    [[ ! -v ACTIVE_DIRS["${build_file%/*}"] ]] && rm -f "$build_file" && REMOVED_COUNT["$dest_dir"]=$((${REMOVED_COUNT["$dest_dir"]:-0} + 1))
  done < <(find "$dest_dir" -type f -name "BUILD.plz")

  find "$dest_dir" -type d -empty -delete 2>/dev/null || true
done

for dest_dir in $(printf '%s\n' "${!TOTAL_COUNT[@]}" | sort); do
  echo "✅ $dest_dir: ${TOTAL_COUNT["$dest_dir"]} files, ${UPDATED_COUNT["$dest_dir"]:-0} updated, ${REMOVED_COUNT["$dest_dir"]:-0} removed"
done
