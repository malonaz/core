#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="plz-out/gen"

declare -A ACTIVE_FILES
declare -A DEST_DIRS
declare -A TOTAL_COUNT
declare -A UPDATED_COUNT
declare -A REMOVED_COUNT

JOBS=$( { command -v nproc >/dev/null && nproc; } 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4 )
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

targets=$(plz query alltargets --hidden --include copy_generated_code*,codegen 2>/dev/null)

if [[ -z "$targets" ]]; then
  echo "No targets found, skipping"
  exit 0
fi

# One query for every target: --label prints a line per target, in order. Falls
# back to the per-target fan-out if the counts don't line up.
declare -A TARGET_LABELS
target_list=($targets)
mapfile -t label_list < <(plz query print "${target_list[@]}" --label=copy_generated_code: 2>/dev/null)

if [[ ${#label_list[@]} -eq ${#target_list[@]} ]]; then
  for i in "${!target_list[@]}"; do
    [[ -n "${label_list[$i]}" ]] && TARGET_LABELS["${target_list[$i]}"]="${label_list[$i]}"
  done
else
  while IFS=$'\t' read -r target label; do
    [[ -n "$label" ]] && TARGET_LABELS["$target"]="$label"
  done < <(
    printf '%s\n' "${target_list[@]}" |
      xargs -P"$JOBS" -I{} bash -c 'label=$(plz query print "{}" --label=copy_generated_code: 2>/dev/null); [[ -n "$label" ]] && printf "%s\t%s\n" "{}" "$label"'
  )
fi

labeled_targets=("${!TARGET_LABELS[@]}")
if [[ ${#labeled_targets[@]} -eq 0 ]]; then
  echo "No labeled targets, skipping"
  exit 0
fi

build_output=$(plz build "${labeled_targets[@]}" 2>&1 | grep "plz-out/gen/" || true)

IFS=$'\n' sorted_targets=($(for t in "${labeled_targets[@]}"; do pkg="${t%%:*}"; pkg="${pkg#//}"; printf '%d\t%s\n' "${#pkg}" "$t"; done | sort -rn | cut -f2))
unset IFS

# A file belongs to the longest package prefixing it; ties keep the first target
# in the sorted order. Looking that up per file beats scanning every output once
# per target.
declare -A PKG_LABEL
for target in "${sorted_targets[@]}"; do
  pkg="${target%%:*}"
  pkg="${pkg#//}"
  [[ -v PKG_LABEL["$pkg"] ]] || PKG_LABEL["$pkg"]="${TARGET_LABELS[$target]}"
done

# Register every destination up front so stale cleanup still runs for a
# destination whose targets produced no outputs this run.
for target in "${labeled_targets[@]}"; do
  IFS=':' read -r _ dest_dir <<< "${TARGET_LABELS[$target]}"
  DEST_DIRS["${dest_dir:-.}"]=1
done

# Collect every copy as a src/dest/dest_dir triple; the copying itself is done
# in parallel below.
PAIRS="$WORK_DIR/pairs"
: > "$PAIRS"

record() {
  local src="$1" dest="$2" dest_dir="$3"
  ACTIVE_FILES["$dest"]=1
  TOTAL_COUNT["$dest_dir"]=$(( ${TOTAL_COUNT["$dest_dir"]:-0} + 1 ))
  printf '%s\t%s\t%s\n' "$src" "$dest" "$dest_dir" >> "$PAIRS"
}

for file in $build_output; do
  rel_path="${file#plz-out/gen/}"

  label=""
  dir="$rel_path"
  while [[ "$dir" == */* ]]; do
    dir="${dir%/*}"
    if [[ -v PKG_LABEL["$dir"] ]]; then
      label="${PKG_LABEL[$dir]}"
      break
    fi
  done
  [[ -z "$label" ]] && continue

  IFS=':' read -r strip_prefix dest_dir <<< "$label"
  dest_dir="${dest_dir:-.}"

  if [[ -n "$strip_prefix" ]]; then
    rel_path="${rel_path#$strip_prefix/}"
    # output path may equal the prefix exactly (directory outputs)
    [[ "$rel_path" == "$strip_prefix" ]] && rel_path=""
  fi

  # build outputs can be directories; copy them recursively
  if [[ -d "$file" ]]; then
    target_dir="$dest_dir${rel_path:+/$rel_path}"
    while read -r copied; do
      record "$copied" "$target_dir/${copied#$file/}" "$dest_dir"
    done < <(find "$file" -type f)
    continue
  fi

  # rel_path is empty only when the output path equalled the strip prefix
  record "$file" "$dest_dir/${rel_path:-${file##*/}}" "$dest_dir"
done

# Copy in parallel, skipping files whose contents already match so that only
# real changes are written (and reported).
if [[ -s "$PAIRS" ]]; then
  awk -v jobs="$JOBS" -v out="$WORK_DIR/chunk." '{print > (out (NR % jobs))}' "$PAIRS"
  for chunk in "$WORK_DIR"/chunk.*; do
    (
      while IFS=$'\t' read -r src dest dest_dir; do
        cmp -s "$src" "$dest" && continue
        mkdir -p "${dest%/*}"
        cp -f "$src" "$dest"
        echo "$dest_dir"
      done < "$chunk" > "$chunk.updated"
    ) &
  done
  wait

  while read -r dest_dir; do
    UPDATED_COUNT["$dest_dir"]=$(( ${UPDATED_COUNT["$dest_dir"]:-0} + 1 ))
  done < <(cat "$WORK_DIR"/chunk.*.updated)
fi

# Directories that received a file this run; a BUILD.plz anywhere else is stale.
declare -A ACTIVE_DIRS
for f in "${!ACTIVE_FILES[@]}"; do ACTIVE_DIRS["${f%/*}"]=1; done

for dest_dir in "${!DEST_DIRS[@]}"; do
  [[ ! -d "$dest_dir" || "$dest_dir" == "." ]] && continue

  while read -r f; do
    [[ ! -v ACTIVE_FILES["$f"] ]] && rm -f "$f" && REMOVED_COUNT["$dest_dir"]=$(( ${REMOVED_COUNT["$dest_dir"]:-0} + 1 ))
  done < <(find "$dest_dir" -type f ! -name "BUILD.plz")

  while read -r build_file; do
    [[ ! -v ACTIVE_DIRS["${build_file%/*}"] ]] && rm -f "$build_file" && REMOVED_COUNT["$dest_dir"]=$(( ${REMOVED_COUNT["$dest_dir"]:-0} + 1 ))
  done < <(find "$dest_dir" -type f -name "BUILD.plz")

  find "$dest_dir" -type d -empty -delete 2>/dev/null || true
done

# Union of the two so a destination that only had removals still reports them.
for dest_dir in $(printf '%s\n' "${!TOTAL_COUNT[@]}" "${!REMOVED_COUNT[@]}" | sort -u); do
  echo "✅ $dest_dir: ${TOTAL_COUNT["$dest_dir"]:-0} files, ${UPDATED_COUNT["$dest_dir"]:-0} updated, ${REMOVED_COUNT["$dest_dir"]:-0} removed"
done
