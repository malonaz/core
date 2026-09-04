#!/usr/bin/env bash
set -euo pipefail

# Kept bash 3.2 compatible on purpose: macOS ships 3.2 as /bin/bash and plz runs
# us with the restricted [build] path from .plzconfig, which excludes homebrew,
# so `env bash` lands on 3.2 there whatever else is installed. That rules out
# associative arrays, mapfile and [[ -v ]] - anything wanting a hash table is
# done in awk instead, and sets live in files under $WORK_DIR.

JOBS=$( { command -v nproc >/dev/null && nproc; } 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4 )
# BSD mktemp wants an explicit template.
WORK_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t copy_generated_code)
trap 'rm -rf "$WORK_DIR"' EXIT

TARGET_LABELS="$WORK_DIR/target_labels" # target \t label
PKG_LABELS="$WORK_DIR/pkg_labels"       # package \t label, longest package first
DEST_DIRS="$WORK_DIR/dest_dirs"         # one destination per line
RESOLVED="$WORK_DIR/resolved"           # build output \t dest_dir \t rel_path
PAIRS="$WORK_DIR/pairs"                 # src \t dest \t dest_dir
ACTIVE_FILES="$WORK_DIR/active_files"   # every destination path written this run
ACTIVE_DIRS="$WORK_DIR/active_dirs"     # directories those paths live in
REMOVED="$WORK_DIR/removed"             # one dest_dir per file deleted

# "$1" is a list of keys, one per line; writes "count \t key" to "$2".
tally() {
  sort "$1" | uniq -c | awk '{ n = $1; sub(/^ *[0-9]+ +/, ""); print n "\t" $0 }' > "$2"
}

# Reads back a count written by tally(), 0 when the key is absent.
count_of() {
  awk -F'\t' -v key="$2" '$2 == key { c = $1 } END { print c + 0 }' "$1"
}

targets=$(plz query alltargets --hidden --include copy_generated_code*,codegen 2>/dev/null)

if [[ -z "$targets" ]]; then
  echo "No targets found, skipping"
  exit 0
fi

# One query for every target: --label prints a line per target, in order. Falls
# back to the per-target fan-out if the counts don't line up.
target_list=($targets)
plz query print "${target_list[@]}" --label=copy_generated_code: 2>/dev/null > "$WORK_DIR/labels" || true

if [[ $(awk 'END { print NR }' "$WORK_DIR/labels") -eq ${#target_list[@]} ]]; then
  printf '%s\n' "${target_list[@]}" | paste - "$WORK_DIR/labels" | awk -F'\t' '$2 != ""' > "$TARGET_LABELS"
else
  printf '%s\n' "${target_list[@]}" |
    xargs -P"$JOBS" -I{} bash -c 'label=$(plz query print "{}" --label=copy_generated_code: 2>/dev/null); [ -n "$label" ] && printf "%s\t%s\n" "{}" "$label"' > "$TARGET_LABELS"
fi

labeled_targets=()
while IFS= read -r target; do labeled_targets+=("$target"); done < <(cut -f1 "$TARGET_LABELS")

if [[ ${#labeled_targets[@]} -eq 0 ]]; then
  echo "No labeled targets, skipping"
  exit 0
fi

plz build "${labeled_targets[@]}" 2>&1 | grep "plz-out/gen/" > "$WORK_DIR/build_output" || true

# A file belongs to the longest package prefixing it; ties keep the first target
# in the sorted order. Looking that up per file beats scanning every output once
# per target.
awk -F'\t' '{ pkg = $1; sub(/:.*$/, "", pkg); sub(/^\/\//, "", pkg); print length(pkg) "\t" pkg "\t" $2 }' "$TARGET_LABELS" |
  sort -rn -k1,1 |
  awk -F'\t' '!($2 in seen) { seen[$2] = 1; print $2 "\t" $3 }' > "$PKG_LABELS"

# Register every destination up front so stale cleanup still runs for a
# destination whose targets produced no outputs this run.
cut -f2 "$TARGET_LABELS" |
  awk '{ i = index($0, ":"); d = (i ? substr($0, i + 1) : ""); print (d == "" ? "." : d) }' |
  sort -u > "$DEST_DIRS"

# Walk each output path up to its package, then apply that package's label.
awk -F'\t' '
  NR == FNR { label[$1] = $2; next }
  {
    file = $0
    sub(/^[ \t]+/, "", file)
    sub(/[ \t]+$/, "", file)
    if (file == "") next

    rel = file
    sub(/^plz-out\/gen\//, "", rel)

    dir = rel
    lbl = ""
    while (index(dir, "/") > 0) {
      sub(/\/[^\/]*$/, "", dir)
      if (dir in label) { lbl = label[dir]; break }
    }
    if (lbl == "") next

    i = index(lbl, ":")
    strip = (i ? substr(lbl, 1, i - 1) : lbl)
    dest = (i ? substr(lbl, i + 1) : "")
    if (dest == "") dest = "."

    if (strip != "") {
      prefix = strip "/"
      if (substr(rel, 1, length(prefix)) == prefix) rel = substr(rel, length(prefix) + 1)
      # output path may equal the prefix exactly (directory outputs)
      if (rel == strip) rel = ""
    }

    print file "\t" dest "\t" rel
  }
' "$PKG_LABELS" "$WORK_DIR/build_output" > "$RESOLVED"

# Collect every copy as a src/dest/dest_dir triple; the copying itself is done
# in parallel below.
while IFS=$'\t' read -r file dest_dir rel_path; do
  # build outputs can be directories; copy them recursively
  if [[ -d "$file" ]]; then
    target_dir="$dest_dir${rel_path:+/$rel_path}"
    while read -r copied; do
      printf '%s\t%s\t%s\n' "$copied" "$target_dir/${copied#$file/}" "$dest_dir"
    done < <(find "$file" -type f)
    continue
  fi

  # rel_path is empty only when the output path equalled the strip prefix
  printf '%s\t%s\t%s\n' "$file" "$dest_dir/${rel_path:-${file##*/}}" "$dest_dir"
done < "$RESOLVED" > "$PAIRS"

cut -f2 "$PAIRS" | sort -u > "$ACTIVE_FILES"
# Directories that received a file this run; a BUILD.plz anywhere else is stale.
sed 's|/[^/]*$||' "$ACTIVE_FILES" | sort -u > "$ACTIVE_DIRS"

cut -f3 "$PAIRS" > "$WORK_DIR/total_keys"
tally "$WORK_DIR/total_keys" "$WORK_DIR/total_counts"
: > "$WORK_DIR/updated_counts"

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

  cat "$WORK_DIR"/chunk.*.updated > "$WORK_DIR/updated_keys"
  tally "$WORK_DIR/updated_keys" "$WORK_DIR/updated_counts"
fi

: > "$REMOVED"

while IFS= read -r dest_dir; do
  [[ ! -d "$dest_dir" || "$dest_dir" == "." ]] && continue

  find "$dest_dir" -type f ! -name "BUILD.plz" | grep -Fxv -f "$ACTIVE_FILES" > "$WORK_DIR/stale" || true

  find "$dest_dir" -type f -name "BUILD.plz" | awk -v dirs="$ACTIVE_DIRS" '
    BEGIN { while ((getline dir < dirs) > 0) active[dir] = 1 }
    { d = $0; sub(/\/[^\/]*$/, "", d); if (!(d in active)) print }
  ' >> "$WORK_DIR/stale"

  while IFS= read -r stale; do
    rm -f "$stale"
    echo "$dest_dir" >> "$REMOVED"
  done < "$WORK_DIR/stale"

  find "$dest_dir" -type d -empty -delete 2>/dev/null || true
done < "$DEST_DIRS"

tally "$REMOVED" "$WORK_DIR/removed_counts"

# Union of the two so a destination that only had removals still reports them.
while IFS= read -r dest_dir; do
  echo "✅ $dest_dir: $(count_of "$WORK_DIR/total_counts" "$dest_dir") files, $(count_of "$WORK_DIR/updated_counts" "$dest_dir") updated, $(count_of "$WORK_DIR/removed_counts" "$dest_dir") removed"
done < <(cut -f2 "$WORK_DIR/total_counts" "$WORK_DIR/removed_counts" | sort -u)
