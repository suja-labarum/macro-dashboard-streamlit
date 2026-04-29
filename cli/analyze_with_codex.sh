#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="${HOME}/.macro_dashboard"
POSTS_FILE="${BASE_DIR}/x_intel_posts.json"
OUTPUT_FILE="${BASE_DIR}/x_intel_analyzed.json"
MAX_IMAGES="${MAX_IMAGES:-30}"

if [ "${TERM:-dumb}" = "dumb" ]; then
  export TERM="xterm-256color"
fi

mkdir -p "${BASE_DIR}"

if [ ! -s "${POSTS_FILE}" ]; then
  echo "No scraped posts found at ${POSTS_FILE}"
  exit 0
fi

if [ ! -f "${OUTPUT_FILE}" ]; then
  printf '[]\n' > "${OUTPUT_FILE}"
fi

JOBS=()
JOBS_FILE="$(mktemp)"
python3 - "${POSTS_FILE}" "${OUTPUT_FILE}" "${MAX_IMAGES}" <<'PY' >"${JOBS_FILE}"
from collections import defaultdict
from datetime import datetime
import json
import sys

posts_path, output_path, max_images_raw = sys.argv[1:4]
max_images = int(max_images_raw)

with open(posts_path, "r", encoding="utf-8") as handle:
    posts = json.load(handle) or []

with open(output_path, "r", encoding="utf-8") as handle:
    analyzed = json.load(handle) or []

existing_by_path = {entry.get("image_path"): entry for entry in analyzed if entry.get("image_path")}

gamma_accounts = {"spotgamma", "menthorqpro"}
gamma_keywords = ("gamma", "gex", "dex", "call wall", "put wall", "gamma flip", "positive gamma", "negative gamma")
cta_keywords = ("cta", "systematic", "positioning", "trigger", "trend following")

def parse_dt(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return datetime.min

def infer_priority(post, image_path):
    text = f"{post.get('text', '')} {post.get('author_handle', '')}".lower()
    account = str(post.get("source_account", "")).lower()
    existing = existing_by_path.get(image_path)
    analysis = (existing or {}).get("analysis", {})

    already_analyzed = existing is not None
    needs_upgrade = (
        not analysis
        or "theme" not in analysis
        or "dashboard_points" not in analysis
        or "source_date" not in analysis
        or analysis.get("theme") == "option_gamma"
    )
    if already_analyzed and not needs_upgrade:
        return None

    priority = 100 if not already_analyzed else 20
    if account in gamma_accounts:
        priority += 60
    if any(keyword in text for keyword in gamma_keywords):
        priority += 80
    if any(keyword in text for keyword in cta_keywords):
        priority += 180
    return {
        "priority": priority,
        "already_analyzed": already_analyzed,
        "needs_upgrade": needs_upgrade,
    }

jobs_by_account = defaultdict(list)

for post in posts:
    created_at = parse_dt(post.get("created_at"))
    for image_path in post.get("image_paths", []):
        meta = infer_priority(post, image_path)
        if meta is None:
            continue
        payload = dict(post)
        payload["image_path"] = image_path
        payload["_priority"] = meta["priority"]
        payload["_already_analyzed"] = meta["already_analyzed"]
        payload["_needs_upgrade"] = meta["needs_upgrade"]
        payload["_created_at_sort"] = created_at.isoformat()
        jobs_by_account[str(post.get("source_account", "")).lower()].append(payload)

for items in jobs_by_account.values():
    items.sort(
        key=lambda item: (
            int(item.get("_priority", 0)),
            0 if not item.get("_already_analyzed") else 1,
            item.get("_created_at_sort", ""),
        ),
        reverse=True,
    )

ordered = []
seen_paths = set()

# First pass: guarantee breadth across all tracked accounts.
for account in sorted(
    jobs_by_account.keys(),
    key=lambda account: (
        max((int(item.get("_priority", 0)) for item in jobs_by_account[account]), default=0),
        account,
    ),
    reverse=True,
):
    if jobs_by_account[account]:
        item = jobs_by_account[account].pop(0)
        ordered.append(item)
        seen_paths.add(item["image_path"])

# Second pass: prioritize fresh CTA / option gamma posts, then other fresh posts, then upgrades.
remaining = []
for items in jobs_by_account.values():
    remaining.extend(items)

remaining.sort(
    key=lambda item: (
        int(item.get("_priority", 0)),
        0 if not item.get("_already_analyzed") else 1,
        item.get("_created_at_sort", ""),
    ),
    reverse=True,
)

for item in remaining:
    if item["image_path"] in seen_paths:
        continue
    ordered.append(item)
    seen_paths.add(item["image_path"])

for payload in ordered[:max_images]:
    payload.pop("_priority", None)
    payload.pop("_already_analyzed", None)
    payload.pop("_needs_upgrade", None)
    payload.pop("_created_at_sort", None)
    print(json.dumps(payload, ensure_ascii=False))
PY
while IFS= read -r line; do
  JOBS+=("${line}")
done < "${JOBS_FILE}"
rm -f "${JOBS_FILE}"

if [ "${#JOBS[@]}" -eq 0 ]; then
  echo "No unanalyzed images found."
  exit 0
fi

COUNT=0

for JOB_JSON in "${JOBS[@]}"; do
  COUNT=$((COUNT + 1))
  PROMPT_FILE="$(mktemp)"
  OUTPUT_TMP="$(mktemp)"
  ERROR_TMP="$(mktemp)"

  python3 - "${JOB_JSON}" "${PROMPT_FILE}" <<'PY'
import json
import sys

job = json.loads(sys.argv[1])
prompt_path = sys.argv[2]

reply_lines = []
for reply in job.get("replies", []):
    handle = reply.get("author_handle", "unknown")
    followers = reply.get("author_followers") or 0
    text = (reply.get("text") or "").strip()
    reply_lines.append(f"@{handle} ({followers} followers): {text}")

parent_tweet = job.get("parent_tweet") or {}
quoted_tweet = job.get("quoted_tweet") or {}
parent_line = "No parent tweet context."
quoted_line = "No quoted tweet context."
if parent_tweet:
    parent_line = f"@{parent_tweet.get('author', 'unknown')}: {parent_tweet.get('text', '')}"
if quoted_tweet:
    quoted_line = f"@{quoted_tweet.get('author', 'unknown')}: {quoted_tweet.get('text', '')}"

prompt = (
    f"Author: @{job.get('author_handle')} ({job.get('author_followers', 0)} followers, "
    f"verified: {job.get('author_verified')})\n"
    f"Caption: {job.get('text', '')}\n"
    f"Engagement: {job.get('likes', 0)} likes, {job.get('retweets', 0)} RTs, "
    f"{job.get('views', 0)} views, {job.get('bookmarks', 0)} bookmarks\n"
    f"Parent tweet context: {parent_line}\n"
    f"Quoted tweet context: {quoted_line}\n"
    "Replies:\n"
    f"{chr(10).join(reply_lines) if reply_lines else 'No replies captured.'}\n"
    "Analyze this financial chart image with the above post context. Return ONLY raw JSON no markdown "
    "with keys: title, source_service (which paid service e.g. Goldman GS Prime SG JPM 21stCenturyAdvisor), "
    "metric, time_range, latest_value, trend (bullish/bearish/neutral), key_levels (array of strings), "
    "author_interpretation (what the caption says about the chart), community_sentiment (bullish/bearish/mixed "
    "based on replies), notable_replies (array of top 3 reply texts), signal_for_dashboard (one sentence "
    "actionable macro signal), confidence (high/medium/low based on chart readability), "
    "dashboard_points (array of objects with keys label, value, unit, role), "
    "theme (one of cta, optiongamma, macro, other), source_date (the date shown on the chart itself, not the tweet date). "
    "dashboard_points.role must be one of: spot, resistance, support, current_position, trigger, capitulation, vol_trigger, call_wall, put_wall, hvi, other. "
    "Extract ALL numeric levels visible on the chart axes and annotations. "
    "For CTA charts extract visible levels or positioning values such as spot, CTA line, current position, triggers, support, resistance, capitulation. "
    "For option gamma charts extract visible levels or values such as spot, call wall, put wall, support, resistance, HVI, vol trigger, GEX, DEX. "
    "Use numeric values only in dashboard_points.value and return [] when no reliable numeric points are visible."
)

with open(prompt_path, "w", encoding="utf-8") as handle:
    handle.write(prompt)
PY

  IMAGE_PATH="$(python3 - "${JOB_JSON}" <<'PY'
import json
import sys
print(json.loads(sys.argv[1])["image_path"])
PY
)"

  AUTHOR_HANDLE="$(python3 - "${JOB_JSON}" <<'PY'
import json
import sys
print(json.loads(sys.argv[1]).get("author_handle", "unknown"))
PY
)"

  if ! codex exec \
    --full-auto \
    --skip-git-repo-check \
    --image "${IMAGE_PATH}" \
    --output-last-message "${OUTPUT_TMP}" \
    - <"${PROMPT_FILE}" >"${ERROR_TMP}" 2>&1; then
    echo "Codex analysis failed for ${IMAGE_PATH}"
    cat "${ERROR_TMP}"
    rm -f "${PROMPT_FILE}" "${OUTPUT_TMP}" "${ERROR_TMP}"
    continue
  fi

  if ! TITLE="$(
    python3 - "${JOB_JSON}" "${OUTPUT_TMP}" "${OUTPUT_FILE}" <<'PY'
import datetime as dt
import json
import sys

job = json.loads(sys.argv[1])
output_path = sys.argv[2]
store_path = sys.argv[3]

with open(output_path, "r", encoding="utf-8") as handle:
    raw = handle.read().strip()

analysis = None
for candidate in (raw, raw[raw.find("{"): raw.rfind("}") + 1] if "{" in raw and "}" in raw else ""):
    if not candidate:
        continue
    try:
        analysis = json.loads(candidate)
        break
    except json.JSONDecodeError:
        continue

if analysis is None:
    raise SystemExit("Unable to parse JSON from codex output")

with open(store_path, "r", encoding="utf-8") as handle:
    existing = json.load(handle) or []

record = dict(job)
record["analysis"] = analysis
record["analyzed_at"] = dt.datetime.utcnow().isoformat() + "Z"

updated = []
replaced = False
for entry in existing:
    if entry.get("image_path") == job.get("image_path"):
        updated.append(record)
        replaced = True
    else:
        updated.append(entry)
if not replaced:
    updated.append(record)

with open(store_path, "w", encoding="utf-8") as handle:
    json.dump(updated, handle, ensure_ascii=False, indent=2)

print(analysis.get("title", "Untitled chart"))
PY
  )"; then
    echo "Failed to parse or persist analysis for ${IMAGE_PATH}"
    cat "${OUTPUT_TMP}"
    rm -f "${PROMPT_FILE}" "${OUTPUT_TMP}" "${ERROR_TMP}"
    continue
  fi

  echo "[${COUNT}/${MAX_IMAGES}] @${AUTHOR_HANDLE} - ${TITLE} found"
  rm -f "${PROMPT_FILE}" "${OUTPUT_TMP}" "${ERROR_TMP}"
done
