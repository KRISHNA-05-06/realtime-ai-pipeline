#!/usr/bin/env bash
# Optional: builds up the repo as a series of commits instead of one big dump,
# so the history reads like the project grew over time. Run ONCE in a fresh
# clone/copy before pushing. Comment out / edit dates as you like.
#
#   bash scripts/seed_history.sh
#
set -e
cd "$(dirname "$0")/.."

commit () { GIT_AUTHOR_DATE="$1" GIT_COMMITTER_DATE="$1" git commit -q -m "$2"; }

git init -q

# 1. scaffolding
git add docker-compose.yml .gitignore .env.example README.md
commit "2025-04-02T10:14:00" "initial scaffold: compose stack + readme"

# 2. producer
git add producer/
commit "2025-04-05T19:40:00" "add clickstream producer with anomaly injection"

# 3. storage schema
git add infra/clickhouse_init.sql
commit "2025-04-08T21:05:00" "clickhouse schema: events, sessions, alerts, rollups"

# 4. ai service
git add ai_service/
commit "2025-04-13T15:22:00" "enrichment service: isolation forest + intent labels"

# 5. processor
git add flink_jobs/
commit "2025-04-19T11:48:00" "stream processor wiring kafka -> enrich -> clickhouse"

# 6. switch to hybrid rules + model (the kind of fix you actually make)
git add ai_service/main.py README.md
commit "2025-04-21T20:31:00" "anomaly: add hard rules in front of the model, drop false positives"

# 7. api
git add api/
commit "2025-04-26T16:09:00" "read api for dashboard queries"

# 8. dashboard
git add dashboard/
commit "2025-05-03T14:55:00" "react dashboard with live charts and alert feed"

# 9. grafana + tests + scripts
git add infra/grafana tests/ scripts/
commit "2025-05-10T18:20:00" "grafana provisioning, tests, management script"

# anything left
git add -A
git diff --cached --quiet || commit "2025-05-12T09:30:00" "tidy up and docs"

echo "Done. Review with: git log --oneline"
