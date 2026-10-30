#!/usr/bin/env bash
# One-time tidy of docs/ (27 Oct 2026): keep only the current runbooks and the diagrams.
# Run from the repo root in Git Bash AFTER unzipping docs_cleanup.zip. Old versions stay in git history.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# 1. old runbook copies and the older architecture design
for f in "docs/Reconcile-POC-Runbook (2).docx" "docs/Reconcile-POC-Runbook (3).docx" \
         "docs/Reconcile-POC-Runbook (5).docx" "docs/Reconcile-POC-Runbook-v1.9-1.pdf" \
         "docs/Self-Reconciling-Data-Pipeline-Architecture-v5.1-combined.pdf"; do
  if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then git rm -q "$f" && echo "removed  $f"; fi
done

# 2. latest architecture design: one clean name
old="docs/Self-Reconciling-Data-Pipeline-Architecture-v5.2-combined (2).pdf"
if git ls-files --error-unmatch "$old" >/dev/null 2>&1; then
  git mv "$old" docs/Self-Reconciling-Data-Pipeline-Architecture-v5.2.pdf && echo "renamed  architecture v5.2"
fi

# 3. loose diagrams → docs/diagrams/ (sources) and docs/images/ (pictures)
mkdir -p docs/diagrams docs/images
mvf() { if git ls-files --error-unmatch "$1" >/dev/null 2>&1; then git mv "$1" "$2" && echo "moved    $1 → $2"; fi; }
mvf docs/flow.mmd                          docs/diagrams/flow.mmd
mvf docs/seq.mmd                           docs/diagrams/agent-sequence.mmd
mvf docs/flow.png                          docs/images/flow.png
mvf docs/seq.png                           docs/images/agent-sequence.png
mvf docs/mermaid-diagram-Agents.png        docs/images/agents.png
mvf docs/Agent-interaction-flow-diagram.png docs/images/agent-interaction-flow.png

# 4. the new files from the zip (the two large v2.0 files only if you copied them into docs/)
for f in docs/RUNBOOK.md docs/Reconcile-POC-Short-Runbook.docx docs/Reconcile-POC-Short-Runbook.pdf \
         docs/images/architecture-overview.png docs/images/decision-gates.png README.md scripts/clean_docs.sh \
         docs/Reconcile-POC-Runbook-v2.0.docx docs/Reconcile-POC-Runbook-v2.0.pdf; do
  if [ -f "$f" ]; then git add "$f"; else echo "missing  $f (skipped)"; fi
done

echo
echo "docs/ now holds:"
git ls-files docs | sed 's/^/  /'
echo
echo "Review with: git status   then commit (see the message in the chat)."
