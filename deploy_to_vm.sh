#!/usr/bin/env bash
# Deploy: push tracked git files to VM (broadsign00)
# Usage: ./deploy_to_vm.sh [--dry-run]
#
# Używa git archive + ssh tar — nie wymaga rsync ani sudo na VM.
# Kopiuje tylko pliki śledzone przez git (pomija Data/, .env, __pycache__).

set -euo pipefail

VM_USER="janr"
VM_HOST="10.1.2.19"
VM_PATH="/dane/BroadsignApi"
SSH_KEY="/c/Users/janr/.ssh/id_ed25519"
SSH="ssh -i $SSH_KEY"

if [[ "${1:-}" == "--dry-run" ]]; then
  echo "[dry-run] Pliki które zostaną wysłane:"
  git archive HEAD | tar t
  exit 0
fi

LOCAL_COMMIT=$(git rev-parse --short HEAD)
LOCAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)

echo "Deploy: $LOCAL_BRANCH @ $LOCAL_COMMIT -> $VM_USER@$VM_HOST:$VM_PATH"
echo ""

# Wyślij wszystkie pliki śledzone przez git jako tarball.
# Po rozpakowaniu konwertuj CRLF->LF (git archive na Windows może dodać \r).
git archive HEAD | $SSH "$VM_USER@$VM_HOST" \
  "cd $VM_PATH && tar xf - && find . -name '*.py' -not -path '*/__pycache__/*' -exec sed -i 's/\r//' {} +"

echo "OK — deploy zakończony."
echo ""

# Weryfikacja: MD5 na gitowym obiekcie (zawsze LF) vs VM
echo "Weryfikacja (MD5 3 plików):"
for f in "ScriptLinux_v4.py" "Pipeline/gold/build_fact_play_logs.py" "Pipeline/silver/build_play_logs.py"; do
  local_hash=$(git cat-file blob "HEAD:$f" | md5sum | cut -d' ' -f1)
  vm_hash=$($SSH "$VM_USER@$VM_HOST" "md5sum $VM_PATH/$f | cut -d' ' -f1")
  if [[ "$local_hash" == "$vm_hash" ]]; then
    echo "  OK  $f"
  else
    echo "  DIFF $f (local=$local_hash vm=$vm_hash)"
  fi
done
