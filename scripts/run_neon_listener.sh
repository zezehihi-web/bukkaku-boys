#!/bin/bash
# Neon リスナー自動再起動ラッパー
# 使い方: bash scripts/run_neon_listener.sh

export DATABASE_URL="postgresql://neondb_owner:npg_t8L6IUlWuKgF@ep-floral-bread-a1kdt3p1-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8

cd "C:/Users/yamag/空確くん"
set -a && source .env && set +a

while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] neon_listener を起動..."
    python -u -m backend.neon_listener
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] neon_listener 終了 (exit=$EXIT_CODE)。10秒後に再起動..."
    sleep 10
done
