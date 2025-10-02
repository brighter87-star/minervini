#!/bin/bash
LOG="/home/brighter87/minervini/src/cron/logs/minervini_screening.log"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] 스크립트 실행" >> "$LOG"

cd /home/brighter87/minervini
/home/brighter87/minervini/venv/bin/python -m src.cron.minervini_screening >> "$LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] 스크립트 종료" >> "$LOG"

