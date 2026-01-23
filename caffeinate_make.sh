#!/bin/sh
set -u

### ================== НАСТРОЙКИ (менять тут) ==================
DELAY=0       # 65 минут до первого запуска
INTERVAL=5400    # 90 минут между запусками
TICK=300         # печатать обратный отсчёт раз в 5 минут

# (опционально) папка проекта, где надо выполнять make
WORKDIR=/Users/alexanderonishchenko/Documents/_Projects/fetchgraph

LOG="$HOME/batch_tag.log"

# Команда для ПЕРВОГО запуска
FIRST_CMD='make batch-tag TAG=my_tag NOTE="прогон перед мерджем"'

# Команда для ПОВТОРНЫХ запусков
REPEAT_CMD='make batch-tag TAG=my_tag NOTE="прогон перед мерджем"'
### ============================================================

LOCKDIR="/tmp/batch_tag_runner.lock"

log() { printf '%s\n' "$*" | tee -a "$LOG"; }

cleanup() {
  [ -n "${CAF_PID:-}" ] && kill "$CAF_PID" 2>/dev/null || true
  rmdir "$LOCKDIR" 2>/dev/null || true
}
trap 'cleanup' EXIT INT TERM HUP

# Защита от двух копий
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  echo "Похоже, уже запущено (lock: $LOCKDIR). Если уверены — удалите lock и запустите снова." >&2
  exit 1
fi

log "PID $$ started at $(date '+%F %T')"

# Не даём Mac уснуть
if command -v caffeinate >/dev/null 2>&1; then
  caffeinate -dimsu -w $$ &
  CAF_PID=$!
  log "caffeinate pid: $CAF_PID"
else
  log "WARNING: caffeinate не найден — Mac может уснуть."
fi

# Переходим в папку проекта (если существует)
if [ -d "$WORKDIR" ]; then
  cd "$WORKDIR" || exit 1
else
  log "WARNING: WORKDIR не существует: $WORKDIR (останусь в текущей папке)"
fi

countdown() {
  total="$1"
  label="$2"

  while [ "$total" -gt 0 ]; do
    mins=$(( total / 60 ))
    secs=$(( total % 60 ))
    log "$label: осталось ${mins}m$(printf '%02d' "$secs")s ($(date '+%F %T'))"

    step=$TICK
    [ "$total" -lt "$step" ] && step=$total
    sleep "$step" || exit 1
    total=$(( total - step ))
  done
}

run_cmd() {
  label="$1"
  cmd="$2"

  log "---- $label $(date '+%F %T') ----"
  log "CMD: $cmd"
  sh -c "$cmd" 2>&1 | tee -a "$LOG"
  log ""
}

countdown "$DELAY" "До первого запуска"
run_cmd "FIRST RUN" "$FIRST_CMD"

while :; do
  countdown "$INTERVAL" "До следующего запуска"
  run_cmd "REPEAT RUN" "$REPEAT_CMD"
done
