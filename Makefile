# Makefile — алиасы для examples.demo_qa (без ~/.bashrc / ~/.zshrc)
#
# Быстрый старт:
#   make init
#   make chat
#   make batch
#   make help
#
# Примечание про venv:
# - Makefile НЕ "активирует" venv в текущем терминале (это невозможно из make).
# - Но он автоматически использует .venv/bin/python, если он существует.

SHELL := /bin/bash

# ==============================================================================
# 1) Локальный конфиг (не коммитить; удобно добавить в .gitignore)
# ==============================================================================
CONFIG ?= .demo_qa.mk
-include $(CONFIG)

# ==============================================================================
# 2) Значения по умолчанию (для make init)
# ==============================================================================
DEFAULT_DATA   := _demo_data/shop
DEFAULT_SCHEMA := _demo_data/shop/schema.yaml
DEFAULT_CASES  := examples/demo_qa/cases/retail_cases.json

# ==============================================================================
# 3) Python / CLI
# ==============================================================================
VENV   ?= .venv
PYTHON ?= $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python)
CLI    := $(PYTHON) -m examples.demo_qa.cli

# ==============================================================================
# 4) Пути demo_qa (можно переопределять через CLI или в $(CONFIG))
# ==============================================================================
DATA   ?=
SCHEMA ?=
CASES  ?=
OUT    ?= $(DATA)/.runs/results.jsonl

# ==============================================================================
# 5) Параметры команд
# ==============================================================================
TAG   ?=
NOTE  ?=
CASE  ?=
LIMIT ?= 50
CHANGES ?= 10
NEW_TAG ?=
PATTERN ?=
TAGS_FORMAT ?= table
TAGS_COLOR ?= auto

ONLY_FAILED_FROM ?=
ONLY_MISSED_FROM ?=

BASE     ?=
NEW      ?=
DIFF_OUT ?= $(DATA)/.runs/diff.md
JUNIT    ?= $(DATA)/.runs/diff.junit.xml
BASE_TAG ?= baseline
COMPARE_TAG_OUT ?= $(DATA)/.runs/diff.tags.md
COMPARE_TAG_JUNIT ?= $(DATA)/.runs/diff.tags.junit.xml

MAX_FAILS ?= 5

PURGE_RUNS ?= 0
PRUNE_HISTORY ?= 0
PRUNE_CASE_HISTORY ?= 0
DRY ?= 0

# ==============================================================================
# 6) Настройки LLM-конфига (редактирование/просмотр)
# ==============================================================================
# Если у тебя конфиг лежит иначе — переопредели:
#   make llm-edit LLM_TOML=path/to/demo_qa.toml
LLM_TOML ?= demo_qa.toml
LLM_TOML_EXAMPLE ?= demo_qa.toml.example

# macOS: открываем в TextEdit
OPEN ?= open
EDITOR_APP ?= TextEdit

# ==============================================================================
# 7) Вспомогательные флаги (не передавать пустые)
# ==============================================================================
TAG_FLAG   := $(if $(strip $(TAG)),--tag "$(TAG)",)
NOTE_FLAG  := $(if $(strip $(NOTE)),--note "$(NOTE)",)
LIMIT_FLAG := $(if $(strip $(LIMIT)),--limit $(LIMIT),)

# ==============================================================================
# 8) PHONY
# ==============================================================================
.PHONY: help init show-config check ensure-runs-dir venv-check \
        llm-init llm-show llm-edit \
        chat \
        batch batch-tag batch-failed batch-failed-from \
        batch-missed batch-missed-from batch-failed-tag batch-missed-tag \
        batch-fail-fast batch-max-fails \
        stats history-case report-tag report-tag-changes tags tag-rm case-run case-open compare compare-tag

# ==============================================================================
# help (на русском)
# ==============================================================================
help:
	@echo ""
	@echo "DemoQA: Makefile-алиасы (без ~/.bashrc или ~/.zshrc)"
	@echo "==================================================="
	@echo ""
	@echo "Быстрый старт:"
	@echo "  make init"
	@echo "  make chat"
	@echo ""
	@echo "Конфигурация:"
	@echo "  Настройки хранятся в: $(CONFIG)"
	@echo "  Можно переопределять переменные так:"
	@echo "    make chat DATA=_demo_data/shop SCHEMA=_demo_data/shop/schema.yaml"
	@echo ""
	@echo "Основные переменные:"
	@echo "  DATA     - путь к датасету (например: _demo_data/shop)"
	@echo "  SCHEMA   - путь к schema.yaml"
	@echo "  CASES    - путь к cases.json"
	@echo "  OUT      - куда писать results.jsonl (по умолчанию: \$$DATA/.runs/results.jsonl)"
	@echo ""
	@echo "Команды:"
	@echo "  make chat                 - интерактивный чат"
	@echo "  make batch                - полный прогон всего набора"
	@echo "  make batch-tag TAG=... NOTE='...'  - полный прогон с тегом и заметкой"
	@echo "  make batch-failed         - перепрогон только упавших (baseline = latest)"
	@echo "  make batch-failed-from ONLY_FAILED_FROM=path/results.jsonl  - only-failed от явного baseline"
	@echo "  make batch-missed [TAG=...] - добить missed (если TAG задан — относительно effective по тегу)"
	@echo "  make batch-failed-tag TAG=...   - добить failed/error/mismatch относительно effective snapshot тега"
	@echo "  make batch-missed-tag TAG=...   - добить missed относительно effective snapshot тега"
	@echo "  make batch-missed-from ONLY_MISSED_FROM=path/results.jsonl  - добить missed от явного baseline"
	@echo "  make batch-fail-fast      - быстрый smoke (остановиться на первом фейле)"
	@echo "  make batch-max-fails MAX_FAILS=5 - остановиться после N фейлов"
	@echo "  make stats                - stats по последним 10 прогонов"
	@echo "  make tags                 - список тегов (effective snapshots)"
	@echo ""
	@echo "Диагностика / анализ:"
	@echo "  make history-case CASE=case_42 [TAG=...] [LIMIT=50] - история по кейсу"
	@echo "  make report-tag TAG=...    - сводка по тегу (effective snapshot)"
	@echo "  make report-tag-changes TAG=... [CHANGES=10] - сводка + последние изменения effective snapshot"
	@echo "  make tags [PATTERN=*] DATA=... - показать список тегов"
	@echo "  make case-run  CASE=case_42 - прогнать один кейс"
	@echo "  make case-open CASE=case_42 - открыть артефакты кейса"
	@echo ""
	@echo "Уборка:"
	@echo "  make tag-rm TAG=... [DRY=1] [PURGE_RUNS=1] [PRUNE_HISTORY=1] [PRUNE_CASE_HISTORY=1]"
	@echo "    - удаляет effective snapshot тега и tag-latest* указатели"
	@echo "    DRY=1                - dry-run: только показать, что будет удалено"
	@echo "    PURGE_RUNS=1          - дополнительно удалить все runs, где run_meta.tag == TAG"
	@echo "    PRUNE_HISTORY=1       - вычистить записи с этим тегом из $${DATA}/.runs/history.jsonl"
	@echo "    PRUNE_CASE_HISTORY=1  - вычистить записи с этим тегом из $${DATA}/.runs/runs/cases/*.jsonl"
	@echo ""
	@echo "Сравнение результатов:"
	@echo "  make compare BASE=... NEW=... [DIFF_OUT=...] [JUNIT=...]"
	@echo "  make compare-tag BASE_TAG=baseline NEW_TAG=... [COMPARE_TAG_OUT=...] [COMPARE_TAG_JUNIT=...]"
	@echo ""
	@echo "LLM конфиг:"
	@echo "  make llm-init             - создать $(LLM_TOML) из $(LLM_TOML_EXAMPLE)"
	@echo "  make llm-show             - показать первые ~200 строк $(LLM_TOML)"
	@echo "  make llm-edit             - открыть $(LLM_TOML) в TextEdit (macOS)"
	@echo ""
	@echo "Сервисные:"
	@echo "  make venv-check           - показать, какой python будет использоваться"
	@echo "  make show-config          - показать текущие значения переменных"
	@echo ""

# ==============================================================================
# Конфиг проекта
# ==============================================================================
init:
	@set -euo pipefail; \
	if [ -f "$(CONFIG)" ] && [ "$${FORCE:-0}" != "1" ]; then \
	  echo "Файл $(CONFIG) уже существует. Чтобы перезаписать: FORCE=1 make init"; \
	  exit 1; \
	fi; \
	DATA="$${DATA:-$(DEFAULT_DATA)}"; \
	SCHEMA="$${SCHEMA:-$(DEFAULT_SCHEMA)}"; \
	CASES="$${CASES:-$(DEFAULT_CASES)}"; \
	mkdir -p "$$DATA/.runs"; \
	{ \
	  echo "# Локальные настройки demo_qa (генерируется командой: make init)"; \
	  echo "# Можно редактировать руками. Рекомендуется добавить в .gitignore."; \
	  echo "DATA=$$DATA"; \
	  echo "SCHEMA=$$SCHEMA"; \
	  echo "CASES=$$CASES"; \
	  echo "# OUT можно не задавать: по умолчанию OUT=\$${DATA}/.runs/results.jsonl"; \
	  echo "# OUT=$$DATA/.runs/results.jsonl"; \
	} > "$(CONFIG)"; \
	echo "Ок: создан $(CONFIG)"; \
	echo "Создана папка: $$DATA/.runs"; \
	echo "Дальше: make chat / make batch / make help"

show-config:
	@echo "CONFIG  = $(CONFIG)"
	@echo "VENV    = $(VENV)"
	@echo "PYTHON  = $(PYTHON)"
	@echo "DATA    = $(DATA)"
	@echo "SCHEMA  = $(SCHEMA)"
	@echo "CASES   = $(CASES)"
	@echo "OUT     = $(OUT)"
	@echo "LLM_TOML= $(LLM_TOML)"
	@echo "TAG     = $(TAG)"
	@echo "NOTE    = $(NOTE)"
	@echo "CASE    = $(CASE)"
	@echo "LIMIT   = $(LIMIT)"

venv-check:
	@if [ -x "$(VENV)/bin/python" ]; then \
	  echo "OK: venv найден: $(VENV) (использую $(VENV)/bin/python)"; \
	else \
	  echo "INFO: venv не найден: $(VENV) (использую системный python: $$(command -v $(PYTHON) || echo 'python'))"; \
	fi

check:
	@test -n "$(strip $(DATA))"   || (echo "DATA не задан. Запусти: make init (или передай DATA=...)" && exit 1)
	@test -n "$(strip $(SCHEMA))" || (echo "SCHEMA не задан. Запусти: make init (или передай SCHEMA=...)" && exit 1)
	@test -n "$(strip $(CASES))"  || (echo "CASES не задан. Запусти: make init (или передай CASES=...)" && exit 1)

ensure-runs-dir: check
	@mkdir -p "$(DATA)/.runs"

# ==============================================================================
# LLM конфиг (без проверок доступности — это задача приложения)
# ==============================================================================
llm-init:
	@set -euo pipefail; \
	if [ -f "$(LLM_TOML)" ]; then \
	  echo "Файл уже существует: $(LLM_TOML)"; \
	  exit 0; \
	fi; \
	if [ -f "$(LLM_TOML_EXAMPLE)" ]; then \
	  cp "$(LLM_TOML_EXAMPLE)" "$(LLM_TOML)"; \
	  echo "Ок: создан $(LLM_TOML) из $(LLM_TOML_EXAMPLE)"; \
	else \
	  echo "Не найден пример: $(LLM_TOML_EXAMPLE). Создай $(LLM_TOML) вручную."; \
	  exit 1; \
	fi

llm-show:
	@echo "LLM config: $(LLM_TOML)"
	@echo "----------------------------------------"
	@sed -n '1,200p' "$(LLM_TOML)" 2>/dev/null || (echo "Файл не найден: $(LLM_TOML). Сделай: make llm-init" && exit 1)

llm-edit:
	@$(OPEN) -a "$(EDITOR_APP)" "$(LLM_TOML)"

# ==============================================================================
# Алиасы под команды CLI
# ==============================================================================
chat: check
	@$(CLI) chat --data "$(DATA)" --schema "$(SCHEMA)"

# 1) Полный прогон всего набора
batch: ensure-runs-dir
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)"

# 2) Полный прогон с тегом + заметка
batch-tag: ensure-runs-dir
	@test -n "$(strip $(TAG))" || (echo "TAG обязателен: make batch-tag TAG=..." && exit 1)
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" $(TAG_FLAG) $(NOTE_FLAG)

# 3) only-failed от latest
batch-failed: ensure-runs-dir
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" --only-failed

# 4) only-failed от явного baseline
batch-failed-from: ensure-runs-dir
	@test -n "$(strip $(ONLY_FAILED_FROM))" || (echo "Нужно задать ONLY_FAILED_FROM=.../results.jsonl" && exit 1)
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" \
	  --only-failed-from "$(ONLY_FAILED_FROM)"

# 5) only-missed (relative to effective по TAG или latest)
batch-missed: ensure-runs-dir
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" \
	  $(TAG_FLAG) --only-missed

# 6) only-missed от явного baseline
batch-missed-from: ensure-runs-dir
	@test -n "$(strip $(ONLY_MISSED_FROM))" || (echo "Нужно задать ONLY_MISSED_FROM=.../results.jsonl" && exit 1)
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" \
	  --only-missed --only-missed-from "$(ONLY_MISSED_FROM)"

# 7) fail-fast / max-fails
batch-fail-fast: ensure-runs-dir
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" --fail-fast

batch-max-fails: ensure-runs-dir
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" --max-fails "$(MAX_FAILS)"

batch-failed-tag: ensure-runs-dir
	@test -n "$(strip $(TAG))" || (echo "TAG обязателен: make batch-failed-tag TAG=..." && exit 1)
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" --tag "$(TAG)" --only-failed-effective

batch-missed-tag: ensure-runs-dir
	@test -n "$(strip $(TAG))" || (echo "TAG обязателен: make batch-missed-tag TAG=..." && exit 1)
	@$(CLI) batch --data "$(DATA)" --schema "$(SCHEMA)" --cases "$(CASES)" --out "$(OUT)" --tag "$(TAG)" --only-missed-effective

# stats (последние 10)
stats: check
	@$(CLI) stats --data "$(DATA)" --last 10

tags: check
	@$(CLI) tags list --data "$(DATA)" --format "$(TAGS_FORMAT)" --color "$(TAGS_COLOR)" $(if $(strip $(PATTERN)),--pattern "$(PATTERN)",) $(if $(strip $(LIMIT)),--limit $(LIMIT),)

# 8) История по кейсу (TAG опционален)
history-case: check
	@test -n "$(strip $(CASE))" || (echo "Нужно задать CASE=case_42" && exit 1)
	@$(CLI) history case "$(CASE)" --data "$(DATA)" $(TAG_FLAG) $(LIMIT_FLAG)

# 9) Сводка по тегу
report-tag: check
	@test -n "$(strip $(TAG))" || (echo "TAG обязателен: make report-tag TAG=..." && exit 1)
	@$(CLI) report tag --data "$(DATA)" --tag "$(TAG)"

report-tag-changes: check
	@test -n "$(strip $(TAG))" || (echo "TAG обязателен: make report-tag-changes TAG=... [CHANGES=10]" && exit 1)
	@$(CLI) report tag --data "$(DATA)" --tag "$(TAG)" --changes "$(CHANGES)"

# 10) Дебаг 1 кейса
case-run: check
	@test -n "$(strip $(CASE))" || (echo "Нужно задать CASE=case_42" && exit 1)
	@$(CLI) case run "$(CASE)" --cases "$(CASES)" --data "$(DATA)" --schema "$(SCHEMA)"

case-open: check
	@test -n "$(strip $(CASE))" || (echo "Нужно задать CASE=case_42" && exit 1)
	@$(CLI) case open "$(CASE)" --data "$(DATA)"

# compare (diff.md + junit)
compare: check
	@test -n "$(strip $(BASE))" || (echo "Нужно задать BASE=.../results_prev.jsonl" && exit 1)
	@test -n "$(strip $(NEW))"  || (echo "Нужно задать NEW=.../results.jsonl" && exit 1)
	@mkdir -p "$(DATA)/.runs"
	@$(CLI) compare \
	  --base "$(BASE)" \
	  --new  "$(NEW)" \
	  --out  "$(DIFF_OUT)" \
	  --junit "$(JUNIT)"

compare-tag: OUT := $(COMPARE_TAG_OUT)
compare-tag: JUNIT := $(COMPARE_TAG_JUNIT)
compare-tag: check
	@test -n "$(strip $(DATA))" || (echo "Нужно задать DATA=... (где лежит .runs)" && exit 1)
	@test -n "$(strip $(NEW_TAG))" || (echo "Нужно задать NEW_TAG=... (например NEW_TAG=baseline_v2)" && exit 1)
	@mkdir -p "$(DATA)/.runs"
	@$(CLI) compare \
	  --data "$(DATA)" \
	  --base-tag "$(BASE_TAG)" \
	  --new-tag "$(NEW_TAG)" \
	  --out  "$(OUT)" \
	  --junit "$(JUNIT)"

# команды очистки

tag-rm:
	@test -n "$(strip $(TAG))" || (echo "TAG обязателен: make tag-rm TAG=..." && exit 1)
	@TAG="$(TAG)" DATA="$(DATA)" PURGE_RUNS="$(PURGE_RUNS)" PRUNE_HISTORY="$(PRUNE_HISTORY)" PRUNE_CASE_HISTORY="$(PRUNE_CASE_HISTORY)" DRY="$(DRY)" $(PYTHON) -m scripts.tag_rm





