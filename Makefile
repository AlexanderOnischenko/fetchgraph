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
-include .demo_qa.mk

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
DATA   ?= _demo_data/shop
REPLAY_IDATA ?= $(DATA)
SCHEMA ?=
CASES  ?=
OUT    ?= $(DATA)/.runs/results.jsonl

# ==============================================================================
# 5) Параметры команд
# ==============================================================================
TAG   ?=
NOTE  ?=
CASE  ?=
NAME ?=
NEW_NAME ?=
PATTERN ?=
SPEC_IDX ?=
PROVIDER ?=
INPUT_HASH ?=
BUCKET ?= known_bad
REPLAY_ID ?=
EVENTS ?=
RUN_ID ?=
CASE_DIR ?=
TRACER_ROOT ?= tests/fixtures/replay_cases
TRACER_OUT_DIR ?= $(TRACER_ROOT)/$(BUCKET)
RUN_DIR ?=
ALLOW_BAD_JSON ?=
OVERWRITE ?= 0
SCOPE ?= both
WITH_RESOURCES ?= 1
ALL ?=
LIMIT ?= 50
CHANGES ?= 10
NEW_TAG ?=
TAGS_FORMAT ?= table
TAGS_COLOR ?= auto
SELECT ?= latest
SELECT_INDEX ?=
RUN_SELECT_INDEX ?=
REPLAY_SELECT_INDEX ?=
REQUIRE_UNIQUE ?= 0
NO_VALIDATE ?= 0
EXPECTED_FROM ?= replay

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
VALIDATE ?= 0
OVERWRITE_EXPECTED ?= 0
MOVE_TRACES ?= 0

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
.PHONY: help init show-config warn-config warn-missing-init warn-missing-tracer warn-missing-llm-config check check-data-dir ensure-runs-dir venv-check \
        llm-init llm-show llm-edit \
        chat \
        batch batch-tag batch-failed batch-failed-from \
        batch-missed batch-missed-from batch-failed-tag batch-missed-tag \
        batch-fail-fast batch-max-fails \
        stats history-case report-tag report-tag-changes tags tag-rm case-run case-open tracer-export tracer-matches tracer-ls tracer-replay-ids known-bad known-bad-one \
        fixture-green fixture-demote fixture-ls fixture-rm fixture-fix fixture-migrate \
        compare compare-tag

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
	@echo "Команды (DemoQA):"
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
	@echo ""
	@echo "Tracer → Export fixture (4 шага):"
	@echo "  Примечание: DATA обычно подтягивается из $(CONFIG) (make init)."
	@echo "  0) (опционально) история по кейсу:"
	@echo "     make history-case CASE=agg_003 [TAG=...] [LIMIT=50]"
	@echo "  1) Найти кандидатов прогонов по CASE:"
	@echo "     make tracer-ls CASE=agg_003 [DATA=...] [TAG=...] [RUN_ID=...] [CASE_DIR=...]"
	@echo "  2) Узнать доступные REPLAY_ID:"
	@echo "     make tracer-replay-ids CASE=agg_003 [DATA=...] [RUN_ID=...] [PROVIDER=...] [SPEC_IDX=...]"
	@echo "  3) Посмотреть матчи REPLAY_ID (подсказки для фильтрации):"
	@echo "     make tracer-matches CASE=agg_003 REPLAY_ID=plan_normalize.spec_v1 [SPEC_IDX=...] [PROVIDER=...] [INPUT_HASH=...]"
	@echo "     или explicit: make tracer-matches REPLAY_ID=plan_normalize.spec_v1 EVENTS=... [RUN_DIR=...] [CASE_DIR=...]"
	@echo "  4) Экспортировать:"
	@echo "     make tracer-export CASE=agg_003 REPLAY_ID=plan_normalize.spec_v1 [BUCKET=known_bad|fixed] [OVERWRITE=1] [ALLOW_BAD_JSON=1]"
	@echo "     или explicit: make tracer-export REPLAY_ID=plan_normalize.spec_v1 EVENTS=... RUN_DIR=... [CASE_DIR=...] [OVERWRITE=1]"
	@echo "  Advanced: RUN_SELECT_INDEX / REPLAY_SELECT_INDEX / SELECT / REQUIRE_UNIQUE / RUN_ID / CASE_DIR / TAG"
	@echo ""
	@echo "Фикстуры (fixture tools):"
	@echo "  fixtures layout: replay_cases/<bucket>/<name>.case.json, resources: replay_cases/<bucket>/resources/<fixture_stem>/<resource_id>/..."
	@echo "  make fixture-green CASE=agg_003|fixture_stem|path/to/case.case.json [TRACER_ROOT=...] [NO_VALIDATE=1] [EXPECTED_FROM=replay|observed] [OVERWRITE_EXPECTED=1] [DRY=1]"
	@echo "  make fixture-ls CASE=agg_003 [TRACER_ROOT=...] [BUCKET=known_bad]"
	@echo "  make fixture-rm CASE=agg_003|fixture_stem|path [SELECT=latest|first|last] [SELECT_INDEX=N] [REQUIRE_UNIQUE=1] [ALL=1]"
	@echo "    или: make fixture-rm [BUCKET=fixed|known_bad|all] [NAME=...] [PATTERN=...] [SCOPE=cases|resources|both] [DRY=1]"
	@echo "  make fixture-migrate CASE=agg_003|fixture_stem|path [SELECT=latest|first|last] [SELECT_INDEX=N] [REQUIRE_UNIQUE=1] [ALL=1]"
	@echo "    или: make fixture-migrate [BUCKET=fixed|known_bad|all] [DRY=1]"
	@echo "  make fixture-demote CASE=agg_003|fixture_stem|path [SELECT=latest|first|last] [SELECT_INDEX=N] [REQUIRE_UNIQUE=1] [ALL=1]"
	@echo "  make fixture-fix BUCKET=... NAME=... NEW_NAME=... [DRY=1]"
	@echo ""
	@echo "Тесты (pytest):"
	@echo "  make known-bad - запустить backlog-suite для known_bad (ожидаемо красный)"
	@echo "  make known-bad-one NAME=fixture_stem - запустить один known_bad кейс"
	@echo ""
	@echo "Диагностика / отчёты / сравнение:"
	@echo "  make report-tag TAG=...    - сводка по тегу (effective snapshot)"
	@echo "  make report-tag-changes TAG=... [CHANGES=10] - сводка + последние изменения effective snapshot"
	@echo "  make tags [PATTERN=*] DATA=... - показать список тегов"
	@echo "  make case-run  CASE=case_42 - прогнать один кейс"
	@echo "  make case-open CASE=case_42 - открыть артефакты кейса"
	@echo "  make compare BASE=<ref> NEW=<ref> [DIFF_OUT=...] [JUNIT=...]"
	@echo "    refs: /path/to/results.jsonl | /path/to/run_dir | <run_id> | latest | tag:<tag> | latest:<tag>"
	@echo "    examples:"
	@echo "      make compare BASE=32d32108 NEW=44148564"
	@echo "      make compare BASE=latest:baseline NEW=latest:qwen"
	@echo "      make compare BASE=tag:baseline NEW=tag:qwen"
	@echo "      make compare BASE=/tmp/a.results.jsonl NEW=/tmp/b.results.jsonl"
	@echo "      make compare BASE=path/to/run_dir NEW=path/to/other_run_dir"
	@echo "    tag:<tag> uses effective snapshot; latest:<tag> uses latest real run with run_meta.tag=<tag>"
	@echo "  make compare-tag BASE_TAG=baseline NEW_TAG=... [COMPARE_TAG_OUT=...] [COMPARE_TAG_JUNIT=...] (alias over compare refs)"
	@echo ""
	@echo "Уборка:"
	@echo "  make tag-rm TAG=... [DRY=1] [PURGE_RUNS=1] [PRUNE_HISTORY=1] [PRUNE_CASE_HISTORY=1]"
	@echo "    - удаляет effective snapshot тега и tag-latest* указатели"
	@echo "    DRY=1                - dry-run: только показать, что будет удалено"
	@echo "    PURGE_RUNS=1          - дополнительно удалить все runs, где run_meta.tag == TAG"
	@echo "    PRUNE_HISTORY=1       - вычистить записи с этим тегом из $${DATA}/.runs/history.jsonl"
	@echo "    PRUNE_CASE_HISTORY=1  - вычистить записи с этим тегом из $${DATA}/.runs/runs/cases/*.jsonl"
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
	@echo "EVENTS  = $(EVENTS)"
	@echo "REPLAY_ID = $(REPLAY_ID)"
	@echo "SPEC_IDX = $(SPEC_IDX)"
	@echo "PROVIDER = $(PROVIDER)"
	@echo "INPUT_HASH = $(INPUT_HASH)"
	@echo "RUN_DIR = $(RUN_DIR)"
	@echo "TRACER_ROOT = $(TRACER_ROOT)"
	@echo "TRACER_OUT_DIR = $(TRACER_OUT_DIR)"
	@echo "BUCKET = $(BUCKET)"
	@echo "ALLOW_BAD_JSON = $(ALLOW_BAD_JSON)"
	@echo "OVERWRITE = $(OVERWRITE)"
	@echo "RUN_SELECT_INDEX = $(RUN_SELECT_INDEX)"
	@echo "REPLAY_SELECT_INDEX = $(REPLAY_SELECT_INDEX)"
	@echo "NO_VALIDATE = $(NO_VALIDATE)"
	@echo "EXPECTED_FROM = $(EXPECTED_FROM)"
	@echo "REQUIRE_UNIQUE = $(REQUIRE_UNIQUE)"
	@echo "ALL = $(ALL)"
	@echo "NAME = $(NAME)"
	@echo "NEW_NAME = $(NEW_NAME)"
	@echo "PATTERN = $(PATTERN)"
	@echo "SCOPE = $(SCOPE)"
	@echo "DRY = $(DRY)"
	@echo "VALIDATE = $(VALIDATE)"
	@echo "OVERWRITE_EXPECTED = $(OVERWRITE_EXPECTED)"
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

warn-missing-init:
	@if [ ! -f "$(CONFIG)" ]; then \
	  echo "WARNING: local defaults not initialized (run: make init). Using Makefile defaults / environment vars."; \
	fi

warn-missing-tracer:
	@command -v fetchgraph-tracer >/dev/null 2>&1 || \
	  echo "WARNING: fetchgraph-tracer not installed. Run: pip install -e ."

warn-missing-llm-config:
	@if [ ! -f "$(LLM_TOML)" ]; then \
	  echo "WARNING: LLM config not found (LLM_TOML='$(LLM_TOML)'). Run: make llm-init"; \
	fi

warn-config: warn-missing-init
	@if [ -z "$(strip $(SCHEMA))" ]; then \
	  echo "WARNING: SCHEMA is not set (SCHEMA='$(SCHEMA)')"; \
	elif [ ! -f "$(SCHEMA)" ]; then \
	  echo "WARNING: SCHEMA path not found (SCHEMA='$(SCHEMA)')"; \
	fi
	@if [ -z "$(strip $(CASES))" ]; then \
	  echo "WARNING: CASES is not set (CASES='$(CASES)')"; \
	elif [ ! -f "$(CASES)" ]; then \
	  echo "WARNING: CASES path not found (CASES='$(CASES)')"; \
	fi

check-data-dir: warn-missing-init
	@test -n "$(strip $(DATA))" || (echo "DATA не задан. Запусти: make init (или передай DATA=...)" && exit 1)
	@test -d "$(DATA)" || (echo "DATA не найдена как директория: $(DATA)" && exit 1)
	@test -d "$(DATA)/.runs" || (echo "DATA/.runs не найдена. Запусти: make init (или создай $(DATA)/.runs)" && exit 1)

check: warn-config
	@test -n "$(strip $(DATA))"   || (echo "DATA не задан. Запусти: make init (или передай DATA=...)" && exit 1)
	@test -n "$(strip $(SCHEMA))" || (echo "SCHEMA не задан. Запусти: make init (или передай SCHEMA=...)" && exit 1)
	@test -n "$(strip $(CASES))"  || (echo "CASES не задан. Запусти: make init (или передай CASES=...)" && exit 1)
	@test -f "$(SCHEMA)" || (echo "SCHEMA не найден: $(SCHEMA)" && exit 1)
	@test -f "$(CASES)"  || (echo "CASES не найден: $(CASES)" && exit 1)

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
chat: warn-missing-llm-config check
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

tracer-export: warn-config warn-missing-tracer
	@test -n "$(strip $(REPLAY_ID))" || (echo "REPLAY_ID обязателен: make tracer-export REPLAY_ID=plan_normalize.spec_v1" && exit 2)
	@if [ -z "$(strip $(EVENTS))" ]; then $(MAKE) --no-print-directory check-data-dir; fi
	@if [ -z "$(strip $(EVENTS))" ]; then \
	  test -n "$(strip $(CASE))" || (echo "CASE обязателен: make tracer-export CASE=agg_003" && exit 2); \
	fi
	@case "$(BUCKET)" in fixed|known_bad) ;; *) echo "BUCKET должен быть fixed или known_bad для tracer-export" && exit 2 ;; esac
	@set -euo pipefail; \
	if [ -n "$(strip $(EVENTS))" ]; then \
	  fetchgraph-tracer export-case-bundle \
	    --id "$(REPLAY_ID)" \
	    --out "$(TRACER_OUT_DIR)" \
	    --events "$(EVENTS)" \
	    $(if $(CASE_DIR),--case-dir "$(CASE_DIR)",) \
	    $(if $(RUN_DIR),--run-dir "$(RUN_DIR)",) \
	    $(if $(strip $(INPUT_HASH)),--input-hash "$(INPUT_HASH)",) \
	    $(if $(strip $(SPEC_IDX)),--spec-idx "$(SPEC_IDX)",) \
	    $(if $(strip $(PROVIDER)),--provider "$(PROVIDER)",) \
	    $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	    $(if $(strip $(RUN_SELECT_INDEX)),--select-index "$(RUN_SELECT_INDEX)",) \
	    $(if $(strip $(REPLAY_SELECT_INDEX)),--replay-select-index "$(REPLAY_SELECT_INDEX)",) \
	    $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	    $(if $(filter 1 true yes on,$(OVERWRITE)),--overwrite,) \
	    $(if $(filter 1 true yes on,$(ALLOW_BAD_JSON)),--allow-bad-json,); \
	else \
	  fetchgraph-tracer export-case-bundle \
	    --id "$(REPLAY_ID)" \
	    --out "$(TRACER_OUT_DIR)" \
	    --case "$(CASE)" \
	    --data "$(REPLAY_IDATA)" \
	    --pick-run latest_with_replay \
	    $(if $(strip $(INPUT_HASH)),--input-hash "$(INPUT_HASH)",) \
	    $(if $(strip $(SPEC_IDX)),--spec-idx "$(SPEC_IDX)",) \
	    $(if $(strip $(PROVIDER)),--provider "$(PROVIDER)",) \
	    $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	    $(if $(strip $(RUN_SELECT_INDEX)),--select-index "$(RUN_SELECT_INDEX)",) \
	    $(if $(strip $(REPLAY_SELECT_INDEX)),--replay-select-index "$(REPLAY_SELECT_INDEX)",) \
	    $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	    $(if $(RUN_ID),--run-id "$(RUN_ID)",) \
	    $(if $(CASE_DIR),--case-dir "$(CASE_DIR)",) \
	    $(if $(RUN_DIR),--run-dir "$(RUN_DIR)",) \
	    $(if $(TAG),--tag "$(TAG)",) \
	    $(if $(filter 1 true yes on,$(OVERWRITE)),--overwrite,) \
	    $(if $(filter 1 true yes on,$(ALLOW_BAD_JSON)),--allow-bad-json,); \
	fi

tracer-matches: warn-missing-tracer
	@if [ -z "$(strip $(EVENTS))" ]; then $(MAKE) --no-print-directory check-data-dir; fi
	@if [ -z "$(strip $(EVENTS))" ]; then \
	  test -n "$(strip $(CASE))" || (echo "CASE обязателен: make tracer-matches CASE=agg_003" && exit 2); \
	fi
	@test -n "$(strip $(REPLAY_ID))" || (echo "REPLAY_ID обязателен" && echo "Сначала выполните: make tracer-replay-ids CASE=agg_003" && exit 2)
	@set -euo pipefail; \
	if [ -n "$(strip $(EVENTS))" ]; then \
	  output="$$(fetchgraph-tracer export-case-bundle \
	    --id "$(REPLAY_ID)" \
	    --events "$(EVENTS)" \
	    $(if $(CASE_DIR),--case-dir "$(CASE_DIR)",) \
	    $(if $(RUN_DIR),--run-dir "$(RUN_DIR)",) \
	    --pick-run latest_with_replay \
	    $(if $(strip $(INPUT_HASH)),--input-hash "$(INPUT_HASH)",) \
	    $(if $(strip $(SPEC_IDX)),--spec-idx "$(SPEC_IDX)",) \
	    $(if $(strip $(PROVIDER)),--provider "$(PROVIDER)",) \
	    $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	    $(if $(strip $(REPLAY_SELECT_INDEX)),--replay-select-index "$(REPLAY_SELECT_INDEX)",) \
	    $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	    --list-replay-matches)"; \
	else \
	  output="$$(fetchgraph-tracer export-case-bundle \
	    --case "$(CASE)" \
	    --id "$(REPLAY_ID)" \
	    --data "$(REPLAY_IDATA)" \
	    --pick-run latest_with_replay \
	    $(if $(strip $(INPUT_HASH)),--input-hash "$(INPUT_HASH)",) \
	    $(if $(strip $(SPEC_IDX)),--spec-idx "$(SPEC_IDX)",) \
	    $(if $(strip $(PROVIDER)),--provider "$(PROVIDER)",) \
	    $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	    $(if $(strip $(REPLAY_SELECT_INDEX)),--replay-select-index "$(REPLAY_SELECT_INDEX)",) \
	    $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	    $(if $(RUN_ID),--run-id "$(RUN_ID)",) \
	    $(if $(CASE_DIR),--case-dir "$(CASE_DIR)",) \
	    $(if $(RUN_DIR),--run-dir "$(RUN_DIR)",) \
	    $(if $(strip $(TAG)),--tag "$(TAG)",) \
	    --list-replay-matches)"; \
	fi; \
	echo "$$output"; \
	match_lines="$$(printf "%s\n" "$$output" | grep -E '^[0-9]+\t' || true)"; \
	if [ -n "$$match_lines" ]; then \
	  last_line="$$(printf "%s\n" "$$match_lines" | tail -n1)"; \
	  idx="$$(printf "%s" "$$last_line" | awk -F '\t' '{print $$1}')"; \
	  hash8="$$(printf "%s" "$$last_line" | awk -F '\t' '{print $$7}')"; \
	  echo "Suggested: REPLAY_SELECT_INDEX=$$idx"; \
	  if [ -n "$$hash8" ]; then \
	    echo "Suggested: INPUT_HASH=$$hash8"; \
	  fi; \
	fi

tracer-ls: warn-missing-tracer
	$(if $(strip $(EVENTS)),,$(MAKE) --no-print-directory check-data-dir)
	@test -n "$(strip $(CASE))" || (echo "CASE обязателен: make tracer-ls CASE=agg_003" && exit 2)
	@fetchgraph-tracer export-case-bundle \
	  --case "$(CASE)" \
	  --data "$(REPLAY_IDATA)" \
	  $(if $(strip $(REPLAY_ID)),--id "$(REPLAY_ID)" --pick-run latest_with_replay,--pick-run latest_non_missed) \
	  $(if $(RUN_ID),--run-id "$(RUN_ID)",) \
	  $(if $(CASE_DIR),--case-dir "$(CASE_DIR)",) \
	  $(if $(RUN_DIR),--run-dir "$(RUN_DIR)",) \
	  $(if $(strip $(TAG)),--tag "$(TAG)",) \
	  --list-matches

tracer-replay-ids: warn-missing-tracer
	@if [ -z "$(strip $(EVENTS))" ]; then $(MAKE) --no-print-directory check-data-dir; fi
	@if [ -z "$(strip $(EVENTS))" ]; then \
	  test -n "$(strip $(CASE))" || (echo "CASE обязателен: make tracer-replay-ids CASE=agg_003" && exit 2); \
	fi
	@set -euo pipefail; \
	if [ -n "$(strip $(EVENTS))" ]; then \
	  output="$$(fetchgraph-tracer export-case-bundle \
	    --events "$(EVENTS)" \
	    $(if $(RUN_DIR),--run-dir "$(RUN_DIR)",) \
	    --pick-run latest_non_missed \
	    $(if $(strip $(SPEC_IDX)),--spec-idx "$(SPEC_IDX)",) \
	    $(if $(strip $(PROVIDER)),--provider "$(PROVIDER)",) \
	    --list-replay-ids)"; \
	else \
	  output="$$(fetchgraph-tracer export-case-bundle \
	    --case "$(CASE)" \
	    --data "$(REPLAY_IDATA)" \
	    --pick-run latest_non_missed \
	    $(if $(strip $(SPEC_IDX)),--spec-idx "$(SPEC_IDX)",) \
	    $(if $(strip $(PROVIDER)),--provider "$(PROVIDER)",) \
	    $(if $(RUN_ID),--run-id "$(RUN_ID)",) \
	    $(if $(CASE_DIR),--case-dir "$(CASE_DIR)",) \
	    $(if $(RUN_DIR),--run-dir "$(RUN_DIR)",) \
	    $(if $(strip $(TAG)),--tag "$(TAG)",) \
	    --list-replay-ids)"; \
	fi; \
	echo "$$output"; \
	ids="$$(printf "%s\n" "$$output" | awk '{print $$1}' | sed '/^$$/d')"; \
	count="$$(printf "%s\n" "$$ids" | wc -l | tr -d ' ')"; \
	first="$$(printf "%s\n" "$$ids" | head -n1)"; \
	if [ "$$count" -eq 1 ]; then \
	  echo "Suggested: REPLAY_ID=$$first"; \
	else \
	  printf "REPLAY_ID=%s\n" $$ids; \
	fi

known-bad:
	@pytest -m known_bad -vv

known-bad-one:
	@test -n "$(strip $(NAME))" || (echo "NAME обязателен: make known-bad-one NAME=fixture_stem" && exit 2)
	@pytest -m known_bad -k "$(NAME)" -vv

fixture-green: warn-config
	@test -n "$(strip $(CASE))" || (echo "CASE обязателен: make fixture-green CASE=agg_003 или CASE=path/to/fixture.case.json" && exit 1)
	@case_value="$(CASE)"; \
	if [ -f "$$case_value" ]; then \
	  case_args="--case $$case_value"; \
	elif [[ "$$case_value" == *".case.json" || "$$case_value" == *"/"* ]]; then \
	  case_args="--case $(TRACER_ROOT)/known_bad/$$case_value"; \
	elif [[ "$$case_value" == *"__"* ]]; then \
	  case_args="--name $$case_value"; \
	else \
	  case_args="--case-id $$case_value"; \
	fi; \
	$(PYTHON) -m fetchgraph.tracer.cli fixture-green $$case_args --root "$(TRACER_ROOT)" \
	  $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	  $(if $(strip $(SELECT_INDEX)),--select-index "$(SELECT_INDEX)",) \
	  $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	  $(if $(filter 1 true yes on,$(NO_VALIDATE)),--no-validate,) \
	  $(if $(strip $(EXPECTED_FROM)),--expected-from "$(EXPECTED_FROM)",) \
	  $(if $(filter 1 true yes on,$(OVERWRITE_EXPECTED)),--overwrite-expected,) \
	  $(if $(filter 1 true yes on,$(DRY)),--dry-run,)

fixture-ls: warn-config
	@test -n "$(strip $(CASE))" || (echo "CASE обязателен: make fixture-ls CASE=agg_003" && exit 1)
	@$(PYTHON) -m fetchgraph.tracer.cli fixture-ls --root "$(TRACER_ROOT)" --bucket "$(BUCKET)" --case-id "$(CASE)"

fixture-rm: warn-config
	@case "$(BUCKET)" in fixed|known_bad|all) ;; *) echo "BUCKET должен быть fixed, known_bad или all для fixture-rm" && exit 1 ;; esac
	@case_value="$(CASE)"; \
	if [ -n "$$case_value" ]; then \
	  if [ -f "$$case_value" ]; then \
	    case_args="--case $$case_value"; \
	  elif [[ "$$case_value" == *".case.json" || "$$case_value" == *"/"* ]]; then \
	    if [ "$(BUCKET)" = "all" ]; then \
	      echo "BUCKET=all не поддерживает относительный путь CASE. Используйте BUCKET=fixed|known_bad или задайте CASE=case_id/NAME/PATTERN."; \
	      exit 1; \
	    fi; \
	    case_args="--case $(TRACER_ROOT)/$(BUCKET)/$$case_value"; \
	  elif [[ "$$case_value" == *"__"* ]]; then \
	    case_args="--name $$case_value"; \
	  else \
	    case_args="--case-id $$case_value"; \
	  fi; \
	fi; \
	$(PYTHON) -m fetchgraph.tracer.cli fixture-rm $$case_args --root "$(TRACER_ROOT)" --bucket "$(BUCKET)" \
	  $(if $(strip $(NAME)),--name "$(NAME)",) \
	  $(if $(strip $(PATTERN)),--pattern "$(PATTERN)",) \
	  $(if $(strip $(SCOPE)),--scope "$(SCOPE)",) \
	  $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	  $(if $(strip $(SELECT_INDEX)),--select-index "$(SELECT_INDEX)",) \
	  $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	  $(if $(filter 1 true yes on,$(ALL)),--all,) \
	  $(if $(filter 1 true yes on,$(DRY)),--dry-run,)

fixture-fix: warn-config
	@test -n "$(strip $(NAME))" || (echo "NAME обязателен: make fixture-fix NAME=old_stem NEW_NAME=new_stem" && exit 1)
	@test -n "$(strip $(NEW_NAME))" || (echo "NEW_NAME обязателен: make fixture-fix NAME=old_stem NEW_NAME=new_stem" && exit 1)
	@$(PYTHON) -m fetchgraph.tracer.cli fixture-fix --root "$(TRACER_ROOT)" --bucket "$(BUCKET)" \
	  --name "$(NAME)" --new-name "$(NEW_NAME)" \
	  $(if $(filter 1 true yes on,$(DRY)),--dry-run,)

fixture-migrate: warn-config
	@case_value="$(CASE)"; \
	if [ -n "$$case_value" ]; then \
	  if [ -f "$$case_value" ]; then \
	    case_args="--case $$case_value"; \
	  elif [[ "$$case_value" == *".case.json" || "$$case_value" == *"/"* ]]; then \
	    case_args="--case $(TRACER_ROOT)/$(BUCKET)/$$case_value"; \
	  else \
	    case_args="--case-id $$case_value"; \
	  fi; \
	fi; \
	$(PYTHON) -m fetchgraph.tracer.cli fixture-migrate $$case_args --root "$(TRACER_ROOT)" --bucket "$(BUCKET)" \
	  $(if $(strip $(NAME)),--name "$(NAME)",) \
	  $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	  $(if $(strip $(SELECT_INDEX)),--select-index "$(SELECT_INDEX)",) \
	  $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	  $(if $(filter 1 true yes on,$(ALL)),--all,) \
	  $(if $(filter 1 true yes on,$(DRY)),--dry-run,)

fixture-demote: warn-config
	@test -n "$(strip $(CASE))" || (echo "CASE обязателен: make fixture-demote CASE=agg_003" && exit 1)
	@case_value="$(CASE)"; \
	if [ -f "$$case_value" ]; then \
	  case_args="--case $$case_value"; \
	elif [[ "$$case_value" == *".case.json" || "$$case_value" == *"/"* ]]; then \
	  case_args="--case $(TRACER_ROOT)/fixed/$$case_value"; \
	else \
	  case_args="--case-id $$case_value"; \
	fi; \
	$(PYTHON) -m fetchgraph.tracer.cli fixture-demote $$case_args --root "$(TRACER_ROOT)" \
	  $(if $(strip $(SELECT)),--select "$(SELECT)",) \
	  $(if $(strip $(SELECT_INDEX)),--select-index "$(SELECT_INDEX)",) \
	  $(if $(filter 1 true yes on,$(REQUIRE_UNIQUE)),--require-unique,) \
	  $(if $(filter 1 true yes on,$(ALL)),--all,) \
	  $(if $(filter 1 true yes on,$(OVERWRITE)),--overwrite,) \
	  $(if $(filter 1 true yes on,$(DRY)),--dry-run,)



# compare (diff.md + junit)
compare: check
	@test -n "$(strip $(BASE))" || (echo "Нужно задать BASE=<ref>" && exit 1)
	@test -n "$(strip $(NEW))"  || (echo "Нужно задать NEW=<ref>" && exit 1)
	@mkdir -p "$(DATA)/.runs"
	@$(CLI) compare 	  --data "$(DATA)" 	  --base "$(BASE)" 	  --new  "$(NEW)" 	  --out  "$(DIFF_OUT)" 	  --junit "$(JUNIT)"

compare-tag: OUT := $(COMPARE_TAG_OUT)
compare-tag: JUNIT := $(COMPARE_TAG_JUNIT)
compare-tag: check
	@test -n "$(strip $(DATA))" || (echo "Нужно задать DATA=... (где лежит .runs)" && exit 1)
	@test -n "$(strip $(NEW_TAG))" || (echo "Нужно задать NEW_TAG=... (например NEW_TAG=baseline_v2)" && exit 1)
	@$(MAKE) --no-print-directory compare DATA="$(DATA)" BASE="tag:$(BASE_TAG)" NEW="tag:$(NEW_TAG)" DIFF_OUT="$(OUT)" JUNIT="$(JUNIT)"

# команды очистки

tag-rm:
	@test -n "$(strip $(TAG))" || (echo "TAG обязателен: make tag-rm TAG=..." && exit 1)
	@TAG="$(TAG)" DATA="$(DATA)" PURGE_RUNS="$(PURGE_RUNS)" PRUNE_HISTORY="$(PRUNE_HISTORY)" PRUNE_CASE_HISTORY="$(PRUNE_CASE_HISTORY)" DRY="$(DRY)" $(PYTHON) -m scripts.tag_rm
