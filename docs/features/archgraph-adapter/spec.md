# Feature: LLM-assisted Schema-Driven Artifact Context Engine

## Problem Statement
Текущий `fetchgraph` выполняет детерминированный retrieval по жестко заданным связям (PLAN → FETCH → SYNTH), но неспособен отвечать на вопросы, требующие обоснованного выбора evidence, объяснения цепочек вывода или диагностики причин. Для задач explain/debug необходим механизм, который:
- читает артефакты прогона (run artifacts);
- строит локальный граф артефактов и связей между ними;
- по запросу собирает не просто closure, а **обоснованный пакет доказательств**;
- при необходимости делает **ограниченные шаги с участием LLM** для решения, какие фрагменты артефактов действительно нужны.

## Users / Actors
- **Разработчики, использующие fetchgraph как библиотеку** — вызывают новый слой через API.
- **Агенты (agent pipeline)** — используют новый слой как provider для получения evidence-пакетов.
- **CLI-пользователи** — запускают команды для сбора dossier/explain.

## Use Cases
- **EXPLAIN_TARGET** — объяснить, почему конкретный target (например, AS-017) присутствует в результатах.
- **EXPLAIN_DERIVATION** — объяснить, как из одних артефактов следуют другие (например, E2E гарантия из IR).
- **CASE_DOSSIER** — собрать полный пакет доказательств по конкретному кейсу (case_id).
- **VALIDATOR_DEFECT_PACK** — подготовить evidence-пакет для дефекта, найденного валидатором.

## Proposed Solution

### Affected Components
**Новые модули** (src/fetchgraph/):
- `artifact_context/` — модели запросов, политик, выходных пакетов, сессий.
- `artifact_graph/` — `ArtifactGraphProvider`, neighborhood expansion, traversal.
- `investigation/` — оркестратор, action space, оценка покрытия, сборка пакетов.
- `pipeline_schema/` — расширение схемы pipeline: QueryTemplate, RelationRule, ArtifactTypeSchema.
- `adapters/artifact_context_provider.py` — адаптер для использования в agent pipeline.
- `cli/artifact_context.py` — CLI-команды.

**Изменяемые компоненты**:
- `fetchgraph/` — добавляется точка входа `artifact_context_engine.fetch()`.
- `pipeline_schema/loader.py` — загрузка расширенной схемы.

### Data Models
**Новые сущности**:
- `ArtifactGraphProvider` (протокол) — методы: `get_artifact`, `get_fragment`, `list_neighbors`, `expand_neighborhood`, `resolve_provenance`.
- `PipelineSchema` — расширяется полями:
  - `query_templates: dict[str, QueryTemplate]`
  - `relation_rules: list[RelationRule]`
  - `artifact_type_schemas: dict[str, ArtifactTypeSchema]`
- `QueryTemplate` — описание семейства запросов: default_mode, seed_policy, output_kind и др.
- `RelationRule` — from_type, to_type, via_field, cardinality, supports_explain, supports_root_cause.
- `ArtifactTypeSchema` — stable_keys, id_fields, ref_fields, provenance_fields, evidence_fields.
- `RetrievalPolicy` — mode, max_llm_calls, max_steps, max_capsules, max_quotes, max_total_chars, max_hops_upstream/downstream, require_traceability и др.
- `ContextRequest` — pipeline_id, query_family, run_id, anchors, options, policy.
- `EvidencePack` — target, primary_evidence, supporting_evidence, rejected_candidates, quotes, boundary, investigation_trace.
- `ExplainPack` — explain_target, selected_evidence, reasoning_summary, provenance_chain, unresolved_gaps.
- `CaseDossier` — symptom_capsules, upstream_capsules, boundary, quotes, trace.
- `InvestigationSession` — session_id, request, steps, hypotheses, selected_refs, rejected_refs, open_questions.

### Interfaces
**Публичное API**:
```python
def fetch(request: ContextRequest) -> Union[EvidencePack, ExplainPack, CaseDossier]: ...
```

**Provider adapter** для использования в агентах:
```python
class ArtifactContextProvider(ContextProvider):
    name = "artifact_context"
    def fetch(self, request: dict) -> dict: ...
```

**CLI**:
```bash
fetchgraph artifact-context --run-id <id> --query-family EXPLAIN_DERIVATION --anchors '{"target_id": "..."}'
```

### Integration Notes
- Новый слой **аддитивен** — не требует переписывания существующего кода fetchgraph.
- Использует существующий `RunArtifactStore` для чтения артефактов.
- `ArtifactGraphProvider` реализуется поверх файлового хранилища (JSON/YAML) без graph DB.
- Для LLM-вызовов используется адаптер, соответствующий интерфейсу `LLMInvoke` (не входит в объем).
- Взаимодействие с agent pipeline: агент может вызывать новый provider напрямую и использовать возвращённый EvidencePack в синтезе ответа.
- Все операции ограничены рамками одного run (run-scoped).

## Правила и инварианты
1. **Лимиты вызовов LLM**:
   - В режиме `deterministic` — не более 1 LLM-вызова (опционально).
   - В режиме `assisted` — не более 3 LLM-вызовов.
   - В режиме `investigator` — не более 6 LLM-вызовов (макс. шагов = 5).
2. **Лимиты размера**:
   - Общее количество символов во всех фрагментах не превышает `max_total_chars` (по умолчанию 30 000).
   - Количество капсул (capsules) не более `max_capsules` (20).
   - Количество цитат (quotes) не более `max_quotes` (20).
3. **Инварианты целостности**:
   - Каждый возвращаемый evidence-пакет должен содержать `investigation_trace` с последовательностью действий.
   - Если `require_traceability=True`, каждый фрагмент должен быть привязан к конкретному артефакту и его provenance.
   - Retrieval boundary (что не удалось достать) и reasoning boundary (что не удалось доказать) всегда разделены и явно отражены в выходных данных.
4. **Запреты**:
   - Запрещены бесконечные циклы: после исчерпания `max_steps` оркестратор завершает работу и возвращает собранное.
   - Запрещён прямой доступ LLM к файловой системе; действия ограничены Action Space (LIST_ARTIFACTS, READ_FRAGMENT, EXPAND_NEIGHBORHOOD и т.д.).
   - Запрещено игнорирование схемы pipeline: все расширения окрестности должны соответствовать `RelationRule` из схемы.

## ADRs
- Принятые архитектурные решения описаны в `docs/adr/artifact-context-engine.md` (ссылка).

## Non-Goals / Out of Scope
- Хранение артефактов в graph DB.
- Универсальный семантический поиск по всем проектам.
- Бесконечный самоуправляемый агент (open-ended loop).
- Автоматическое исправление артефактов.
- Полноценная ноутбучная система (manus-like) в первой версии.
- Хранение каждого item как отдельного top-level объекта.
- Глобальная knowledge graph поверх всех runs.

## Open Questions
1. Какой именно формат policy будет наиболее гибким для пользователей? (сейчас предложен набор полей, нужно согласование)
2. Поддержка investigator mode в первой версии: делать минимальную реализацию (один шаг) или полноценную с сессией?
3. Детали формата `anchors` в запросе — требуется унифицированный способ передачи seed-объектов.
4. Нужно ли добавить поддержку нескольких схем pipeline (кроме archgraph_v1) сразу или только одну встроенную?