# Comprehensive Refactoring Plan for the Data Engineering and Data Science Skill Library

## Status

- **This document is a pre-implementation plan only.**
- No skill content is being rewritten yet.
- The purpose of this plan is to establish a clear, evidence-based target architecture before any migration work starts.

---

## 1. What was studied

This plan is based on a direct audit of the current repository and the Claude agent skill authoring guidance.

### Repository audit inputs

- `README.md`
- all current `skills/*/SKILL.md` files
- representative detailed references and scripts across data-engineering, data-science, and `flowerpower`
- `tools/skill_lint.py`
- current file tree, line counts, duplicate-content analysis, and broken-reference analysis

### External authoring guidance studied

- Claude Agent Skills best practices:  
  `https://platform.claude.com/docs/en/agents-and-tools/agent-skills/best-practices`

Key best-practice themes used in this plan:

1. **Be concise in SKILL.md** and push detail into on-demand references.
2. **Use progressive disclosure** with direct, explicit references.
3. **Avoid deep or confusing reference chains.**
4. **Prefer workflows for complex tasks.**
5. **Add scripts for fragile or deterministic operations.**
6. **Build evaluations first / early, not after the fact.**
7. **Use naming and descriptions that improve skill discovery.**

---

## 2. Verified current-state findings

### 2.1 Repository inventory

Current audited totals:

- **29 skills**
- **183 markdown files** under `skills/`
- **~19,873 total lines** across markdown and Python in `skills/`

### 2.2 Duplication

There is major duplication in the current data-science skill set.

Verified duplicate-content stats:

- **21 duplicate markdown groups**
- **105 redundant file copies** beyond the first copy
- **~4,060 duplicate lines** of repeated reference content

The duplicated files are copied across all six current data-science skills, including:

- `altair-grammar.md`
- `automated-profiling.md`
- `bokeh-server.md`
- `categorical-encoding.md`
- `cross-validation.md`
- `datetime-features.md`
- `experiment-tracking.md`
- `feature-selection.md`
- `holoviz-datashader.md`
- `hyperparameter-tuning.md`
- `large-dataset-eda.md`
- `matplotlib-advanced.md`
- `metrics-guide.md`
- `notebook-testing.md`
- `plotly-dash.md`
- `seaborn-statistical.md`
- `sharing-publishing.md`
- `statistical-tests.md`
- `streamlit-advanced.md`
- `text-features.md`
- `visualization-patterns.md`

### 2.3 Broken and inconsistent references

Verified local-reference problems:

- **22 broken local markdown references**
- concentrated in:
  - `skills/data-science-eda/SKILL.md`
  - `skills/data-science-feature-engineering/SKILL.md`
  - `skills/data-science-model-evaluation/SKILL.md`
  - `skills/data-science-notebooks/SKILL.md`
  - `skills/data-science-interactive-apps/SKILL.md`
  - plus two stale references in `README.md`

Examples of current issues:

- data-science skills refer to `../references/...` even though the actual files live in `./references/...`
- some referenced files do not exist at all (`jupyter-advanced.md`, `marimo-guide.md`, `panel-holoviz.md`, `gradio-ml.md`, `app-testing.md`, `production-deployment.md`)
- several engineering skills use **hybrid reference syntax** such as `@skill-name/path.md`, mixing skill routing and file routing in a way that is hard to lint, hard to reason about, and not aligned with the best-practices guidance

### 2.4 Lint and structural warnings

Running `python3 tools/skill_lint.py` currently produces:

- **0 errors**
- **49 warnings**

The biggest buckets are:

1. `dependsOn` is treated as a non-standard frontmatter field everywhere it appears.
2. broken local references in data-science skills.
3. stale references in `README.md`.

### 2.5 Long files without navigational structure

A large number of files over 100 lines do **not** include a table of contents, even though the best-practices guidance recommends a TOC for longer reference files.

This affects many major references and several SKILL files, including long files in:

- `data-engineering-core`
- `data-engineering-best-practices`
- `data-engineering-storage-authentication`
- `data-engineering-storage-formats`
- `data-engineering-storage-lakehouse`
- `data-engineering-orchestration`
- `data-engineering-observability`
- `data-engineering-quality`
- `data-engineering-ai-ml`
- `data-engineering-storage-remote-access`
- `flowerpower`
- most data-science SKILL files

### 2.6 Overlap and trigger-noise issues

Current overlap hotspots:

- `data-engineering` hub skill is too broad and competes with all engineering skills.
- `data-engineering-core` overlaps with `data-engineering-best-practices`.
- `data-engineering-storage-lakehouse` overlaps with Delta/Iceberg remote-access integration skills.
- `data-engineering-storage-remote-access` is fragmented into too many child skills for one domain.
- `data-engineering-orchestration` overlaps with `flowerpower`.
- `data-engineering-quality` and `data-engineering-observability` are logically adjacent but operationally split.
- `data-science-eda` and `data-science-visualization` overlap heavily.
- the data-science references are duplicated far beyond what is useful.

### 2.7 Missing evaluation infrastructure

Verified absence:

- no `evals/`
- no `evals.json`
- no benchmark artifacts
- no trigger evaluation set
- no regression harness for skill quality or skill routing

This is a major gap relative to Claude’s recommended evaluation-first workflow.

---

## 3. Refactoring goals

The refactor should optimize for the following outcomes.

### 3.1 Primary goals

1. **Reduce the number of top-level skills substantially** without making any one skill vague or overloaded.
2. **Eliminate duplicated reference content.**
3. **Make every remaining skill self-contained and coherent.**
4. **Use clear, action-oriented names** and highly triggerable descriptions.
5. **Convert ultra-broad hub behavior into docs/indexes, not triggerable skills.**
6. **Replace tiny or shallow reference stubs** with either:
   - genuinely comprehensive references, or
   - broader topic references with authoritative links.
7. **Introduce evaluation and validation infrastructure** before and during implementation.
8. **Standardize reference and script patterns** across the whole repo.

### 3.2 Secondary goals

1. Improve maintainability and reviewability.
2. Make skill boundaries obvious to both humans and models.
3. Reduce naming sprawl like `data-engineering-storage-remote-access-integrations-polars`.
4. Remove ambiguous cross-skill/file hybrid references.
5. Establish a migration path for existing users of the current skill names.

---

## 4. Decision rubric: dedicated skill vs reference doc vs script

This refactor should make every framework/tool decision explicitly, using the same rubric everywhere.

### 4.1 Create a **dedicated skill** only when all or most of these are true

A tool/framework deserves its own skill if it:

1. has a **distinct user intent** that should trigger independently,
2. has a **standalone workflow** rather than being a subtopic,
3. requires **substantial decision-making** or orchestration,
4. benefits from dedicated **scripts/templates/validators**, and/or
5. is strategically important enough that users will ask for it directly by name.

### 4.2 Use a **comprehensive reference document** when

A tool/framework should live inside another skill as a detailed reference if it:

1. is usually selected within a broader workflow,
2. does not need its own trigger boundary,
3. is one of several alternatives in a comparison set,
4. shares most of its context with a parent domain,
5. would create trigger noise if promoted to a top-level skill.

### 4.3 Add a **script** when the work is deterministic or fragile

A script is preferred when the work involves:

1. validation,
2. scaffolding,
3. filesystem or credentials checks,
4. repeated transformations,
5. objective pass/fail behavior,
6. error-prone configuration steps.

### 4.4 Anti-rules

Do **not** create a separate top-level skill just because:

- a framework is popular,
- a framework has its own brand name,
- there is already a folder for it today,
- a 30–50 line stub exists.

Do **not** keep tiny reference files unless they are upgraded into either:

- a deep practical reference with structure and examples, or
- a routing/reference page with strong outbound links and clear “when to read this” guidance.

---

## 5. Proposed target architecture

### 5.1 Target skill count

Target the library toward **14 top-level skills** instead of 29.

This is a large simplification while still preserving clear trigger boundaries.

### 5.2 Proposed future skill set

| Proposed skill | Purpose | Current skills folded into it |
|---|---|---|
| `building-data-pipelines` | Core batch ETL/dataframe/SQL patterns + production architecture rules | `data-engineering-core`, `data-engineering-best-practices` |
| `accessing-cloud-storage` | Auth + remote object storage access + library/tool integrations | `data-engineering-storage-authentication`, `data-engineering-storage-remote-access`, all remote-access library/integration skills |
| `designing-data-storage` | File formats + lakehouse table formats + storage design tradeoffs | `data-engineering-storage-formats`, `data-engineering-storage-lakehouse`, Delta/Iceberg integration details currently split elsewhere |
| `managing-data-catalogs` | Catalog architecture, metadata systems, and multi-source access patterns | `data-engineering-catalogs` |
| `orchestrating-data-pipelines` | Prefect, Dagster, dbt, scheduling, retries, deployment patterns | `data-engineering-orchestration` |
| `assuring-data-pipelines` | Data quality + observability + operational validation loops | `data-engineering-quality`, `data-engineering-observability` |
| `building-streaming-pipelines` | Kafka, MQTT, NATS JetStream, streaming architecture | `data-engineering-streaming` |
| `engineering-ai-pipelines` | Embeddings, vector stores, RAG, LLM monitoring, batch inference | `data-engineering-ai-ml` |
| `using-flowerpower` | Dedicated FlowerPower/Hamilton skill because it has a specific framework workflow and executable scripts | `flowerpower` |
| `analyzing-data` | EDA + statistical exploration + visualization selection and patterns | `data-science-eda`, `data-science-visualization` |
| `engineering-ml-features` | Feature engineering, representation choices, leakage-safe preprocessing | `data-science-feature-engineering` |
| `evaluating-ml-models` | Cross-validation, metrics, model comparison, tuning, experiment tracking | `data-science-model-evaluation` |
| `working-in-notebooks` | Jupyter/marimo/reproducible notebook workflows | `data-science-notebooks` |
| `building-data-apps` | Streamlit/Panel/Gradio/Dash/NiceGUI app-building workflows | `data-science-interactive-apps` |

### 5.3 What disappears as a skill

| Current item | Future state |
|---|---|
| `data-engineering` | convert to non-triggerable documentation/index page only |
| remote-access subskills | folded into `accessing-cloud-storage` references |
| lakehouse integration subskills | folded into `designing-data-storage` references |
| duplicated data-science references | deleted and reauthored in the new skill homes |

### 5.4 Why this target architecture is better

1. It removes the broad hub skill from the trigger path.
2. It groups topics by **workflow**, not by arbitrary taxonomy depth.
3. It keeps skills self-contained for packaging and selective installation.
4. It reduces top-level skill count while preserving distinct user intents.
5. It converts tool fragmentation into well-structured reference sets.

---

## 6. Current → future migration map

### 6.1 Data engineering

| Current | Future |
|---|---|
| `data-engineering` | `docs/skill-map.md` (no longer a skill) |
| `data-engineering-core` | `building-data-pipelines` |
| `data-engineering-best-practices` | `building-data-pipelines` |
| `data-engineering-storage-authentication` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-libraries-fsspec` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-libraries-pyarrow-fs` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-libraries-obstore` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-polars` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-duckdb` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-pandas` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-pyarrow` | `accessing-cloud-storage` |
| `data-engineering-storage-remote-access-integrations-delta-lake` | `designing-data-storage` |
| `data-engineering-storage-remote-access-integrations-iceberg` | `designing-data-storage` |
| `data-engineering-storage-formats` | `designing-data-storage` |
| `data-engineering-storage-lakehouse` | `designing-data-storage` |
| `data-engineering-catalogs` | `managing-data-catalogs` |
| `data-engineering-orchestration` | `orchestrating-data-pipelines` |
| `data-engineering-quality` | `assuring-data-pipelines` |
| `data-engineering-observability` | `assuring-data-pipelines` |
| `data-engineering-streaming` | `building-streaming-pipelines` |
| `data-engineering-ai-ml` | `engineering-ai-pipelines` |
| `flowerpower` | `using-flowerpower` |

### 6.2 Data science

| Current | Future |
|---|---|
| `data-science-eda` | `analyzing-data` |
| `data-science-visualization` | `analyzing-data` |
| `data-science-feature-engineering` | `engineering-ml-features` |
| `data-science-model-evaluation` | `evaluating-ml-models` |
| `data-science-notebooks` | `working-in-notebooks` |
| `data-science-interactive-apps` | `building-data-apps` |

---

## 7. Framework/tool disposition matrix

This is the decision table for the major frameworks and tools currently represented in the repo.

### 7.1 Core data engineering tools

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| Polars | `building-data-pipelines` | No | deep reference | Core transformation engine, usually chosen inside pipeline work |
| DuckDB | `building-data-pipelines` | No | deep reference | Core embedded SQL/OLAP tool, central to pipeline construction |
| PyArrow | `building-data-pipelines` | No | deep reference | Foundational interchange/runtime layer, not a standalone user intent |
| PostgreSQL | `building-data-pipelines` | No | focused reference | Source/target system within ETL workflows, not a separate skill boundary |

### 7.2 Storage and lakehouse tools

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| Parquet | `designing-data-storage` | No | deep reference | Core storage-format decision |
| Arrow/Feather/IPC | `designing-data-storage` | No | combined deep reference | Storage/interchange design topic |
| Avro | `designing-data-storage` | No | section or focused reference | Important but usually part of format-selection workflows |
| ORC | `designing-data-storage` | No | section or focused reference | Similar to Avro; important but not a standalone skill |
| Zarr | `designing-data-storage` | No | focused reference | Specialized format within storage design decisions |
| Delta Lake | `designing-data-storage` | No | deep reference | Strongly important, but should remain under the broader storage-design workflow |
| Apache Iceberg | `designing-data-storage` | No | deep reference | Same rationale as Delta Lake |
| Apache Hudi | `designing-data-storage` | No | focused reference | Important but narrower than Delta/Iceberg |
| Lance / LanceDB | `engineering-ai-pipelines` with cross-link from storage | No | deep reference | More natural in vector/AI data workflows than generic storage |

### 7.3 Cloud access and authentication

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| AWS IAM / STS / Secrets Manager | `accessing-cloud-storage` | No | deep provider reference | Auth is essential but belongs with storage access |
| GCP ADC / Workload Identity | `accessing-cloud-storage` | No | deep provider reference | Same |
| Azure Managed Identity / Service Principal | `accessing-cloud-storage` | No | deep provider reference | Same |
| fsspec | `accessing-cloud-storage` | No | deep reference | One of several library choices for storage access |
| pyarrow.fs | `accessing-cloud-storage` | No | deep reference | Same |
| obstore | `accessing-cloud-storage` | No | deep reference | Same |
| s3fs | `accessing-cloud-storage` | No | section within fsspec reference | Adapter detail, not a top-level skill |
| gcsfs | `accessing-cloud-storage` | No | section within fsspec reference | Same |
| adlfs | `accessing-cloud-storage` | No | section within fsspec reference | Same |
| Pandas remote I/O | `accessing-cloud-storage` | No | integration reference | Integration pattern, not its own skill |
| Polars cloud I/O | `accessing-cloud-storage` | No | integration reference | Same |
| DuckDB HTTPFS | `accessing-cloud-storage` | No | integration reference | Same |
| PyArrow remote datasets | `accessing-cloud-storage` | No | integration reference | Same |

### 7.4 Catalog and metadata systems

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| Hive Metastore | `managing-data-catalogs` | No | deep reference | Part of catalog selection and architecture |
| AWS Glue Catalog | `managing-data-catalogs` | No | deep reference | Same |
| Tabular / REST catalogs / Nessie-like concepts | `managing-data-catalogs` | No | deep reference | Same |
| DuckDB multi-source attach pattern | `managing-data-catalogs` | No | deep reference | Same |
| Amundsen | `managing-data-catalogs` | No | comparison section or focused reference | Catalog product comparison, not standalone skill |
| DataHub | `managing-data-catalogs` | No | comparison section or focused reference | Same |
| OpenMetadata | `managing-data-catalogs` | No | comparison section or focused reference | Same |

### 7.5 Orchestration and operations

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| Prefect | `orchestrating-data-pipelines` | No | deep reference | Orchestrator choice inside a common workflow |
| Dagster | `orchestrating-data-pipelines` | No | deep reference | Same |
| dbt | `orchestrating-data-pipelines` | No | deep reference | Same |
| FlowerPower | `using-flowerpower` | **Yes** | dedicated skill + scripts | Distinct framework workflow, already has scripts, likely direct user intent |
| Hamilton | `using-flowerpower` | No | deep reference | Part of FlowerPower mental model |
| uv | `using-flowerpower` | No | section/reference | Supporting tool for that workflow |
| Great Expectations | `assuring-data-pipelines` | No | deep reference | Operational assurance, not separate skill boundary |
| Pandera | `assuring-data-pipelines` | No | deep reference | Same |
| OpenTelemetry | `assuring-data-pipelines` | No | deep reference | Same |
| Prometheus | `assuring-data-pipelines` | No | deep reference | Same |
| Grafana | `assuring-data-pipelines` | No | supporting reference | Visualization layer within assurance workflow |

### 7.6 Streaming and AI data systems

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| Apache Kafka | `building-streaming-pipelines` | No | deep reference | One of the core options in a single workflow area |
| MQTT | `building-streaming-pipelines` | No | focused reference | Same |
| NATS JetStream | `building-streaming-pipelines` | No | focused reference | Same |
| OpenAI embeddings APIs | `engineering-ai-pipelines` | No | deep reference | Workflow component, not standalone skill |
| Sentence Transformers | `engineering-ai-pipelines` | No | deep reference | Same |
| pgvector | `engineering-ai-pipelines` | No | deep reference | Same |
| RAG architecture patterns | `engineering-ai-pipelines` | No | deep reference | Core workflow pattern |
| LLM monitoring/cost tracking | `engineering-ai-pipelines` | No | deep reference | Same |

### 7.7 Data science analysis and modeling tools

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| ydata-profiling | `analyzing-data` | No | focused reference | Part of EDA toolkit |
| Sweetviz / D-Tale | `analyzing-data` | No | grouped reference | Secondary profiling tools |
| SciPy statistical tests | `analyzing-data` | No | focused reference | EDA/analysis subtopic |
| Matplotlib | `analyzing-data` | No | deep reference | Visualization library within broader analysis workflow |
| Seaborn | `analyzing-data` | No | deep reference | Same |
| Plotly | `analyzing-data` with cross-link to apps | No | deep reference | Same |
| Altair | `analyzing-data` | No | focused reference | Same |
| hvPlot / HoloViz | `analyzing-data` | No | focused reference | Same |
| Bokeh | `analyzing-data` with cross-link to apps | No | focused reference | Same |
| scikit-learn preprocessing / ColumnTransformer | `engineering-ml-features` | No | deep reference | Feature workflow core |
| category_encoders | `engineering-ml-features` | No | focused reference | Same |
| Feature-engine | `engineering-ml-features` | No | focused reference | Same |
| datetime feature engineering | `engineering-ml-features` | No | focused reference | Same |
| text feature engineering | `engineering-ml-features` | No | focused reference | Same |
| scikit-learn CV utilities | `evaluating-ml-models` | No | deep reference | Evaluation workflow core |
| scikit-learn metrics | `evaluating-ml-models` | No | deep reference | Same |
| Optuna | `evaluating-ml-models` | No | focused reference | Same |
| Ray Tune | `evaluating-ml-models` | No | focused reference | Same |
| MLflow | `evaluating-ml-models` | No | deep reference | Same |
| Weights & Biases | `evaluating-ml-models` | No | focused reference | Same |

### 7.8 Notebook and application tools

| Tool/framework | Proposed home | Dedicated top-level skill? | Form | Rationale |
|---|---|---:|---|---|
| Jupyter / JupyterLab | `working-in-notebooks` | No | deep reference | Notebook workflow core |
| marimo | `working-in-notebooks` | No | deep reference | Same |
| Colab | `working-in-notebooks` | No | supporting reference | Environment choice, not skill boundary |
| nbstripout | `working-in-notebooks` | No | section/reference | Utility within notebook workflow |
| Quarto / nbconvert / Voilà | `working-in-notebooks` | No | grouped publication reference | Publishing/sharing subtopic |
| Streamlit | `building-data-apps` | No | deep reference | App-building framework within a common workflow |
| Panel | `building-data-apps` | No | deep reference | Same |
| Gradio | `building-data-apps` | No | deep reference | Same |
| Dash | `building-data-apps` | No | focused reference | Same |
| NiceGUI | `building-data-apps` | No | focused reference | Same |

---

## 8. Information-architecture standards for the refactored skills

### 8.1 Standard skill layout

Every refactored skill should follow this shape:

```text
skill-name/
├── SKILL.md
├── references/
│   ├── <topic>.md
│   └── ...
├── scripts/
│   ├── <utility>.py
│   └── ...
└── assets/            # only if truly needed
```

### 8.2 SKILL.md structure standard

Every new SKILL.md should have this rough outline:

1. frontmatter: `name`, `description`
2. short statement of scope
3. **when to use this skill**
4. **when not to use this skill** / related skills
5. decision checklist or quick-routing table
6. core workflow / operating procedure
7. validation or feedback loop
8. progressive disclosure section with direct references
9. related skills and migration notes if needed

### 8.3 Reference-file standards

Rules:

1. references must be linked **directly from SKILL.md**
2. no nested reference mazes
3. no hybrid `@skill/path` notation
4. use **plain file paths** for local references
5. use **plain skill names** for related-skill routing
6. every reference over **100 lines** must include a **table of contents**
7. every reference must either:
   - be a substantial practical deep-dive, or
   - include enough authoritative links to be genuinely useful

### 8.4 Small-file policy

The current library has many 30–50 line reference stubs. In the refactor, these should be handled using this rule:

- if the topic is major and frequently used: **expand it** into a full deep reference
- if the topic is minor or adjacent: **merge it** into a broader thematic reference
- if the topic is neither substantial nor frequently used: **delete it**

### 8.5 Script standards

Add scripts only where they create real value.

Priority candidates:

- credential and connectivity validation
- storage path inspection / remote access sanity checks
- pipeline scaffolding
- validation-plan generation
- configuration scaffolding for FlowerPower / orchestrators
- reference integrity or duplication audits

Scripts must:

- fail clearly,
- explain errors in actionable terms,
- avoid magic constants,
- be documented from SKILL.md with clear “run this” language.

---

## 9. Authoring and naming decisions

### 9.1 Naming strategy

Adopt **short, action-oriented names** aligned with the Claude guidance.

Examples:

- `building-data-pipelines`
- `accessing-cloud-storage`
- `designing-data-storage`
- `orchestrating-data-pipelines`
- `evaluating-ml-models`

This is preferable to deep taxonomic names like:

- `data-engineering-storage-remote-access-integrations-polars`

### 9.2 Description strategy

Every description should:

1. be in **third person**,
2. state what the skill does,
3. state when it should be used,
4. include trigger language for likely user wording,
5. avoid vague “comprehensive suite” phrasing.

### 9.3 Frontmatter strategy

Unless there is a confirmed runtime requirement, the refactor should **remove `dependsOn` from frontmatter** and use only standard, portable metadata.

Reasoning:

- current lint warns on `dependsOn`
- Claude best-practice guidance emphasizes simple standard frontmatter
- dependencies can often be expressed more transparently with related-skill routing in the body

If dependencies are later proven essential for the target runtime, they should be reintroduced deliberately and the linter should be updated accordingly.

### 9.4 Time-sensitive wording

Remove time-sensitive headings like `Library selection guide (2026)`.

Replace with durable wording, for example:

- `Library selection guide`
- `Current ecosystem notes`
- `Legacy/older patterns`

---

## 10. Evaluation strategy for the refactor

This refactor should not be executed as a pure docs rewrite. It needs evaluation.

### 10.1 Evaluation goals

For each future skill, verify:

1. the skill triggers when it should,
2. the skill does not trigger when it should not,
3. the skill leads to better outputs than a no-skill baseline,
4. the skill references are sufficient but not bloated,
5. any scripts actually improve reliability.

### 10.2 Evaluation artifacts to create

Create a repo-level structure such as:

```text
evals/
├── building-data-pipelines.json
├── accessing-cloud-storage.json
├── designing-data-storage.json
├── ...
└── trigger-evals/
    ├── building-data-pipelines.json
    ├── ...
```

Each skill should have:

- **3–5 task evaluations**
- **10–20 trigger evaluations** (positive + near-miss negative prompts)

### 10.3 Evaluation method

For each future skill:

1. baseline: no skill
2. current skill(s): current state where applicable
3. refactored skill: candidate new version
4. compare:
   - correctness
   - completeness
   - routing quality
   - brevity / context efficiency
   - output usefulness

### 10.4 Trigger-evaluation examples to include

Examples of what to test:

- broad engineering queries that should hit `building-data-pipelines`
- storage-auth questions that should hit `accessing-cloud-storage`
- Delta/Iceberg choice questions that should hit `designing-data-storage`
- EDA/plotting questions that should hit `analyzing-data`
- notebook-specific questions that should hit `working-in-notebooks` and **not** `building-data-apps`
- Streamlit/Gradio questions that should hit `building-data-apps` and **not** `working-in-notebooks`

### 10.5 Success criteria

The refactor is only ready when:

- broken references are zero
- duplicate reference files are zero or intentionally justified
- each future skill has evaluation coverage
- descriptions have been tuned using trigger evals
- old/new migration is documented

---

## 11. Implementation phases

This is the recommended order of work.

### Phase 0 — Freeze and inventory

Deliverables:

- this plan
- current-state inventory snapshot
- duplicate-file report
- broken-reference report
- migration-map skeleton

### Phase 1 — Define the final taxonomy and authoring template

Deliverables:

- approved future skill list
- approved naming convention
- approved SKILL.md template
- approved reference-file template
- decision on `dependsOn`

### Phase 2 — Build evaluation scaffolding before the rewrite

Deliverables:

- eval JSON skeletons for all future skills
- trigger-eval sets
- benchmark workspace structure
- basic review process documented

### Phase 3 — Refactor engineering skills first

Recommended order:

1. `building-data-pipelines`
2. `accessing-cloud-storage`
3. `designing-data-storage`
4. `managing-data-catalogs`
5. `orchestrating-data-pipelines`
6. `assuring-data-pipelines`
7. `building-streaming-pipelines`
8. `engineering-ai-pipelines`
9. `using-flowerpower`

Reason: these are the most structurally tangled and benefit most from early convergence.

### Phase 4 — Refactor data-science skills

Recommended order:

1. `analyzing-data`
2. `engineering-ml-features`
3. `evaluating-ml-models`
4. `working-in-notebooks`
5. `building-data-apps`

Reason: the engineering side will establish reusable standards for reference quality, routing, and evaluation.

### Phase 5 — Add scripts and validation loops

Deliverables:

- selected new utility scripts
- script documentation in SKILL files
- validation feedback loops in workflow sections

### Phase 6 — Repo cleanup and migration

Deliverables:

- delete superseded skill folders
- add docs-only skill map and migration notes
- update README installation examples
- add changelog / migration guide
- add CI checks for lint, duplicates, missing refs, and eval presence

---

## 12. CI and linting changes required

`tools/skill_lint.py` should be enhanced after the taxonomy is approved.

### 12.1 New lint checks to add

1. fail on missing local references
2. fail on duplicate markdown content above a threshold
3. detect hybrid `@skill/path` reference patterns
4. require TOC for long references
5. optionally forbid non-standard frontmatter unless explicitly allowed
6. detect stale year tags like `(2026)` in headings
7. require eval files for each top-level skill

### 12.2 CI checks to add

Recommended CI gates:

- run `python3 tools/skill_lint.py --strict`
- run duplicate-content detection
- verify no missing references
- verify all eval manifests exist
- optionally run a small smoke benchmark set

---

## 13. Migration and compatibility strategy

This refactor is a breaking change and should be treated like one.

### 13.1 Recommended migration strategy

1. ship the new architecture in a major-version release
2. document the old → new mapping clearly
3. avoid keeping overlapping deprecated skill stubs in the main install set for long, because they will create trigger noise
4. if temporary compatibility wrappers are needed, keep them minimal and clearly marked, then remove them quickly

### 13.2 Documentation to add

Add:

- `docs/skill-map.md`
- `docs/migration-map.md`
- `CHANGELOG.md`
- `CONTRIBUTING.md`

---

## 14. Key risks and design constraints

### 14.1 Packaging/self-containment risk

Because this repo supports selective installation of individual skills, cross-skill shared references are risky unless packaging behavior is explicitly validated.

**Implication:** prefer **skill mergers** over a central shared-reference directory that sits outside a skill folder.

### 14.2 Over-consolidation risk

Too much merging will recreate broad, vague skills.

**Mitigation:** preserve distinct skills where the user intent is meaningfully different, e.g.:

- notebooks vs app building
- orchestration vs FlowerPower
- storage design vs storage access

### 14.3 Under-consolidation risk

Too little merging will preserve the current fragmentation.

**Mitigation:** no separate top-level skills for every integration or library choice.

### 14.4 Evaluation debt risk

If the rewrite starts before evals exist, the refactor may produce cleaner files but worse triggering and worse outcomes.

**Mitigation:** create eval scaffolding before or alongside the first rewrite wave.

---

## 15. Final recommendation

Proceed with a **workflow-centered, action-named, self-contained 14-skill architecture**.

### The most important structural decisions are:

1. **remove `data-engineering` as a triggerable hub skill**
2. **merge core + best practices**
3. **merge auth + remote access + integrations**
4. **merge formats + lakehouse**
5. **merge quality + observability**
6. **merge EDA + visualization**
7. **keep FlowerPower dedicated** because it has a real framework workflow and scripts
8. **keep notebooks and apps separate** because their trigger boundaries are different
9. **eliminate duplicated data-science references entirely**
10. **introduce evaluation-driven skill development before broad content rewrites**

---

## 16. Immediate next step after approval

After this plan is approved, the next step should be:

1. finalize the future skill names,
2. confirm the keep/remove decision for `dependsOn`,
3. create the evaluation skeletons,
4. start with `building-data-pipelines` as the first refactored skill.
