# Implementation Plan

## Goal
Create the `building-streaming-pipelines` skill by refactoring content from `data-engineering-streaming` into the new architecture defined in SKILL_REFACTORING_PLAN.md, using `building-data-pipelines` as the format template.

## Tasks

1. **Create skill directory structure**
   - Create `skills/building-streaming-pipelines/` directory
   - Create `skills/building-streaming-pipelines/references/` subdirectory
   - Acceptance: Directory structure exists and matches standard layout

2. **Create SKILL.md with standard structure**
   - File: `skills/building-streaming-pipelines/SKILL.md`
   - Changes: Write SKILL.md following the building-data-pipelines template with:
     - Frontmatter: `name`, `description` (no dependsOn)
     - Action-oriented description with trigger keywords
     - "When to use this skill" section (streaming use cases)
     - "When not to use this skill" with cross-references to related skills
     - Quick tool selection table (Kafka vs MQTT vs NATS)
     - Core workflow section (design → implement → operate)
     - Production standards section (idempotency, error handling, DLQ)
     - Progressive disclosure with direct references
     - Related skills section with proper cross-references
     - Migration notes from data-engineering-streaming
   - Acceptance: SKILL.md renders correctly, all cross-references use plain skill names (not @syntax)

3. **Create references/kafka.md**
   - File: `skills/building-streaming-pipelines/references/kafka.md`
   - Changes: Migrate Kafka content from data-engineering-streaming/SKILL.md:
     - Table of contents (required for >100 lines)
     - Installation and configuration
     - Producer patterns (basic, with Schema Registry/Avro)
     - Consumer patterns (groups, manual commit, error handling)
     - Stream processing patterns (ksqlDB, Kafka Streams)
     - Production considerations (partitioning, replication, monitoring)
   - Acceptance: File has TOC, comprehensive examples, no broken internal links

4. **Create references/mqtt.md**
   - File: `skills/building-streaming-pipelines/references/mqtt.md`
   - Changes: Migrate MQTT content from data-engineering-streaming/SKILL.md:
     - Table of contents
     - Installation and broker setup
     - Publisher patterns (QoS levels, retained messages)
     - Subscriber patterns (topic wildcards, callbacks)
     - IoT-specific considerations (constrained networks, last will)
   - Acceptance: File has TOC, IoT-focused examples, explains QoS levels

5. **Create references/nats.md**
   - File: `skills/building-streaming-pipelines/references/nats.md`
   - Changes: Migrate NATS JetStream content from data-engineering-streaming/SKILL.md:
     - Table of contents
     - Installation and connection
     - Stream configuration (persistence, retention policies)
     - Push vs pull consumers
     - Work queue patterns
     - Request-reply patterns
   - Acceptance: File has TOC, covers JetStream specifically, includes async patterns

6. **Verify cross-references and related skills**
   - Review all files for proper cross-references to:
     - `accessing-cloud-storage` — for cloud streaming setup
     - `designing-data-storage` — for persisting streams to Delta/Iceberg
     - `orchestrating-data-pipelines` — for scheduling stream processors
     - `assuring-data-pipelines` — for data quality in streams
     - `building-data-pipelines` — for batch processing of stream data
     - `engineering-ai-pipelines` — for ML inference on streams
   - Acceptance: All cross-references use plain skill names, no broken references

7. **Verify eval manifests exist and are valid**
   - Confirm `eval/building-streaming-pipelines.json` exists (already present)
   - Confirm `eval/trigger-eval/building-streaming-pipelines.json` exists (already present)
   - Acceptance: Both eval files are valid JSON with proper structure

8. **Run skill lint to validate structure**
   - Run `python3 tools/skill_lint.py` on the new skill
   - Acceptance: No errors, minimal warnings (dependsOn warning expected if lint checks for it)

## Files to Modify
- None (all new files)

## New Files

1. `skills/building-streaming-pipelines/SKILL.md` — Main skill file with workflow guidance
2. `skills/building-streaming-pipelines/references/kafka.md` — Kafka patterns and examples
3. `skills/building-streaming-pipelines/references/mqtt.md` — MQTT patterns for IoT
4. `skills/building-streaming-pipelines/references/nats.md` — NATS JetStream patterns

## Dependencies

- Task 1 (directory creation) must complete before Tasks 2-4
- Task 2 (SKILL.md) should be drafted after reviewing Tasks 3-5 content to ensure references align
- Tasks 3, 4, 5 (reference files) can be done in parallel
- Task 6 (cross-references) must be done after Tasks 2-5
- Task 7 (eval verification) is independent
- Task 8 (lint) must be done last

## Risks

1. **Content scope creep**: The original data-engineering-streaming SKILL.md is ~400 lines. Ensure references are comprehensive but SKILL.md stays concise per best practices.

2. **Broken cross-references**: The source uses `@skill-name` syntax. Must convert to plain skill names for cross-references and file paths for local references.

3. **Related skill naming**: Some related skills in the source (like `@assuring-data-pipelines`) may not exist yet. Verify actual skill names exist in the repo.

4. **Reference file length**: Each reference should be substantial (>100 lines ideally) with TOC. May need to expand beyond migrated content if source sections are thin.

5. **Missing context**: The anchor-context.md was missing, so this plan assumes standard SKILL_REFACTORING_PLAN.md architecture. If additional requirements exist in the ticket, adjust accordingly.

## Content Migration Map

From `data-engineering-streaming/SKILL.md`:
- Frontmatter → New SKILL.md frontmatter (remove dependsOn)
- Quick Comparison table → SKILL.md Quick tool selection
- When to Use Which? → SKILL.md When to use this skill
- Skill Dependencies → SKILL.md Related skills
- Apache Kafka section → references/kafka.md
- MQTT for IoT section → references/mqtt.md
- NATS JetStream section → references/nats.md
- Production Patterns → Split across references and SKILL.md Production standards
- References section → Remove (external links go to references, cross-references use skill names)
