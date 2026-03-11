## Review

- **What’s correct**
  - `skills/building-streaming-pipelines/SKILL.md` has the expected frontmatter shape (`name`, `description`) and does not include `dependsOn`.
  - The skill is structured with the new high-level sections (when to use / when not to use / quick selection / core workflow / production standards / progressive disclosure / related skills / migration notes).
  - `skills/building-streaming-pipelines/references/kafka.md` is substantial, has a TOC, and includes practical producer/consumer/schema/ops examples.

- **Issue [Major]**: Broken progressive-disclosure references to files that do not exist yet.  
  **File**: `skills/building-streaming-pipelines/SKILL.md`  
  **Details**: SKILL.md links to `references/mqtt.md` and `references/nats.md`, but only `references/kafka.md` exists in the implementation scope. This currently breaks navigation and violates the “no broken references” requirement.  
  **Suggested fix**: Add `skills/building-streaming-pipelines/references/mqtt.md` and `skills/building-streaming-pipelines/references/nats.md` (with the planned detailed content), or temporarily remove those links until files exist.

- **Issue [Major]**: SKILL.md is not sufficiently concise for the new architecture; it duplicates implementation-level code that should live in references.  
  **File**: `skills/building-streaming-pipelines/SKILL.md`  
  **Details**: The main SKILL includes multiple long Kafka-centric code blocks (producer, consumer, idempotency, DLQ, batching). This overlaps heavily with `references/kafka.md` and works against progressive disclosure standards.  
  **Suggested fix**: Keep SKILL.md at guidance/decision/workflow level with short snippets only; move detailed code to `references/*.md` and link out.

- **Issue [Major]**: Migration notes are currently ahead of implementation state.  
  **File**: `skills/building-streaming-pipelines/SKILL.md`  
  **Details**: Migration notes claim content has been reorganized into detailed reference files for each streaming platform, but MQTT and NATS reference files are not present in this implementation snapshot.  
  **Suggested fix**: Either complete MQTT/NATS reference migrations first, or adjust migration notes to reflect partial/in-progress migration accurately.

- **Issue [Minor]**: A few Kafka examples are not copy-paste runnable due to missing imports/context.  
  **File**: `skills/building-streaming-pipelines/references/kafka.md`  
  **Details**: Some snippets use modules/symbols not imported in the same snippet context (e.g., `time`, `json`, `KafkaException`).  
  **Suggested fix**: Ensure each standalone snippet includes required imports or explicitly annotate prerequisites above each block.

- **Note: Observations**
  - `implementation.md` and `anchor-context.md` were not present at the provided paths; review scope was inferred from current changed artifacts under `skills/building-streaming-pipelines/` and the supplied `plan.md`.
  - Cross-references in “Related skills” appear to use plain skill names and align with existing skill directories.

- **Gate**: **Fail**
