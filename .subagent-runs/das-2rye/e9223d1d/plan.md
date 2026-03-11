# Implementation Plan

## Goal
Consolidate lakehouse design references in `data-engineering-storage-lakehouse` and `data-engineering-storage-formats` skills by updating to direct-link style references and ensuring explicit cross-links between storage formats and lakehouse documentation.

## Tasks

1. **Update lakehouse SKILL.md Related Skills section**
   - File: `skills/data-engineering-storage-lakehouse/SKILL.md`
   - Changes:
     - Remove deprecated references to `@data-engineering-storage-remote-access/integrations/delta-lake` and `@data-engineering-storage-remote-access/integrations/iceberg` in the Related Skills section
     - Replace with direct links to the skill's own detailed guides: `@data-engineering-storage-lakehouse/delta-lake.md`, `@data-engineering-storage-lakehouse/iceberg.md`, `@data-engineering-storage-lakehouse/hudi.md`
     - Update the cloud storage reference from `@data-engineering-storage-remote-access` to `@accessing-cloud-storage`
     - Ensure all related skill references use consistent direct-link style
   - Acceptance: No deprecated skill references remain; all links use direct `@skill-name` or `@skill-name/file.md` format

2. **Update lakehouse SKILL.md Skill Dependencies section**
   - File: `skills/data-engineering-storage-lakehouse/SKILL.md`
   - Changes:
     - Update `@data-engineering-storage-remote-access` reference to `@accessing-cloud-storage`
   - Acceptance: Dependencies point to canonical non-deprecated skills

3. **Verify and enhance cross-links in storage-formats SKILL.md**
   - File: `skills/data-engineering-storage-formats/SKILL.md`
   - Changes:
     - Verify the existing lakehouse reference in the "Skill Dependencies" section is correct
     - Ensure the References section at the bottom has explicit cross-link to `@data-engineering-storage-lakehouse`
     - Update any `@data-engineering-storage-remote-access` references to `@accessing-cloud-storage`
   - Acceptance: Explicit bidirectional cross-links exist between storage-formats and lakehouse skills

4. **Update delta-lake.md detailed guide**
   - File: `skills/data-engineering-storage-lakehouse/delta-lake.md`
   - Changes:
     - Update the "Cloud Storage Integration" section reference from `@data-engineering-storage-remote-access/integrations/delta-lake` to `@accessing-cloud-storage`
   - Acceptance: No references to deprecated integration skills

5. **Update iceberg.md detailed guide**
   - File: `skills/data-engineering-storage-lakehouse/iceberg.md`
   - Changes:
     - Update any references to `@data-engineering-storage-remote-access/integrations/iceberg` to point to `@accessing-cloud-storage`
   - Acceptance: No references to deprecated integration skills

## Files to Modify

- `skills/data-engineering-storage-lakehouse/SKILL.md` - Update Related Skills and Dependencies sections
- `skills/data-engineering-storage-lakehouse/delta-lake.md` - Update cloud storage integration reference
- `skills/data-engineering-storage-lakehouse/iceberg.md` - Update cloud storage integration reference (verify)
- `skills/data-engineering-storage-formats/SKILL.md` - Verify/enhance cross-links to lakehouse

## New Files

None - this is a reference consolidation task only.

## Dependencies

- Task 1 should be completed before Task 2 (same file)
- Task 4 and 5 can be done in parallel after Task 1
- Task 3 is independent but should be consistent with Task 1's linking style

## Risks

1. **Deprecated content still valuable**: The deprecated integration skills contain detailed cloud storage setup that hasn't been fully migrated. The brief references in accessing-cloud-storage may not cover all the Delta Lake/Iceberg-specific storage_options details. Consider if any critical content needs to be preserved or referenced.

2. **Breaking existing references**: If other skills reference these deprecated paths, they should be updated too. Verify with grep for the deprecated paths.

3. **Storage-design skill doesn't exist yet**: The deprecated skills mention migration to a future `storage-design` skill that doesn't exist. The references should point to the current canonical skills until that skill is created.
