Done. I've written `anchor-context.md` with:

- **Ticket Summary**: das-8erm - making `skill_lint.py` fail on missing refs and flag hybrid `@skill/path` in strict mode
- **Complexity**: Simple (<50 LOC) - two discrete changes to existing functions
- **Research Gaps**: None - existing knowledge sufficient
- **External Libraries**: Only pyyaml and stdlib already in use
- **Testing**: Verify strict mode exit code changes from 0→1
- **Recommended Path**: A (Minimal) - single-file modification
- **Concrete Hints**: 
  - Change 1: Upgrade missing reference warnings to errors in strict mode
  - Change 2: Add detection for `@skill/path` hybrid patterns (the `@` prefix with a `/` path)
  - Update `lint_markdown_references()` signature to accept `args` parameter