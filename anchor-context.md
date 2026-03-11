Done! I've created `anchor-context.md` with the implementation context for ticket **das-r7yk**.

**Summary of findings:**

1. **Ticket das-r7yk** aims to wire strict lint and eval-presence checks into CI (GitHub Actions)

2. **Complexity: Simple** - All dependencies are already completed:
   - `tools/skill_lint.py --strict` (das-8erm, das-6zd4)
   - 14 target-skill eval manifests (das-lih7)

3. **No research gaps** - Everything needed is already in place

4. **External libraries**: GitHub Actions (built-in CI/CD), Python 3

5. **Testing**: Verify CI catches lint failures and validates 14 eval manifests

6. **Recommended Path: A (Minimal)** - Create `.github/workflows/ci.yml` with lint and eval-presence jobs

7. **Key tasks**:
   - Create `.github/workflows/ci.yml` 
   - Run `python3 tools/skill_lint.py --strict` 
   - Verify all 14 target skill eval manifests exist in `eval/` and `eval/trigger-eval/`