# Data quality validation instructions

- Use Python and `uv` for this project.
- Keep SharePoint access read-only.
- Preserve the downloaded source workbook as XLSX.
- Process only worksheet 2; worksheet 1 contains broken Excel transformations.
- Before proposing or implementing analysis or validation logic, research the official library APIs and established solutions; prefer built-in functionality over recreating it.
- Establish typed analysis views once and reuse them; do not repeat coercion or normalization chains in downstream expressions.
- Verify that a cleanup operation is necessary against the actual data before encoding it.
- Prefer simple, happy-path-heavy code.
- Let failures propagate naturally instead of adding retries, fallbacks, or broad exception handling.
- Ask before adding defensive programming or custom error handling.
- Never commit credentials or downloaded source data.
