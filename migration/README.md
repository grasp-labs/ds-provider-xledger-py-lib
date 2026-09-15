# Mutation field migration

Syncs Xledger GraphQL `add` / `update` input fields into packaged
`src/ds_provider_xledger_py_lib/assets/*/create|update/metadata.json` files.

The script introspects live mutation input types, flattens nested inputs
(`owner.dbId` → `owner_dbId`), then:

- adds fields present in Xledger but missing from assets
- removes asset fields that are no longer in the schema (on `--apply`)

## Setup

1. Ensure dev deps are installed (`python-dotenv`, logger, etc.):

   ```bash
   uv sync --extra dev
   ```

2. Put your token in `.env` at the repo root:

   ```bash
   XLEDGER_TOKEN=your_token_here
   # optional:
   # XLEDGER_HOST=https://www.xledger.net/graphql
   ```

## Usage

Dry-run (default — report only, no writes):

```bash
uv run python migration/migrate_mutation_fields.py
```

Apply changes:

```bash
uv run python migration/migrate_mutation_fields.py --apply
```

Limit to one entrypoint:

```bash
uv run python migration/migrate_mutation_fields.py --entrypoint salesOrderItems --apply
```

Useful flags:

| Flag | Purpose |
|------|---------|
| `--apply` | Write adds/removes into `metadata.json` |
| `--entrypoint NAME` | Only migrate that asset folder |
| `--include-helpers` | Also include `showAlternatives` / `specificity` |
| `--request-interval 0.35` | Throttle GraphQL calls (burst limit is 20/5s) |
| `-v` / `--verbose` | Debug logging |

## Notes

- List-shaped inputs (e.g. `details`) are skipped and reported; they do
  not map cleanly to flat tabular metadata.
- Existing field descriptions / `required` values are preserved; only
  missing fields are appended with schema-derived metadata.
- Prefer dry-run first, then `--apply` after reviewing the report.
