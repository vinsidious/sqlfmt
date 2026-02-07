# Editor and CI Integrations

Copy-paste recipes for using sqlfmt in your development workflow.

## Pre-commit Hooks

### Husky (npm projects)

```bash
npm install --save-dev husky
npx husky init
```

Add to `.husky/pre-commit`:

```bash
npx @vcoppola/sqlfmt --check $(git diff --cached --name-only --diff-filter=ACM -- '*.sql')
```

### pre-commit framework

Add to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/vinsidious/sqlfmt
    rev: v1.1.1  # use the latest tag
    hooks:
      - id: sqlfmt
        name: sqlfmt (format)
        entry: npx @vcoppola/sqlfmt --write
        language: node
        types: [sql]
      - id: sqlfmt-check
        name: sqlfmt (check only)
        entry: npx @vcoppola/sqlfmt --check
        language: node
        types: [sql]
```

Then run:

```bash
pre-commit install
```

Use `sqlfmt` to auto-fix files on commit, or `sqlfmt-check` to fail without modifying files (useful in CI).

## GitHub Actions

```yaml
name: SQL Format Check
on: [push, pull_request]

jobs:
  sqlfmt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - run: npm install -g @vcoppola/sqlfmt
      - run: sqlfmt --check "**/*.sql"
```

> **Note:** Quote the glob pattern (`"**/*.sql"`) to prevent shell expansion. Without quotes, your shell may expand the glob before sqlfmt sees it, which can miss files in subdirectories or fail if no `.sql` files exist in the current directory.

## GitLab CI

```yaml
sqlfmt:
  image: node:20
  stage: lint
  script:
    - npm install -g @vcoppola/sqlfmt
    - sqlfmt --check "**/*.sql"
  rules:
    - changes:
        - "**/*.sql"
```

## VS Code

### Run on Save (recommended)

Use the [Run on Save](https://marketplace.visualstudio.com/items?itemName=emeraldwalk.RunOnSave) extension to format SQL files automatically:

```json
{
  "emeraldwalk.runonsave": {
    "commands": [
      {
        "match": "\\.sql$",
        "cmd": "npx @vcoppola/sqlfmt ${file}"
      }
    ]
  }
}
```

## Vim / Neovim

### formatprg

```vim
" In .vimrc or init.vim
autocmd FileType sql setlocal formatprg=npx\ @vcoppola/sqlfmt
```

Then use `gq` to format a selection, or `gggqG` to format the entire file.

### ALE

```vim
" In .vimrc or init.vim
let g:ale_fixers = {
\   'sql': ['sqlfmt'],
\}
let g:ale_fix_on_save = 1

" Define the sqlfmt fixer
let g:ale_sql_sqlfmt_executable = 'npx'
let g:ale_sql_sqlfmt_options = '@vcoppola/sqlfmt'
```

### Neovim (conform.nvim)

```lua
require("conform").setup({
  formatters_by_ft = {
    sql = { "sqlfmt" },
  },
  formatters = {
    sqlfmt = {
      command = "npx",
      args = { "@vcoppola/sqlfmt" },
      stdin = true,
    },
  },
})
```

## IntelliJ / DataGrip

Configure as an External Tool:

1. Go to **Settings > Tools > External Tools**
2. Click **+** to add a new tool:
   - **Name**: sqlfmt
   - **Program**: `npx`
   - **Arguments**: `@vcoppola/sqlfmt $FilePath$`
   - **Working directory**: `$ProjectFileDir$`
3. Optionally assign a keyboard shortcut under **Settings > Keymap > External Tools > sqlfmt**

To check formatting without modifying files:

- **Arguments**: `@vcoppola/sqlfmt --check $FilePath$`

## npm Scripts

Add these to your project's `package.json`:

```json
{
  "scripts": {
    "sql:format": "sqlfmt --write \"**/*.sql\"",
    "sql:check": "sqlfmt --check \"**/*.sql\""
  }
}
```

> **Note:** Quote glob patterns inside npm scripts to prevent shell expansion on Linux/macOS. The escaped double quotes (`\"**/*.sql\"`) ensure the pattern is passed to sqlfmt as-is.

Then run:

```bash
npm run sql:format   # format all SQL files in place
npm run sql:check    # check formatting (CI-friendly, exits non-zero if unformatted)
```
