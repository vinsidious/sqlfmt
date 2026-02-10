# Editor and CI Integrations

Copy-paste recipes for using holywell in your development workflow.

## Pre-commit Hooks

### Husky (npm projects)

```bash
npm install --save-dev husky
npx husky init
```

Add to `.husky/pre-commit`:

```bash
npx holywell --check $(git diff --cached --name-only --diff-filter=ACM -- '*.sql')
```

### pre-commit framework

Add to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/vinsidious/holywell
    rev: v1.7.0  # update to the latest release tag when bumping holywell
    hooks:
      - id: holywell
        name: holywell (format)
        entry: npx holywell --write
        language: node
        types: [sql]
      - id: holywell-check
        name: holywell (check only)
        entry: npx holywell --check
        language: node
        types: [sql]
```

Then run:

```bash
pre-commit install
```

Use `holywell` to auto-fix files on commit, or `holywell-check` to fail without modifying files (useful in CI).

## GitHub Actions

```yaml
name: SQL Format Check
on: [push, pull_request]

jobs:
  holywell:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - run: npm install -g holywell
      - run: holywell --check "**/*.sql"
```

> **Note:** Quote the glob pattern (`"**/*.sql"`) to prevent shell expansion. Without quotes, your shell may expand the glob before holywell sees it, which can miss files in subdirectories or fail if no `.sql` files exist in the current directory.

## GitLab CI

```yaml
holywell:
  image: node:20
  stage: lint
  script:
    - npm install -g holywell
    - holywell --check "**/*.sql"
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
        "cmd": "npx holywell ${file}"
      }
    ]
  }
}
```

## Vim / Neovim

### formatprg

```vim
" In .vimrc or init.vim
autocmd FileType sql setlocal formatprg=npx\ holywell
```

Then use `gq` to format a selection, or `gggqG` to format the entire file.

### ALE

```vim
" In .vimrc or init.vim
let g:ale_fixers = {
\   'sql': ['holywell'],
\}
let g:ale_fix_on_save = 1

" Define the holywell fixer
let g:ale_sql_holywell_executable = 'npx'
let g:ale_sql_holywell_options = 'holywell'
```

### Neovim (conform.nvim)

```lua
require("conform").setup({
  formatters_by_ft = {
    sql = { "holywell" },
  },
  formatters = {
    holywell = {
      command = "npx",
      args = { "holywell" },
      stdin = true,
    },
  },
})
```

## IntelliJ / DataGrip

Configure as an External Tool:

1. Go to **Settings > Tools > External Tools**
2. Click **+** to add a new tool:
   - **Name**: holywell
   - **Program**: `npx`
   - **Arguments**: `holywell $FilePath$`
   - **Working directory**: `$ProjectFileDir$`
3. Optionally assign a keyboard shortcut under **Settings > Keymap > External Tools > holywell**

To check formatting without modifying files:

- **Arguments**: `holywell --check $FilePath$`

## npm Scripts

Add these to your project's `package.json`:

```json
{
  "scripts": {
    "sql:format": "holywell --write \"**/*.sql\"",
    "sql:check": "holywell --check \"**/*.sql\""
  }
}
```

> **Note:** Quote glob patterns inside npm scripts to prevent shell expansion on Linux/macOS. The escaped double quotes (`\"**/*.sql\"`) ensure the pattern is passed to holywell as-is.

Then run:

```bash
npm run sql:format   # format all SQL files in place
npm run sql:check    # check formatting (CI-friendly, exits non-zero if unformatted)
```
