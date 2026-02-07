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
npx sqlfmt --check $(git diff --cached --name-only --diff-filter=ACM -- '*.sql')
```

### pre-commit framework

Add to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/vinsidious/sqlfmt
    rev: v1.1.1  # use the latest tag
    hooks:
      - id: sqlfmt
```

Then run:

```bash
pre-commit install
```

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
      - run: sqlfmt --check **/*.sql
```

## VS Code

### Extension (recommended)

Install the `vscode-sqlfmt` extension (see `vscode-sqlfmt/README.md` in this repo) and set it as your default SQL formatter:

```json
{
  "[sql]": {
    "editor.defaultFormatter": "vcoppola.vscode-sqlfmt",
    "editor.formatOnSave": true
  }
}
```

### Manual: Run on Save

If you prefer not to use the extension, you can use the [Run on Save](https://marketplace.visualstudio.com/items?itemName=emeraldwalk.RunOnSave) extension:

```json
{
  "emeraldwalk.runonsave": {
    "commands": [
      {
        "match": "\\.sql$",
        "cmd": "npx sqlfmt ${file}"
      }
    ]
  }
}
```

## Vim / Neovim

### formatprg

```vim
" In .vimrc or init.vim
autocmd FileType sql setlocal formatprg=npx\ sqlfmt
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
let g:ale_sql_sqlfmt_options = 'sqlfmt'
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
      args = { "sqlfmt" },
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
   - **Arguments**: `sqlfmt $FilePath$`
   - **Working directory**: `$ProjectFileDir$`
3. Optionally assign a keyboard shortcut under **Settings > Keymap > External Tools > sqlfmt**

To check formatting without modifying files:

- **Arguments**: `sqlfmt --check $FilePath$`

## npm Scripts

Add these to your project's `package.json`:

```json
{
  "scripts": {
    "sql:format": "sqlfmt **/*.sql",
    "sql:check": "sqlfmt --check **/*.sql"
  }
}
```

Then run:

```bash
npm run sql:format   # format all SQL files
npm run sql:check    # check formatting (CI-friendly, exits non-zero if unformatted)
```
