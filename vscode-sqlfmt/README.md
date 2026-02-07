# sqlfmt for VS Code

An opinionated, zero-config SQL formatter for VS Code that implements the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/) with **river alignment**.

## What it does

sqlfmt right-aligns SQL keywords so your queries flow along a consistent vertical "river":

**Before:**

```sql
select e.name, e.salary, d.department_name from employees as e inner join departments as d on e.department_id = d.department_id where e.salary > 50000 and d.department_name in ('Sales', 'Engineering') order by e.salary desc;
```

**After:**

```sql
SELECT e.name, e.salary, d.department_name
  FROM employees AS e
       INNER JOIN departments AS d
       ON e.department_id = d.department_id
 WHERE e.salary > 50000
   AND d.department_name IN ('Sales', 'Engineering')
 ORDER BY e.salary DESC;
```

## Features

- **Zero configuration** -- just install and format
- **River alignment** -- keywords right-align to a consistent column
- **Format on save** -- works with VS Code's built-in format-on-save
- **PostgreSQL support** -- first-class support for `sql` and `pgsql` language modes
- **Command palette** -- run "Format SQL with sqlfmt" from the command palette
- **Status bar indicator** -- shows formatting status for SQL files
- **Keyword uppercasing** -- `SELECT`, `FROM`, `WHERE` are uppercased automatically
- **Idempotent** -- formatting already-formatted SQL produces the same output

## Installation

As of February 7, 2026, `sqlfmt` is not yet published on the VS Code Marketplace.

Install from source:

```bash
cd vscode-sqlfmt
npm install
npm run build
npx @vscode/vsce package
code --install-extension sqlfmt-1.1.1.vsix
```

To update, rebuild and reinstall the generated `.vsix`.

## Usage

1. Open any `.sql` file in VS Code.
2. Run **Format Document** (`Shift+Alt+F` / `Shift+Option+F`).
3. The file is formatted using sqlfmt's river-aligned style.

To make sqlfmt the default SQL formatter and enable format on save:

```json
{
  "[sql]": {
    "editor.defaultFormatter": "vcoppola.sqlfmt",
    "editor.formatOnSave": true
  },
  "[pgsql]": {
    "editor.defaultFormatter": "vcoppola.sqlfmt",
    "editor.formatOnSave": true
  }
}
```

## Configuration

| Setting | Type | Default | Description |
|---|---|---|---|
| `sqlfmt.enable` | `boolean` | `true` | Enable or disable the sqlfmt formatter |
| `sqlfmt.maxInputBytes` | `number` | `1048576` | Maximum file size (UTF-8 bytes) to format; larger files are skipped |

## About river alignment

River alignment is a formatting approach from the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/). Instead of indenting everything uniformly, clause keywords (`SELECT`, `FROM`, `WHERE`, `AND`, `OR`, etc.) are right-aligned so that the content to their right flows along a consistent vertical column -- the "river". This makes SQL structure immediately visible at a glance.

For more about sqlfmt and the full list of supported SQL features, see the [sqlfmt repository](https://github.com/vinsidious/sqlfmt).

## License

MIT
