# vscode-sqlfmt

VS Code extension for [sqlfmt](https://github.com/vinsidious/sqlfmt) -- an opinionated, river-aligned SQL formatter based on [sqlstyle.guide](https://www.sqlstyle.guide/).

## Installation

This extension is not yet published to the VS Code Marketplace. To use it locally:

```bash
cd vscode-sqlfmt
npm install
npm run build
```

Then install the extension from the `vscode-sqlfmt/` directory using the **Extensions: Install from VSIX** command or by symlinking the folder into `~/.vscode/extensions/`.

## Usage

1. Open any `.sql` file in VS Code.
2. Run **Format Document** (`Shift+Alt+F` / `Shift+Option+F`).
3. The file will be formatted using sqlfmt's river-aligned style.

To make sqlfmt the default SQL formatter, add this to your VS Code `settings.json`:

```json
{
  "[sql]": {
    "editor.defaultFormatter": "vcoppola.sqlfmt"
  }
}
```

To format on save:

```json
{
  "[sql]": {
    "editor.defaultFormatter": "vcoppola.sqlfmt",
    "editor.formatOnSave": true
  }
}
```
