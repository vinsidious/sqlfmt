import * as vscode from 'vscode';
import { formatSQL } from '@vcoppola/sqlfmt';

const SUPPORTED_LANGUAGES = ['sql', 'pgsql'];

let outputChannel: vscode.OutputChannel;
let statusBarItem: vscode.StatusBarItem;

class SQLFormattingProvider implements vscode.DocumentFormattingEditProvider {
  provideDocumentFormattingEdits(
    document: vscode.TextDocument,
  ): vscode.TextEdit[] {
    const config = vscode.workspace.getConfiguration('sqlfmt');
    if (!config.get<boolean>('enable', true)) {
      return [];
    }

    const text = document.getText();
    try {
      const formatted = formatSQL(text);
      const fullRange = new vscode.Range(
        document.positionAt(0),
        document.positionAt(text.length),
      );

      updateStatusBar('$(check) sqlfmt', 'Formatted successfully');
      return [vscode.TextEdit.replace(fullRange, formatted)];
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      outputChannel.appendLine(`[error] ${message}`);
      updateStatusBar('$(warning) sqlfmt', `Format error: ${message}`);
      vscode.window.setStatusBarMessage(`sqlfmt: ${message}`, 5000);
      return [];
    }
  }
}

function updateStatusBar(text: string, tooltip: string): void {
  statusBarItem.text = text;
  statusBarItem.tooltip = tooltip;
}

function updateStatusBarVisibility(editor: vscode.TextEditor | undefined): void {
  if (editor && SUPPORTED_LANGUAGES.includes(editor.document.languageId)) {
    statusBarItem.show();
  } else {
    statusBarItem.hide();
  }
}

export function activate(context: vscode.ExtensionContext): void {
  outputChannel = vscode.window.createOutputChannel('sqlfmt');

  statusBarItem = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Right,
    100,
  );
  statusBarItem.text = 'sqlfmt';
  statusBarItem.tooltip = 'SQL formatter (river-aligned)';

  updateStatusBarVisibility(vscode.window.activeTextEditor);

  const provider = new SQLFormattingProvider();

  const config = vscode.workspace.getConfiguration('sqlfmt');
  if (config.get<boolean>('enable', true)) {
    for (const lang of SUPPORTED_LANGUAGES) {
      context.subscriptions.push(
        vscode.languages.registerDocumentFormattingEditProvider(lang, provider),
      );
    }
  }

  const formatCommand = vscode.commands.registerCommand('sqlfmt.format', () => {
    vscode.commands.executeCommand('editor.action.formatDocument');
  });

  context.subscriptions.push(
    outputChannel,
    statusBarItem,
    formatCommand,
    vscode.window.onDidChangeActiveTextEditor(updateStatusBarVisibility),
  );
}

export function deactivate(): void {}
