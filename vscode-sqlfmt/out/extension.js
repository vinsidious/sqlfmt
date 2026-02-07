"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.activate = activate;
exports.deactivate = deactivate;
const vscode = __importStar(require("vscode"));
const sqlfmt_1 = require("@vcoppola/sqlfmt");
const SUPPORTED_LANGUAGES = ['sql', 'pgsql'];
const DEFAULT_MAX_INPUT_BYTES = 1_048_576; // 1 MiB
let outputChannel;
let statusBarItem;
function readMaxInputBytes(config) {
    const configured = config.get('maxInputBytes', DEFAULT_MAX_INPUT_BYTES);
    if (!Number.isFinite(configured) || configured < 1) {
        return DEFAULT_MAX_INPUT_BYTES;
    }
    return Math.floor(configured);
}
class SQLFormattingProvider {
    provideDocumentFormattingEdits(document) {
        const config = vscode.workspace.getConfiguration('sqlfmt');
        if (!config.get('enable', true)) {
            return [];
        }
        const text = document.getText();
        const maxInputBytes = readMaxInputBytes(config);
        const inputBytes = Buffer.byteLength(text, 'utf8');
        if (inputBytes > maxInputBytes) {
            const message = `Document size ${inputBytes.toLocaleString()} bytes exceeds sqlfmt.maxInputBytes (${maxInputBytes.toLocaleString()})`;
            outputChannel.appendLine(`[warn] ${message}`);
            updateStatusBar('$(warning) sqlfmt', message);
            vscode.window.setStatusBarMessage(`sqlfmt: ${message}`, 5000);
            return [];
        }
        try {
            const formatted = (0, sqlfmt_1.formatSQL)(text);
            const fullRange = new vscode.Range(document.positionAt(0), document.positionAt(text.length));
            updateStatusBar('$(check) sqlfmt', 'Formatted successfully');
            return [vscode.TextEdit.replace(fullRange, formatted)];
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            outputChannel.appendLine(`[error] ${message}`);
            updateStatusBar('$(warning) sqlfmt', `Format error: ${message}`);
            vscode.window.setStatusBarMessage(`sqlfmt: ${message}`, 5000);
            return [];
        }
    }
}
function updateStatusBar(text, tooltip) {
    statusBarItem.text = text;
    statusBarItem.tooltip = tooltip;
}
function updateStatusBarVisibility(editor) {
    if (editor && SUPPORTED_LANGUAGES.includes(editor.document.languageId)) {
        statusBarItem.show();
    }
    else {
        statusBarItem.hide();
    }
}
function activate(context) {
    outputChannel = vscode.window.createOutputChannel('sqlfmt');
    statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    statusBarItem.text = 'sqlfmt';
    statusBarItem.tooltip = 'SQL formatter (river-aligned)';
    updateStatusBarVisibility(vscode.window.activeTextEditor);
    const provider = new SQLFormattingProvider();
    const config = vscode.workspace.getConfiguration('sqlfmt');
    if (config.get('enable', true)) {
        for (const lang of SUPPORTED_LANGUAGES) {
            context.subscriptions.push(vscode.languages.registerDocumentFormattingEditProvider(lang, provider));
        }
    }
    const formatCommand = vscode.commands.registerCommand('sqlfmt.format', () => {
        vscode.commands.executeCommand('editor.action.formatDocument');
    });
    context.subscriptions.push(outputChannel, statusBarItem, formatCommand, vscode.window.onDidChangeActiveTextEditor(updateStatusBarVisibility));
}
function deactivate() { }
//# sourceMappingURL=extension.js.map