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
exports.loadEnvFile = loadEnvFile;
const fs = __importStar(require("fs"));
/** Reads KEY=value (.env), flat YAML (`  KEY: value`, helm values) or JSON into a map. */
function loadEnvFile(path) {
    const text = fs.readFileSync(path, 'utf8');
    const out = {};
    if (path.endsWith('.json')) {
        const obj = JSON.parse(text);
        const walk = (o) => {
            if (o && typeof o === 'object') {
                for (const [k, v] of Object.entries(o)) {
                    if (/^[A-Z][A-Z0-9_]*$/.test(k) && (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean'))
                        out[k] = String(v);
                    else
                        walk(v);
                }
            }
        };
        walk(obj);
        return out;
    }
    for (const raw of text.split('\n')) {
        const line = raw.replace(/\s+#.*$/, '');
        let m = line.match(/^\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=\s*(.*?)\s*$/);
        if (!m)
            m = line.match(/^\s+([A-Z][A-Z0-9_]*):\s*(.*?)\s*$/);
        if (!m)
            continue;
        let v = m[2];
        if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'")))
            v = v.slice(1, -1);
        out[m[1]] = v;
    }
    return out;
}
