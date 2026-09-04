import * as fs from 'fs';

/** Reads KEY=value (.env), flat YAML (`  KEY: value`, helm values) or JSON into a map. */
export function loadEnvFile(path: string): Record<string, string> {
  const text = fs.readFileSync(path, 'utf8');
  const out: Record<string, string> = {};
  if (path.endsWith('.json')) {
    const obj = JSON.parse(text);
    const walk = (o: unknown) => {
      if (o && typeof o === 'object') {
        for (const [k, v] of Object.entries(o as Record<string, unknown>)) {
          if (/^[A-Z][A-Z0-9_]*$/.test(k) && (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean')) out[k] = String(v);
          else walk(v);
        }
      }
    };
    walk(obj);
    return out;
  }
  for (const raw of text.split('\n')) {
    const line = raw.replace(/\s+#.*$/, '');
    let m = line.match(/^\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (!m) m = line.match(/^\s+([A-Z][A-Z0-9_]*):\s*(.*?)\s*$/);
    if (!m) continue;
    let v = m[2];
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    out[m[1]] = v;
  }
  return out;
}
