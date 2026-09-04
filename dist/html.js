"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.renderHtml = renderHtml;
/** Self-contained viewer for an AsyncAPI document, in the spirit of swagger-ui. */
function renderHtml(doc) {
    const data = JSON.stringify(doc).replace(/</g, '\\u003c');
    return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escape(doc.info.title)} · Kafka docs</title>
<style>
:root{--bg:#f4f3ee;--card:#fbfaf7;--ink:#1c2230;--muted:#6f7788;--line:#dedbd2;--line2:#ece9e0;--accent:#b8622b;--send:#b8622b;--recv:#2f855a;--code:#edeae2}
@media(prefers-color-scheme:dark){:root{--bg:#141820;--card:#1b212c;--ink:#e6e4dd;--muted:#98a0af;--line:#2a3140;--line2:#222938;--accent:#d97b3f;--send:#d97b3f;--recv:#4cae7a;--code:#232a38}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
.layout{display:grid;grid-template-columns:300px 1fr;min-height:100vh}
nav{border-right:1px solid var(--line);background:var(--card);padding:12px 10px;position:sticky;top:0;height:100vh;overflow:auto}
nav input{width:100%;font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:4px;background:var(--bg);color:var(--ink);margin-bottom:8px}
nav a{display:block;padding:3px 6px;border-radius:4px;color:var(--ink);text-decoration:none;font-family:ui-monospace,Menlo,monospace;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
nav a:hover{background:var(--bg)}nav a small{color:var(--muted);margin-left:6px}
main{padding:24px 32px 80px;max-width:100ch}h1{font-size:24px;margin:0 0 4px}.sub{color:var(--muted);margin:0 0 20px}
section{border:1px solid var(--line);border-radius:6px;background:var(--card);margin:0 0 14px;overflow:hidden}
section>header{display:flex;flex-wrap:wrap;gap:10px;align-items:center;padding:10px 14px;cursor:pointer;border-bottom:1px solid transparent}
section[open]>header{border-bottom-color:var(--line2)}
section>header h3{margin:0;font-family:ui-monospace,Menlo,monospace;font-size:14px;font-weight:600}
.env{color:var(--muted);font-size:12px;font-family:ui-monospace,Menlo,monospace}
.badge{font-size:11px;padding:1px 7px;border-radius:9px;color:#fff}.badge.send{background:var(--send)}.badge.recv{background:var(--recv)}
.body{display:none;padding:6px 14px 14px}section[open] .body{display:block}
.op{padding:8px 0;border-top:1px solid var(--line2)}.op:first-child{border-top:0}
.op .who{font-family:ui-monospace,Menlo,monospace;font-size:12.5px}.op .src{color:var(--muted);font-size:12px}
.tabs{display:flex;gap:2px;margin:8px 0 0;align-items:flex-end}.tabs .copy{margin-left:auto;font:inherit;font-size:11px;padding:3px 9px;border:1px solid var(--line);border-radius:4px;background:var(--bg);color:var(--muted);cursor:pointer;margin-bottom:3px}.tabs .copy:hover{color:var(--ink)}.tabs .copy[data-done]{color:var(--recv);border-color:var(--recv)}.tabs button{font:inherit;font-size:11px;letter-spacing:.04em;text-transform:uppercase;padding:4px 10px;border:1px solid var(--line);border-bottom:0;background:var(--bg);color:var(--muted);border-radius:4px 4px 0 0;cursor:pointer}.tabs button[aria-selected=true]{background:var(--code);color:var(--ink);font-weight:600}
.pane{display:none;background:var(--code);border:1px solid var(--line);border-radius:0 6px 6px 6px;padding:10px 12px;overflow-x:auto;font-family:ui-monospace,Menlo,monospace;font-size:12.5px;line-height:1.55}.pane[data-active]{display:block}
.ex .s{color:#2f6f3f}.ex .n{color:#8a4b0c}.ex .b{color:#5b5ea6}.ex .k{color:var(--ink);font-weight:600}
@media(prefers-color-scheme:dark){.ex .s{color:#8fd19e}.ex .n{color:#f0b26b}.ex .b{color:#a5a8f0}}
.model{white-space:nowrap}.model .prop{padding-left:18px}.model .pn{color:var(--ink);font-weight:600}.model .star{color:#c53030;margin-left:1px}.model .pt{color:var(--muted)}.model .mt{color:var(--accent);font-weight:600}.model .en{color:var(--muted)}.model .desc{color:var(--muted);font-style:italic;margin-left:8px}
.model details{display:inline-block;vertical-align:top}.model details>summary{list-style:none;cursor:pointer;display:inline}.model details>summary::-webkit-details-marker{display:none}.model details>summary::before{content:"▸";display:inline-block;width:12px;color:var(--muted)}.model details[open]>summary::before{content:"▾"}
.model .brace{color:var(--muted)}
.hidden{display:none}code{background:var(--code);padding:1px 4px;border-radius:3px;font-size:12px}
@media(max-width:820px){.layout{grid-template-columns:1fr}nav{position:static;height:auto;max-height:40vh}main{padding:16px}}
</style></head><body><div class="layout"><nav><input id="q" type="search" placeholder="filter channels"><div id="list"></div></nav><main><h1>${escape(doc.info.title)}</h1><p class="sub">${escape(doc.info.description ?? '')} Generated ${escape(doc['x-generated-at'])}.</p><div id="chs"></div></main></div>
<script>
const DOC=${data};
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const deref=r=>{const p=r.replace(/^#\\//,'').split('/');let o=DOC;for(const k of p)o=o?.[k];return o};
function refName(r){return r.split('/').pop()}
function res(s){return s&&s.$ref?deref(s.$ref):s}
function example(s,seen,depth){seen=seen||new Set();depth=depth||0;if(!s||depth>8)return null;
 if(s.$ref){const n=refName(s.$ref);if(seen.has(n))return {};const ns=new Set(seen);ns.add(n);return example(deref(s.$ref),ns,depth+1)}
 if(s.const!==undefined)return s.const;if(s.enum)return s.enum[0];
 if(s.anyOf)return example(s.anyOf[0],seen,depth+1);
 if(s.allOf){return Object.assign({},...s.allOf.map(x=>example(x,seen,depth+1)||{}))}
 if(s.type==='array')return [example(s.items,seen,depth+1)];
 if(s.type==='object'||s.properties){const o={};for(const [k,v] of Object.entries(s.properties||{}))o[k]=example(v,seen,depth+1);if(s.additionalProperties&&!Object.keys(o).length)o['additionalProp1']=example(s.additionalProperties,seen,depth+1);return o}
 if(s.type==='string')return s.format==='date-time'?'2026-01-01T00:00:00.000Z':'string';if(s.type==='number'||s.type==='integer')return 0;if(s.type==='boolean')return true;if(s.type==='null')return null;return {}}
function colorJson(v){const j=JSON.stringify(v,null,2);return esc(j).replace(/("(?:\\\\.|[^"\\\\])*")(\\s*:)?/g,(m,str,colon)=>colon?'<span class="k">'+str+'</span>'+colon:'<span class="s">'+str+'</span>').replace(/\\b(-?\\d+(?:\\.\\d+)?)\\b/g,'<span class="n">$1</span>').replace(/\\b(true|false|null)\\b/g,'<span class="b">$1</span>')}
function tname(s){if(!s)return 'any';if(s.$ref)return refName(s.$ref);if(s.type==='array')return tname(s.items)+'[]';if(s.enum)return 'string';if(s.const!==undefined)return typeof s.const;if(s.anyOf)return s.anyOf.map(tname).join(' | ');return (s.type||s.description||'any')+(s.format?'($'+s.format+')':'')}
function model(s,seen,depth,title){seen=seen||new Set();depth=depth||0;if(!s)return '<span class="pt">any</span>';
 if(s.$ref){const n=refName(s.$ref);if(seen.has(n)||depth>8)return '<span class="mt">'+esc(n)+'</span>';const ns=new Set(seen);ns.add(n);return model(deref(s.$ref),ns,depth,n)}
 if(s.const!==undefined)return '<span class="pt">'+esc(typeof s.const)+'</span> <span class="en">const: '+esc(JSON.stringify(s.const))+'</span>';
 if(s.enum)return '<span class="pt">string</span> <span class="en">Enum: [ '+esc(s.enum.map(v=>JSON.stringify(v)).join(', '))+' ]</span>';
 if(s.anyOf)return s.anyOf.map(x=>model(x,seen,depth+1)).join(' <span class="brace">|</span> ');
 if(s.allOf)return s.allOf.map(x=>model(x,seen,depth+1)).join(' <span class="brace">&amp;</span> ');
 if(s.type==='array')return '<span class="brace">[</span><div class="prop">'+model(s.items,seen,depth+1)+'</div><span class="brace">]</span>';
 if(s.type==='object'||s.properties){const req=new Set(s.required||[]);const rows=Object.entries(s.properties||{}).map(([k,v])=>{const r=res(v);const nested=r&&(r.type==='object'||r.properties||r.type==='array'||r.anyOf||r.allOf);return '<div class="prop"><span class="pn">'+esc(k)+'</span>'+(req.has(k)?'<span class="star">*</span>':'')+' '+(nested?model(v,seen,depth+1):'<span class="pt">'+esc(tname(v))+'</span>'+(r&&r.enum?' <span class="en">Enum: [ '+esc(r.enum.map(x=>JSON.stringify(x)).join(', '))+' ]</span>':'')+(r&&r.const!==undefined?' <span class="en">'+esc(JSON.stringify(r.const))+'</span>':''))+(v.description?'<span class="desc">'+esc(v.description)+'</span>':'')+'</div>'});
  if(s.additionalProperties)rows.push('<div class="prop"><span class="pn">&lt; * &gt;</span> '+model(s.additionalProperties,seen,depth+1)+'</div>');
  const t=title||s.title;return '<details'+(depth<2?' open':'')+'><summary>'+(t?'<span class="mt">'+esc(t)+'</span> ':'')+'<span class="brace">{</span></summary>'+rows.join('')+'<span class="brace">}</span></details>'}
 return '<span class="pt">'+esc(tname(s))+'</span>'}
let uid=0;
const COPY={};
function fullSchema(s,seen,depth){seen=seen||new Set();depth=depth||0;if(!s||depth>10)return s;
 if(s.$ref){const n=refName(s.$ref);if(seen.has(n))return {$ref:s.$ref};const ns=new Set(seen);ns.add(n);const d=deref(s.$ref);return d?fullSchema(d,ns,depth+1):s}
 if(Array.isArray(s))return s.map(x=>fullSchema(x,seen,depth+1));
 if(typeof s==='object'){const o={};for(const [k,v] of Object.entries(s))o[k]=(k==='properties'||k==='items'||k==='additionalProperties'||k==='anyOf'||k==='allOf')?fullSchema(v,seen,depth+1):v;if(s.properties){o.properties={};for(const [k,v] of Object.entries(s.properties))o.properties[k]=fullSchema(v,seen,depth+1)}return o}
 return s}
function copyPane(btn,id){const active=document.getElementById(id+'-0').hasAttribute('data-active')?0:1;const text=COPY[id][active];const done=()=>{btn.setAttribute('data-done','');btn.textContent='Copied';setTimeout(()=>{btn.removeAttribute('data-done');btn.textContent='Copy'},1200)};
 if(navigator.clipboard&&navigator.clipboard.writeText)navigator.clipboard.writeText(text).then(done,()=>fallback());else fallback();
 function fallback(){const ta=document.createElement('textarea');ta.value=text;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();try{document.execCommand('copy');done()}catch(e){}document.body.removeChild(ta)}}
function payloadView(pay){if(!pay)return '<div class="pane" data-active style="color:var(--muted)">payload type not resolved</div>';const id='p'+(uid++);const ex=example(pay);COPY[id]=[JSON.stringify(ex,null,2),JSON.stringify(fullSchema(pay),null,2)];
 return '<div class="tabs" role="tablist"><button aria-selected="true" onclick="tab(this,\\''+id+'\\',0)">Example Value</button><button aria-selected="false" onclick="tab(this,\\''+id+'\\',1)">Schema</button><button class="copy" title="Copy the visible tab as JSON" onclick="copyPane(this,\\''+id+'\\')">Copy</button></div><div class="pane ex" data-active id="'+id+'-0"><pre style="margin:0;background:none;padding:0">'+colorJson(ex)+'</pre></div><div class="pane model" id="'+id+'-1">'+model(pay)+'</div>'}
function tab(btn,id,i){btn.parentNode.querySelectorAll('button').forEach((b,j)=>b.setAttribute('aria-selected',j===i));[0,1].forEach(j=>{const el=document.getElementById(id+'-'+j);if(j===i)el.setAttribute('data-active','');else el.removeAttribute('data-active')})}
const chs=Object.entries(DOC.channels).map(([id,ch])=>{const ops=Object.entries(DOC.operations).filter(([,o])=>o.channel.$ref.split('/').pop()===id).map(([oid,o])=>({oid,...o}));return {id,ch,ops,name:ch.address||ch['x-env-var']}}).sort((a,b)=>a.name.localeCompare(b.name));
document.getElementById('list').innerHTML=chs.map(c=>'<a href="#'+encodeURIComponent(c.id)+'" data-n="'+esc(c.name)+'">'+esc(c.name)+'<small>'+c.ops.filter(o=>o.action==='send').length+'→'+c.ops.filter(o=>o.action==='receive').length+'</small></a>').join('');
document.getElementById('chs').innerHTML=chs.map(c=>{const ops=c.ops.map(o=>{const m=o.messages&&o.messages[0]?deref(o.messages[0].$ref):null;const pay=m?deref(m.$ref||'').payload||m.payload:null;return '<div class="op"><span class="badge '+(o.action==='send'?'send':'recv')+'">'+(o.action==='send'?'produces':'consumes')+'</span> <span class="who">'+esc(o['x-service'])+' · '+esc(o['x-class'])+'.'+esc(o['x-method'])+'</span> <span class="src">'+esc(o['x-source'])+(o['x-key']?' · key: <code>'+esc(o['x-key'])+'</code>':'')+'</span>'+payloadView(pay)+'</div>'}).join('');
 return '<section id="'+esc(c.id)+'" data-n="'+esc(c.name)+'"><header onclick="this.parentNode.toggleAttribute(\\'open\\')"><h3>'+esc(c.name)+'</h3><span class="env">'+esc(c.ch['x-env-var'])+(c.ch.address?'':' (unresolved)')+'</span><span class="badge send">'+c.ops.filter(o=>o.action==='send').length+' producers</span><span class="badge recv">'+c.ops.filter(o=>o.action==='receive').length+' consumers</span></header><div class="body">'+(ops||'<div class="d">no operations</div>')+'</div></section>'}).join('');
const q=document.getElementById('q');q.oninput=()=>{const v=q.value.toLowerCase();document.querySelectorAll('[data-n]').forEach(el=>el.classList.toggle('hidden',v&&!el.dataset.n.toLowerCase().includes(v)))};
if(location.hash){const el=document.getElementById(decodeURIComponent(location.hash.slice(1)));if(el){el.setAttribute('open','');el.scrollIntoView()}}
window.addEventListener('hashchange',()=>{const el=document.getElementById(decodeURIComponent(location.hash.slice(1)));if(el)el.setAttribute('open','')});
</script></body></html>`;
}
function escape(s) {
    return s.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}
