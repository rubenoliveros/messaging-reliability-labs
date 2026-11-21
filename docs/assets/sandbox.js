/* ---------- shared helpers ---------- */
const ACCOUNTS = ["acct-1","acct-2","acct-3","acct-4","acct-5"];
const ACC_COLOR = {"acct-1":"var(--a1)","acct-2":"var(--a2)","acct-3":"var(--a3)","acct-4":"var(--a4)","acct-5":"var(--a5)","acct-9":"#888"};
const pick = a => a[Math.floor(Math.random()*a.length)];
const money = c => (c<0?"-":"") + "$" + Math.abs(c).toLocaleString();
function shuffle(a){for(let i=a.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[a[i],a[j]]=[a[j],a[i]];}return a;}
function makeLogger(el){
  return (level,msg)=>{
    const d=document.createElement("div"); d.className=level;
    d.textContent=`[${level}] ${msg}`; el.appendChild(d);
    while(el.children.length>200) el.removeChild(el.firstChild);
    el.scrollTop=el.scrollHeight;
  };
}
function chip(label, account, cls, title){
  const s=document.createElement("span"); s.className="chip "+(cls||""); s.title=title||"";
  const d=document.createElement("span"); d.className="dot"; d.style.background=ACC_COLOR[account]||"#888";
  s.appendChild(d); s.appendChild(document.createTextNode(label)); return s;
}
function balancesOf(rows){
  const b={}; for(const r of rows){ b[r.account]=b[r.account]||{c:0,p:0}; if(r.kind==="charge") b[r.account].c+=r.amount; else b[r.account].p+=r.amount; } return b;
}
function makeLoop(stepFn, speedSel, playBtn){
  let timer=null, running=true;
  const start=()=>{ clearInterval(timer); timer=setInterval(()=>{ if(running) stepFn(); }, +speedSel.value); };
  speedSel.onchange=start;
  playBtn.onclick=()=>{ running=!running; playBtn.textContent=running?"Pause":"Play"; };
  start();
}
function reconcileReport(out, source, rows, extra){
  const ids={}; rows.forEach(r=>ids[r.id]=(ids[r.id]||0)+1);
  const missing=[...source.keys()].filter(id=>!ids[id]);
  const dupes=Object.keys(ids).filter(id=>ids[id]>1);
  const unexpected=Object.keys(ids).filter(id=>!source.has(id));
  const sb=balancesOf([...source.values()]), db=balancesOf(rows);
  const diffs=[...new Set([...Object.keys(sb),...Object.keys(db)])].filter(a=>{
    const s=sb[a]?sb[a].c-sb[a].p:0, d=db[a]?db[a].c-db[a].p:0; return s!==d;});
  const problems=[];
  if(missing.length) problems.push(`<b>${missing.length} missing</b> in the ledger (look in the DLQ, then redrive; if not there, the message was lost)`);
  if(dupes.length) problems.push(`<b>${dupes.length} duplicated</b> entries (double-counted money: the consumer isn't idempotent)`);
  if(unexpected.length) problems.push(`<b>${unexpected.length} unexpected</b> entries`);
  if(diffs.length) problems.push(`<b>${diffs.length} account balance(s)</b> differ from the partner: ${diffs.join(", ")}`);
  if(extra) problems.push(...extra);
  out.className="recon "+(problems.length?"bad":"ok");
  out.innerHTML = problems.length
    ? `<b>Not reconciled.</b><ul style="margin:6px 0 0 18px;padding:0">${problems.map(p=>`<li>${p}</li>`).join("")}</ul>`
    : `<b>Reconciled.</b> ${source.size} partner events, ${rows.length} ledger entries, every balance matches.`;
}

/* ---------- RabbitMQ simulator ---------- */
(function(){
  const $=id=>document.getElementById(id);
  const log=makeLogger($("r-log"));
  const TIERS=[{name:"retry.5s",ticks:4},{name:"retry.20s",ticks:8},{name:"retry.60s",ticks:13}];
  const MAX_ATTEMPTS=TIERS.length+1;
  let S;
  function reset(){
    S={main:[],tiers:[[],[],[]],dlq:[],rows:[],source:new Map(),tick:0,down:0,seq:0,last:null};
    $("r-log").innerHTML=""; $("r-recon-out").className="recon"; render();
    log("INFO","topology declared: exchange 'ledger' -> queue 'ledger.entries' (+3 retry queues, DLQ)");
  }
  function newEvent(){
    S.seq++; return {id:"e"+S.seq, account:pick(ACCOUNTS), kind:Math.random()<.67?"charge":"payment", amount:(5+Math.floor(Math.random()*95))*100};
  }
  $("r-pub").onclick=()=>{
    let d=0;
    for(let i=0;i<10;i++){
      const ev=newEvent(); S.source.set(ev.id,ev);
      S.main.push({ev,attempt:1});
      if($("r-dup").checked && Math.random()<.3){ S.main.push({ev,attempt:1,dup:true}); d++; }
    }
    log("INFO",`partner published 10 events${d?` (+${d} resends)`:""} with publisher confirms`); render();
  };
  $("r-poison").onclick=()=>{
    S.seq++; S.main.push({ev:{id:"bad"+S.seq,account:"acct-9",kind:pick(["refund","charge"]),amount:pick([-500,0]),poison:true},attempt:1});
    log("WARN","partner sent a malformed event"); render();
  };
  $("r-redrive").onclick=()=>{
    const keep=[], moved=[];
    for(const m of S.dlq){ if(m.ev.poison) keep.push(m); else moved.push({ev:m.ev,attempt:1,redriven:true}); }
    S.dlq=keep; S.main.push(...moved);
    log("INFO",`redrive: ${moved.length} message(s) back to ledger.entries, ${keep.length} poison left in DLQ for a human`); render();
  };
  $("r-recon").onclick=()=>reconcileReport($("r-recon-out"),S.source,S.rows);
  $("r-reset").onclick=reset;

  function insert(ev){
    if($("r-idem").checked && S.rows.some(r=>r.id===ev.id)) return false;
    S.rows.push({id:ev.id,account:ev.account,kind:ev.kind,amount:ev.amount}); return true;
  }
  function step(){
    S.tick++;
    TIERS.forEach((t,i)=>{
      const ready=S.tiers[i].filter(m=>m.readyAt<=S.tick);
      if(ready.length){ S.tiers[i]=S.tiers[i].filter(m=>m.readyAt>S.tick); ready.forEach(m=>S.main.push(m)); log("INFO",`${t.name}: TTL expired, ${ready.length} message(s) dead-lettered back to main queue`); }
    });
    if(S.down>0){ S.down--; if(S.down===0) log("OK","consumer restarted"); render(); return; }
    const m=S.main.shift();
    if(!m){ S.last=null; render(); return; }
    const ev=m.ev, tag=`${ev.id} ${ev.account}`;
    S.last={m,text:""};

    if(ev.poison){ S.dlq.push({...m,reason:"poison: invalid kind/amount"}); S.last.text="poison: sent to DLQ, acked"; log("ERROR",`${tag}: poison -> DLQ (no retry)`); render(); return; }

    const fail=Math.random() < +$("r-fail").value;
    if(fail){
      if(m.attempt<MAX_ATTEMPTS){
        const t=TIERS[m.attempt-1];
        S.tiers[m.attempt-1].push({ev,attempt:m.attempt+1,readyAt:S.tick+t.ticks,redel:true});
        S.last.text=`timeout: retry via ${t.name}`; log("WARN",`${tag}: transient failure (attempt ${m.attempt}) -> ${t.name}`);
      } else {
        S.dlq.push({...m,reason:"max attempts"}); S.last.text="gave up: DLQ"; log("ERROR",`${tag}: failed ${m.attempt} times -> DLQ`);
      }
      render(); return;
    }

    const crash=$("r-crash").checked && Math.random()<.2;
    if($("r-order").value==="ack"){
      if(crash){ S.down=3; S.last.text="ACKED, then crashed before the write: message LOST"; log("ERROR",`${tag}: acked, then crashed before DB commit. Broker deleted it: LOST`); render(); return; }
      const ins=insert(ev); S.last.text=ins?"acked, then stored":"acked, duplicate ignored";
      log(ins?"OK":"INFO",`${tag}: ${S.last.text}`); render(); return;
    }
    const ins=insert(ev);
    if(crash){
      S.down=3; S.main.unshift({ev,attempt:m.attempt,redel:true});
      S.last.text=`${ins?"stored":"duplicate ignored"}, crashed before ack: will be redelivered`;
      log("WARN",`${tag}: ${ins?"committed":"duplicate ignored"}, crashed before ack -> broker redelivers it`); render(); return;
    }
    S.last.text=ins?"stored, then acked":"duplicate ignored (unique key), acked";
    log(ins?"OK":"INFO",`${tag}: ${S.last.text}${m.redel?" [redelivered]":""}`); render();
  }

  function render(){
    const main=$("r-main"); main.innerHTML="";
    S.main.slice(0,60).forEach(m=>main.appendChild(chip(m.ev.id,m.ev.account,m.ev.poison?"poison":m.dup?"dup":m.redel?"redel":"",`${m.ev.kind} ${money(m.ev.amount)} attempt ${m.attempt}`)));
    if(S.main.length>60) main.appendChild(document.createTextNode(` +${S.main.length-60}`));
    $("r-main-n").textContent=S.main.length;
    const retry=$("r-retry"); retry.innerHTML="";
    TIERS.forEach((t,i)=>{
      const row=document.createElement("div"); row.style.marginBottom="6px";
      row.innerHTML=`<div style="font-size:11.5px">${t.name} (${S.tiers[i].length})</div>`;
      const c=document.createElement("div"); c.className="chips";
      S.tiers[i].forEach(m=>c.appendChild(chip(m.ev.id,m.ev.account,"redel",`attempt ${m.attempt}, back in ${m.readyAt-S.tick} ticks`)));
      row.appendChild(c); retry.appendChild(row);
    });
    $("r-retry-n").textContent=S.tiers.reduce((a,t)=>a+t.length,0);
    const dlq=$("r-dlq"); dlq.innerHTML="";
    S.dlq.forEach(m=>dlq.appendChild(chip(m.ev.id,m.ev.account,m.ev.poison?"poison":"",m.reason)));
    $("r-dlq-n").textContent=S.dlq.length;
    const c=$("r-consumer");
    c.className="consumer"+(S.down?" crashed":"");
    c.innerHTML = S.down ? `<div class="what" style="color:var(--bad)"><b>crashed</b>, restarting...</div>`
      : S.last ? `<div class="what"><b>${S.last.m.ev.id}</b> ${S.last.m.ev.account}<br>${S.last.text}</div>` : `<div class="what note">waiting for messages</div>`;
    const b=balancesOf(S.rows), tb=$("r-bal"); tb.innerHTML="";
    ACCOUNTS.forEach(a=>{ const x=b[a]||{c:0,p:0}; tb.insertAdjacentHTML("beforeend",`<tr><td><span class="chip"><span class="dot" style="background:${ACC_COLOR[a]}"></span>${a}</span></td><td class="num">${money(x.c)}</td><td class="num">${money(x.p)}</td><td class="num"><b>${money(x.c-x.p)}</b></td></tr>`); });
    $("r-stats").innerHTML=`<span>partner sent <b>${S.source.size}</b></span><span>ledger rows <b>${S.rows.length}</b></span><span>in flight <b>${S.main.length+S.tiers.reduce((a,t)=>a+t.length,0)}</b></span><span>DLQ <b>${S.dlq.length}</b></span>`;
  }
  reset();
  makeLoop(step,$("r-speed"),$("r-play"));
})();

/* ---------- Kafka simulator ---------- */
(function(){
  const $=id=>document.getElementById(id);
  const log=makeLogger($("k-log"));
  const P=3, MAX_ATTEMPTS=4;
  const partOf=a=>({"acct-1":0,"acct-2":1,"acct-3":2,"acct-4":0,"acct-5":1,"acct-9":2})[a];
  const STATUSES=["applicant","approved","active","delinquent","closed"];
  let S;
  function reset(){
    S={parts:[[],[],[]],committed:[0,0,0],pos:[0,0,0],retry:[null,null,null],rr:{},dlq:[],rows:[],status:{},source:new Map(),srcStatus:{},tick:0,down:0,seq:0};
    $("k-log").innerHTML=""; $("k-recon-out").className="recon";
    log("INFO","topic ledger.events created with 3 partitions; group 'ledger-writer' joins"); logAssignment(); render();
  }
  const consumers=()=>+$("k-cons").value;
  function assignment(){ const n=consumers(), a={}; for(let p=0;p<P;p++) a[p]=p%n; return a; }
  function logAssignment(){
    const a=assignment(), n=consumers(), parts={};
    for(let p=0;p<P;p++){ (parts[a[p]]=parts[a[p]]||[]).push("p"+p); }
    const desc=[...Array(n).keys()].map(c=>`c${c+1}: ${parts[c]?parts[c].join(","):"idle"}`).join(" | ");
    log("INFO",`rebalance -> ${desc}`);
  }
  function produce(rec){ const p=partOf(rec.account); rec.offset=S.parts[p].length; S.parts[p].push(rec); }
  $("k-pub").onclick=()=>{
    let d=0;
    for(let i=0;i<10;i++){
      S.seq++; const ev={type:"entry",id:"e"+S.seq,account:pick(ACCOUNTS),kind:Math.random()<.67?"charge":"payment",amount:(5+Math.floor(Math.random()*95))*100};
      S.source.set(ev.id,ev); produce({...ev});
      if($("k-dup").checked && Math.random()<.3){ produce({...ev,dup:true}); d++; }
    }
    log("INFO",`producer (acks=all, idempotent) sent 10 entries${d?` (+${d} partner resends)`:""}, keyed by account`); render();
  };
  $("k-status").onclick=()=>{
    for(const a of ["acct-1","acct-2","acct-3"]){
      const vs=STATUSES.map((s,i)=>({type:"status",account:a,status:s,version:i+1}));
      S.srcStatus[a]=vs[vs.length-1];
      shuffle(vs).forEach(v=>produce({...v}));
    }
    log("WARN","partner sent status versions 1-5 for acct-1..3 in shuffled order"); render();
  };
  $("k-poison").onclick=()=>{ S.seq++; produce({type:"entry",id:"bad"+S.seq,account:"acct-9",kind:"refund",amount:-500,poison:true}); log("WARN","partner sent a malformed record"); render(); };
  $("k-replay").onclick=()=>{
    S.committed=[0,0,0]; S.pos=[0,0,0]; S.retry=[null,null,null];
    log("WARN","group stopped; offsets reset to 0 on every partition; consumers restart and re-read the whole log"); render();
  };
  $("k-redrive").onclick=()=>{
    const keep=[]; let n=0;
    for(const r of S.dlq){ if(r.poison) keep.push(r); else { const c={...r}; delete c.reason; produce(c); n++; } }
    S.dlq=keep; log("INFO",`redrive: ${n} record(s) re-produced to ledger.events, ${keep.length} poison kept`); render();
  };
  $("k-cons").onchange=()=>{ S.pos=[...S.committed]; S.retry=[null,null,null]; logAssignment(); log("INFO","uncommitted progress is re-read from the committed offsets"); render(); };
  $("k-recon").onclick=()=>{
    const extra=[];
    const stale=Object.keys(S.srcStatus).filter(a=>!S.status[a]||S.status[a].version!==S.srcStatus[a].version);
    if(stale.length) extra.push(`<b>${stale.length} account status(es)</b> stale (an older version overwrote a newer one): ${stale.join(", ")}`);
    reconcileReport($("k-recon-out"),S.source,S.rows,extra);
  };
  $("k-reset").onclick=reset;

  function write(rec){
    if(rec.type==="status"){
      const cur=S.status[rec.account];
      if($("k-ver").checked && cur && cur.version>=rec.version) return `stale v${rec.version} ignored (have v${cur.version})`;
      S.status[rec.account]={status:rec.status,version:rec.version}; return `status -> ${rec.status} v${rec.version}`;
    }
    if($("k-idem").checked && S.rows.some(r=>r.id===rec.id)) return "duplicate ignored";
    S.rows.push({id:rec.id,account:rec.account,kind:rec.kind,amount:rec.amount}); return "stored";
  }
  function crash(){ S.down=3; S.pos=[...S.committed]; S.retry=[null,null,null]; }

  function processPartition(p){
    const rec=S.parts[p][S.pos[p]];
    const tag=`p${p}@${rec.offset} ${rec.id||rec.account}`;
    if(S.retry[p] && S.tick<S.retry[p].until) return;
    if(rec.poison){ S.dlq.push({...rec,reason:"poison"}); S.pos[p]++; S.committed[p]=S.pos[p]; log("ERROR",`${tag}: poison -> DLQ, offset committed`); return; }
    if(rec.type==="entry" && Math.random() < +$("k-fail").value){
      const attempt=(S.retry[p]?S.retry[p].attempt:0)+1;
      if(attempt>=MAX_ATTEMPTS){ S.dlq.push({...rec,reason:"max attempts"}); S.retry[p]=null; S.pos[p]++; S.committed[p]=S.pos[p]; log("ERROR",`${tag}: failed ${attempt} times -> DLQ, partition moves on`); return; }
      const wait=2**(attempt-1); S.retry[p]={attempt,until:S.tick+wait};
      log("WARN",`${tag}: transient failure (attempt ${attempt}), partition p${p} waits ${wait} tick(s) (blocking retry keeps order)`); return;
    }
    S.retry[p]=null;
    const crashNow=$("k-crash").checked && Math.random()<.15;
    if($("k-order").value==="before"){
      S.committed[p]=S.pos[p]+1;
      if(crashNow){ log("ERROR",`${tag}: offset committed, crashed before DB write: record SKIPPED (lost)`); crash(); return; }
      const r=write(rec); S.pos[p]++; log(r==="stored"||r.startsWith("status")?"OK":"INFO",`${tag}: ${r}`); return;
    }
    const r=write(rec);
    if(crashNow){ log("WARN",`${tag}: ${r}, crashed before offset commit -> will re-read from committed offsets`); crash(); return; }
    S.pos[p]++; S.committed[p]=S.pos[p];
    log(r==="stored"||r.startsWith("status")?"OK":"INFO",`${tag}: ${r}, offset committed`);
  }

  function step(){
    S.tick++;
    if(S.down>0){ S.down--; if(S.down===0){ log("OK","consumer restarted, resuming from committed offsets"); } render(); return; }
    const a=assignment(), n=consumers();
    for(let c=0;c<n;c++){
      const mine=[...Array(P).keys()].filter(p=>a[p]===c && S.pos[p]<S.parts[p].length);
      if(!mine.length) continue;
      S.rr[c]=((S.rr[c]||0)+1)%mine.length;
      processPartition(mine[S.rr[c]]);
      if(S.down) break;
    }
    render();
  }

  function render(){
    const wrap=$("k-parts"); wrap.innerHTML="";
    const a=assignment();
    for(let p=0;p<P;p++){
      const lag=S.parts[p].length-S.committed[p];
      const el=document.createElement("div"); el.className="part";
      const retryTag=S.retry[p]?`<span class="tag retry">retrying, attempt ${S.retry[p].attempt}</span>`:"";
      el.innerHTML=`<div class="hdr"><span><b>partition ${p}</b> &nbsp;<span class="tag">consumer c${a[p]+1}</span> ${retryTag}</span><span>log end ${S.parts[p].length} · committed ${S.committed[p]} · <b>lag ${lag}</b></span></div>`;
      const cells=document.createElement("div"); cells.className="cells";
      const from=Math.max(0,S.parts[p].length-40);
      S.parts[p].slice(from).forEach(rec=>{
        const d=document.createElement("div");
        let cls="cell"; if(rec.offset<S.pos[p]) cls+=" done"; if(rec.offset===S.pos[p]) cls+=" pos"; if(rec.offset===S.committed[p]) cls+=" committed-edge";
        if(rec.poison) cls+=" poison"; else if(rec.dup) cls+=" dup"; if(rec.type==="status") cls+=" status";
        d.className=cls;
        const label=rec.type==="status"?`v${rec.version}`:rec.id;
        d.innerHTML=`<div class="off">${rec.offset}</div><div><span class="dot" style="display:inline-block;width:6px;height:6px;border-radius:50%;background:${ACC_COLOR[rec.account]}"></span> ${label}</div>`;
        d.title=rec.type==="status"?`${rec.account} status ${rec.status} v${rec.version}`:`${rec.account} ${rec.kind} ${money(rec.amount)}`;
        cells.appendChild(d);
      });
      if(!S.parts[p].length) cells.innerHTML='<span class="note">empty</span>';
      el.appendChild(cells); wrap.appendChild(el);
    }
    const idle=consumers()>P?`<span class="tag idle">c4 idle: more consumers than partitions</span>`:"";
    if(S.down) wrap.insertAdjacentHTML("afterbegin",`<div class="note" style="color:var(--bad)"><b>consumer crashed</b>, restarting...</div>`);
    if(idle) wrap.insertAdjacentHTML("beforeend",`<div>${idle}</div>`);

    const b=balancesOf(S.rows), tb=$("k-bal"); tb.innerHTML="";
    ACCOUNTS.forEach(acc=>{ const x=b[acc]||{c:0,p:0}; const st=S.status[acc];
      tb.insertAdjacentHTML("beforeend",`<tr><td><span class="chip"><span class="dot" style="background:${ACC_COLOR[acc]}"></span>${acc}</span></td><td class="num"><b>${money(x.c-x.p)}</b></td><td>${st?`${st.status} (v${st.version})`:'<span class="note">none</span>'}</td></tr>`); });
    const dl=$("k-dlq"); dl.innerHTML=""; S.dlq.forEach(r=>dl.appendChild(chip(r.id||r.account,r.account,r.poison?"poison":"",r.reason)));
    $("k-dlq-n").textContent=S.dlq.length;
    const totalLag=S.parts.reduce((s,pt,i)=>s+pt.length-S.committed[i],0);
    $("k-stats").innerHTML=`<span>partner entries <b>${S.source.size}</b></span><span>ledger rows <b>${S.rows.length}</b></span><span>total lag <b>${totalLag}</b></span><span>DLQ <b>${S.dlq.length}</b></span>`;
  }
  reset();
  makeLoop(step,$("k-speed"),$("k-play"));
})();
