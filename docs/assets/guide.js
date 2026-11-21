/* Guided chapter engine: analogy -> predict -> watch step by step -> why -> under the hood -> code -> docs.
   Chapters are plain data (see rabbit-chapters.js / kafka-chapters.js). */

const GLOSSARY = {
  "producer": "The application that sends messages. Here, the partner system sending charges and payments.",
  "consumer": "The application that receives messages and does the work. Here, the service that writes to the ledger.",
  "broker": "The server in the middle (RabbitMQ or Kafka) that receives, stores and hands out messages.",
  "exchange": "RabbitMQ only. Receives messages from producers and routes them to queues using bindings. It never stores messages.",
  "binding": "RabbitMQ only. A rule that links an exchange to a queue, usually with a routing key.",
  "routing key": "RabbitMQ only. A label the producer puts on a message; the exchange uses it to pick queues.",
  "queue": "RabbitMQ only. Stores messages in order until a consumer takes them and acknowledges them.",
  "ack": "Acknowledgement. The consumer tells the broker 'done, you can forget this message'.",
  "nack": "Negative acknowledgement. The consumer tells the broker it failed; the message can be requeued or dead-lettered.",
  "unacked": "Delivered to a consumer but not yet acknowledged. The broker keeps it in case the consumer dies.",
  "prefetch": "RabbitMQ only. The maximum number of unacked messages a consumer can hold at once.",
  "durable": "A queue or exchange that survives a broker restart.",
  "persistent": "A message the broker writes to disk, so it survives a restart (in a durable queue).",
  "publisher confirm": "RabbitMQ only. The broker tells the producer it has safely taken responsibility for a message.",
  "mandatory": "RabbitMQ only. A publish flag: if no queue matches, the broker returns the message instead of dropping it.",
  "TTL": "Time to live. How long a message may wait in a queue before it expires.",
  "DLX": "Dead letter exchange. Where RabbitMQ sends messages that expire, are rejected, or exceed a delivery limit.",
  "DLQ": "Dead letter queue (or topic). A parking place for messages that could not be processed, kept for inspection and replay.",
  "poison message": "A message that can never succeed (bad data). Retrying it only wastes capacity.",
  "redrive": "Moving messages from the DLQ back to the main flow after the cause is fixed, usually gradually.",
  "idempotent": "Doing it twice has the same effect as doing it once. The key property that makes duplicates harmless.",
  "idempotency key": "A unique ID that identifies one business event, used to detect and ignore duplicates. Here, the partner's external_id.",
  "at-least-once": "A delivery guarantee: nothing is lost, but the same message may arrive more than once.",
  "topic": "Kafka only. A named stream of records, split into partitions.",
  "partition": "Kafka only. An ordered, append-only log inside a topic. Order is guaranteed only within one partition.",
  "offset": "Kafka only. The position of a record inside a partition: 0, 1, 2...",
  "commit": "Kafka only. Saving a consumer group's position (the next offset to read) so it can resume after a restart.",
  "consumer group": "Kafka only. A set of consumers that share a topic's partitions; each partition goes to one member.",
  "rebalance": "Kafka only. Reassigning partitions when consumers join, leave or die.",
  "lag": "Kafka only. How far behind a consumer group is: last offset in the partition minus the committed offset.",
  "retention": "Kafka only. How long records are kept, whether or not anyone has read them.",
  "compaction": "Kafka only. A cleanup mode that keeps only the latest record for each key.",
  "key": "Kafka only. A value attached to a record (here, the account ID). Same key, same partition, same order.",
  "leader": "Kafka only. The broker that handles writes for a partition. Followers copy from it.",
  "ISR": "In-sync replicas. The copies of a partition that are fully caught up with the leader.",
  "acks": "Kafka producer setting: how many replicas must confirm a write before the producer counts it as sent.",
  "reconciliation": "Comparing source and destination (counts, keys, totals) to prove nothing was lost or double-counted.",
  "correlation ID": "An ID carried through every log line of one message's journey, so you can trace it end to end."
};

function esc(s){return String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));}

/* {{term}} or {{term|shown text}} -> glossary tooltip */
function t(html){
  return String(html).replace(/\{\{([^}|]+)(?:\|([^}]+))?\}\}/g,(m,term,shown)=>{
    const key=Object.keys(GLOSSARY).find(k=>k.toLowerCase()===term.toLowerCase());
    return key ? `<span class="term" tabindex="0" data-term="${esc(key)}">${shown||term}</span>` : (shown||term);
  });
}

/* tooltip */
(function(){
  const tip=document.createElement("div"); tip.className="tip"; tip.setAttribute("role","tooltip");
  document.addEventListener("DOMContentLoaded",()=>document.body.appendChild(tip));
  function show(el){
    const k=el.dataset.term; tip.innerHTML=`<b>${esc(k)}</b>${esc(GLOSSARY[k])}`; tip.style.display="block";
    const r=el.getBoundingClientRect(), w=Math.min(300, window.innerWidth-24);
    tip.style.maxWidth=w+"px";
    let left=r.left+window.scrollX; left=Math.min(left, window.scrollX+window.innerWidth-w-12);
    tip.style.left=Math.max(12,left)+"px"; tip.style.top=(r.bottom+window.scrollY+6)+"px";
  }
  const hide=()=>{tip.style.display="none";};
  document.addEventListener("mouseover",e=>{const el=e.target.closest(".term"); if(el) show(el);});
  document.addEventListener("mouseout",e=>{if(e.target.closest(".term")) hide();});
  document.addEventListener("focusin",e=>{const el=e.target.closest(".term"); if(el) show(el);});
  document.addEventListener("focusout",hide);
  document.addEventListener("click",e=>{const el=e.target.closest(".term"); if(el){show(el);} else hide();});
})();

function store(key,val){ try{ if(val===undefined) return JSON.parse(localStorage.getItem(key)||"null"); localStorage.setItem(key,JSON.stringify(val)); }catch(e){ return null; } }

/* item syntax in step.set:
   "e1"  chip      "e1*" redelivered chip   "e1!" poison chip   "e1^" duplicate chip
   "=text" plain line   "=+text" good line   "=!text" bad line   "=~text" warning line */
function renderItems(items){
  return (items||[]).map(it=>{
    if(it.startsWith("=+")) return `<div class="line okline">${t(it.slice(2))}</div>`;
    if(it.startsWith("=!")) return `<div class="line badline">${t(it.slice(2))}</div>`;
    if(it.startsWith("=~")) return `<div class="line warnline">${t(it.slice(2))}</div>`;
    if(it.startsWith("=")) return `<div class="line">${t(it.slice(1))}</div>`;
    let cls="", label=it;
    if(it.endsWith("*")){cls="redel";label=it.slice(0,-1);}
    else if(it.endsWith("!")){cls="poison";label=it.slice(0,-1);}
    else if(it.endsWith("^")){cls="dup";label=it.slice(0,-1);}
    return `<span class="chip ${cls}">${esc(label)}</span>`;
  }).join("");
}

function renderPath(P){
  const side=document.getElementById("side"), main=document.getElementById("chapter");
  const seen=new Set(store("seen-"+P.key)||[]);

  function sideList(cur){
    side.innerHTML=`<h2>${esc(P.title)}</h2><ol>${P.chapters.map((c,i)=>
      `<li><a href="#${i+1}" class="${i===cur?"on":""} ${seen.has(i)?"seen":""}"><span class="n">${seen.has(i)&&i!==cur?"✓":i+1}</span><span>${esc(c.title)}</span></a></li>`).join("")}</ol>
      <p class="note">Each chapter: an analogy, a question to predict, a step-by-step animation, then the details.</p>
      <p class="note"><a href="#glossary">Glossary</a> · <a href="sandbox.html">Sandbox</a></p>`;
  }

  function show(i){
    const c=P.chapters[i]; seen.add(i); store("seen-"+P.key,[...seen]); sideList(i);
    main.innerHTML=`
      <article class="chapter">
        <div class="kicker">${esc(P.title)} · chapter ${i+1} of ${P.chapters.length}</div>
        <h1>${esc(c.title)}</h1>
        <p class="lead">${t(c.lead)}</p>

        <div class="block"><h3><span class="num">1</span>Picture it</h3><div class="analogy">${t(c.analogy)}</div></div>

        <div class="block"><h3><span class="num">2</span>Predict</h3>
          <div class="quiz" id="quiz"><div class="q">${t(c.question.q)}</div>
            <div class="opts">${c.question.options.map((o,k)=>`<button class="opt" data-k="${k}">${t(o)}</button>`).join("")}</div>
            <div class="why">${t(c.question.why)}</div></div></div>

        <div class="block"><h3><span class="num">3</span>Watch it happen</h3>
          <div class="stage locked" id="stage">
            <div class="nodes" id="nodes" style="${c.cols?`grid-template-columns:repeat(${c.cols},minmax(0,1fr))`:""}"></div>
            <div class="narr" id="narr"></div>
            <div class="player">
              <button class="btn" id="prev">Back</button>
              <button class="btn primary" id="next">Next step</button>
              <button class="btn" id="restart">Restart</button>
              <div class="dots" id="dots"></div>
            </div>
          </div>
          <p class="note" id="lockmsg">Answer the question above to unlock the animation.</p>
        </div>

        <div class="block"><h3><span class="num">4</span>Why it matters</h3><div class="card">${t(c.why)}</div></div>

        <div class="block"><h3><span class="num">5</span>Say it in an interview</h3><div class="say" style="margin-top:0"><b>One sentence</b>${t(c.say)}</div></div>

        <div class="block"><h3><span class="num">6</span>Under the hood</h3>
          <details class="hood"><summary>For experienced readers: settings, edge cases and trade-offs</summary>
            <ul>${c.hood.map(h=>`<li>${t(h)}</li>`).join("")}</ul></details></div>

        <div class="block"><h3><span class="num">7</span>In the lab code</h3>
          <div class="card"><div class="codehead"><span>${esc(c.code.file)}</span><a href="${P.repo}/${c.code.file}" target="_blank" rel="noopener">Open on GitHub</a></div>
          <pre>${esc(c.code.snippet)}</pre></div></div>

        <div class="block"><h3><span class="num">8</span>Official documentation</h3>
          <p class="note" style="margin-top:0">Where this chapter's behavior is defined, in the project's own docs.</p>
          <div class="docs">${c.docs.map(d=>`<a href="${d.url}" target="_blank" rel="noopener">${esc(d.label)} ↗</a>`).join("")}</div></div>

        <div class="chapnav">
          ${i>0?`<a class="btn" href="#${i}">← ${esc(P.chapters[i-1].title)}</a>`:"<span></span>"}
          ${i<P.chapters.length-1?`<a class="btn primary" href="#${i+2}">Next: ${esc(P.chapters[i+1].title)} →</a>`:`<a class="btn primary" href="sandbox.html">Try the sandbox →</a>`}
        </div>
      </article>`;
    window.scrollTo({top:0});
    wireQuiz(c); wireStage(c);
  }

  function wireQuiz(c){
    const quiz=document.getElementById("quiz");
    quiz.querySelectorAll(".opt").forEach(b=>b.onclick=()=>{
      if(quiz.classList.contains("answered")) return;
      const k=+b.dataset.k; quiz.classList.add("answered");
      quiz.querySelectorAll(".opt").forEach(x=>{ if(+x.dataset.k===c.question.answer) x.classList.add("right"); });
      if(k!==c.question.answer) b.classList.add("wrong");
      quiz.querySelector(".why").insertAdjacentHTML("afterbegin", k===c.question.answer?"<b>Right.</b> ":"<b>Not quite.</b> ");
      document.getElementById("stage").classList.remove("locked");
      document.getElementById("lockmsg").style.display="none";
    });
  }

  function wireStage(c){
    const nodesEl=document.getElementById("nodes"), narr=document.getElementById("narr"), stage=document.getElementById("stage");
    let cur=0, busy=false;
    nodesEl.innerHTML=c.nodes.map(n=>`<div class="node k-${n.kind||"broker"}" id="n-${n.id}" ${n.span?`style="grid-column:span ${n.span}"`:""}>
      <div class="lbl">${t(n.label)}</div>${n.sub?`<div class="subl">${esc(n.sub)}</div>`:""}<div class="items"></div></div>`).join("");
    const dots=document.getElementById("dots");
    dots.innerHTML=c.steps.map(()=>"<span></span>").join("");

    function stateAt(i){
      const st={}; for(let j=0;j<=i;j++){ Object.assign(st, c.steps[j].set||{}); } return st;
    }
    function paint(i){
      const st=stateAt(i), s=c.steps[i];
      c.nodes.forEach(n=>{
        const el=document.getElementById("n-"+n.id);
        el.querySelector(".items").innerHTML=renderItems(st[n.id]);
        el.classList.toggle("hl",(s.hl||[]).includes(n.id));
        el.classList.toggle("boom",(s.boom||[]).includes(n.id));
      });
      narr.innerHTML=`<div class="stepno">Step ${i+1} of ${c.steps.length}</div>${t(s.say)}`;
      [...dots.children].forEach((d,k)=>d.classList.toggle("on",k<=i));
      document.getElementById("prev").disabled=i===0;
      document.getElementById("next").textContent=i===c.steps.length-1?"Done":"Next step";
      document.getElementById("next").disabled=i===c.steps.length-1;
    }
    function fly(move,done){
      const [from,to,label]=move, a=document.getElementById("n-"+from), b=document.getElementById("n-"+to);
      if(!a||!b||window.matchMedia("(prefers-reduced-motion: reduce)").matches){ done(); return; }
      const sr=stage.getBoundingClientRect(), ar=a.getBoundingClientRect(), br=b.getBoundingClientRect();
      const f=document.createElement("div"); f.className="flyer"; f.textContent=label||"msg";
      stage.appendChild(f);
      const x0=ar.left-sr.left+ar.width/2-f.offsetWidth/2, y0=ar.top-sr.top+ar.height/2-f.offsetHeight/2;
      const x1=br.left-sr.left+br.width/2-f.offsetWidth/2, y1=br.top-sr.top+br.height/2-f.offsetHeight/2;
      f.style.left=x0+"px"; f.style.top=y0+"px";
      requestAnimationFrame(()=>requestAnimationFrame(()=>{ f.style.transform=`translate(${x1-x0}px,${y1-y0}px)`; }));
      setTimeout(()=>{ f.remove(); done(); },750);
    }
    function go(i){
      if(busy) return;
      const step=c.steps[i];
      if(i===cur+1 && step.move){
        busy=true; fly(step.move,()=>{ busy=false; cur=i; paint(i); });
      } else { cur=i; paint(i); }
    }
    document.getElementById("next").onclick=()=>{ if(cur<c.steps.length-1) go(cur+1); };
    document.getElementById("prev").onclick=()=>{ if(cur>0) go(cur-1); };
    document.getElementById("restart").onclick=()=>go(0);
    paint(0);
  }

  function route(){
    const h=location.hash.slice(1);
    if(h==="glossary"){ document.getElementById("glossary").scrollIntoView(); return; }
    const i=Math.min(Math.max(parseInt(h||"1",10)-1,0),P.chapters.length-1);
    show(isNaN(i)?0:i);
  }
  window.addEventListener("hashchange",route);

  const g=document.getElementById("glossary");
  if(g){
    const keys=P.glossary||Object.keys(GLOSSARY);
    g.innerHTML=`<h2>Glossary</h2><div class="card gloss"><dl>${keys.map(k=>`<dt>${esc(k)}</dt><dd>${esc(GLOSSARY[k])}</dd>`).join("")}</dl></div>`;
  }
  route();
}
