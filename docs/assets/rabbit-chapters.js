const RMQ = "https://www.rabbitmq.com/docs/";

const RABBIT_PATH = {
  key: "rabbit",
  title: "The RabbitMQ path",
  repo: "https://github.com/rubenoliveros/messaging-reliability-labs/blob/main",
  glossary: ["producer","consumer","broker","exchange","binding","routing key","queue","ack","nack","unacked","prefetch","durable","persistent","publisher confirm","mandatory","TTL","DLX","DLQ","poison message","redrive","idempotent","idempotency key","at-least-once","reconciliation","correlation ID"],
  chapters: [

  /* ---------------- 1 ---------------- */
  {
    title: "How a message travels",
    lead: "Before anything can go wrong, let's follow one payment from the partner to our ledger, and meet every piece of RabbitMQ on the way.",
    analogy: "Think of a <b>post office</b>. The sender (the {{producer}}) drops a letter at the sorting desk (the {{exchange}}). The desk reads the label (the {{routing key}}) and puts the letter in the right mailbox (the {{queue}}). The recipient (the {{consumer}}) picks it up and signs for it (the {{ack}}). Only after the signature does the post office throw away its copy.",
    question: {
      q: "The producer publishes a message, but no queue is bound to the exchange for that routing key. What happens to the message?",
      options: ["The exchange keeps it until a matching queue appears", "It is silently dropped", "The broker always rejects the publish with an error"],
      answer: 1,
      why: "Exchanges route, they don't store. An unroutable message is dropped unless the publisher sets the {{mandatory}} flag (the broker sends it back) or the exchange has an alternate exchange. That's why the lab publishes with <code>mandatory=True</code>: a missing binding fails loudly instead of losing money quietly."
    },
    nodes: [
      {id:"prod", label:"producer", sub:"partner system", kind:"actor"},
      {id:"ex", label:"exchange", sub:"ledger (direct)"},
      {id:"q", label:"queue", sub:"ledger.entries"},
      {id:"cons", label:"consumer", sub:"ledger writer", kind:"actor"},
      {id:"db", label:"database", sub:"Postgres ledger", kind:"store"}
    ],
    steps: [
      {say:"The partner has a payment to send: <b>e1</b>, a $120 charge for acct-1.", set:{prod:["e1"]}, hl:["prod"]},
      {say:"It publishes e1 to the {{exchange}} with routing key <code>entry</code>. Producers never write to a queue directly.", move:["prod","ex","e1"], set:{prod:[], ex:["e1"]}, hl:["ex"]},
      {say:"The exchange checks its {{binding|bindings}}: <code>ledger.entries</code> is bound with key <code>entry</code>, so the message goes there. The queue is {{durable}} and the message is {{persistent}}, so it is written to disk.", move:["ex","q","e1"], set:{ex:[], q:["e1"]}, hl:["q"]},
      {say:"With {{publisher confirm|publisher confirms}} on, the broker tells the producer it has taken responsibility for e1. Now the producer can stop worrying about it.", set:{prod:["=+confirm received"]}, hl:["prod","q"]},
      {say:"The broker delivers e1 to the consumer. It is <b>not deleted yet</b>: it is {{unacked}}, reserved for this consumer in case it dies.", move:["q","cons","e1"], set:{q:["=~e1 (unacked)"], cons:["e1"]}, hl:["cons"]},
      {say:"The consumer writes the charge to the ledger and the database transaction commits.", move:["cons","db","e1"], set:{db:["=e1 · acct-1 · charge $120"]}, hl:["db"]},
      {say:"Only now does the consumer send the {{ack}}. The broker deletes e1. One message, delivered and recorded.", set:{q:[], cons:["=+ack sent"]}, hl:["q","cons"]}
    ],
    why: "Every reliability feature you'll see later hooks into one of these hops: the publish (confirms, mandatory), the queue (durability), the delivery (acks, prefetch) and the write (idempotency). When something is lost or duplicated, the first question is always: <b>at which hop?</b>",
    say: "Producers publish to exchanges, bindings route to queues, and I use durable queues, persistent messages, publisher confirms and the mandatory flag so nothing disappears silently.",
    hood: [
      "Exchange types: <code>direct</code> (exact routing key), <code>topic</code> (pattern match), <code>fanout</code> (every bound queue) and <code>headers</code>. The default exchange routes by queue name, which is what the lab uses to publish into retry queues.",
      "Durability is two settings: a {{durable}} queue survives a restart, but its messages only survive if they were published as {{persistent}} (delivery mode 2).",
      "Publisher confirms are asynchronous in high-throughput producers: you track outstanding sequence numbers and republish on nack or timeout, which can itself create duplicates.",
      "Instead of <code>mandatory</code>, an alternate exchange catches unroutable messages server-side, which is useful when you don't control every publisher."
    ],
    code: {file:"rabbitmq-lab/producer.py", snippet:
`ch.confirm_delivery()   # publisher confirms
ch.basic_publish(
    exchange=EXCHANGE, routing_key=ROUTING_KEY,
    body=json.dumps(event).encode(),
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        correlation_id=correlation_id,
        message_id=event.get("external_id"),
    ),
    mandatory=True,   # fail loudly if unroutable
)`},
    docs: [
      {label:"Exchanges", url:RMQ+"exchanges"},
      {label:"Queues", url:RMQ+"queues"},
      {label:"Publisher confirms", url:RMQ+"confirms#publisher-confirms"},
      {label:"Unroutable messages", url:RMQ+"publishers#unroutable"}
    ]
  },

  /* ---------------- 2 ---------------- */
  {
    title: "Acknowledgements: ack after the write",
    lead: "The {{ack}} is the consumer's signature. When you sign decides whether a crash loses money or merely repeats work.",
    analogy: "A courier hands you a package. If you sign <b>before</b> checking it and then drop it in a river, the courier's records say it was delivered, and it's gone. If you sign <b>after</b> putting it safely inside, the worst case is that the courier, unsure, brings a second copy tomorrow.",
    question: {
      q: "The consumer saves e1 to the database, then crashes <b>before</b> sending the ack. What happens to e1?",
      options: ["It's lost, because the consumer already took it", "RabbitMQ delivers it again", "It stays stuck in the queue forever"],
      answer: 1,
      why: "Unacked deliveries return to the queue when the consumer's channel or connection closes, and RabbitMQ marks them <code>redelivered</code>. So the database write can happen twice. That is the price of safety, and the next chapter shows how to pay it."
    },
    nodes: [
      {id:"q", label:"queue", sub:"ledger.entries"},
      {id:"cons", label:"consumer", kind:"actor"},
      {id:"db", label:"database", kind:"store"}
    ],
    steps: [
      {say:"e1 is waiting in the queue.", set:{q:["e1"], cons:[], db:[]}, hl:["q"]},
      {say:"RabbitMQ delivers e1. It stays in the queue as {{unacked}}.", move:["q","cons","e1"], set:{q:["=~e1 (unacked)"], cons:["e1"]}, hl:["cons"]},
      {say:"The consumer writes e1 and the transaction commits.", move:["cons","db","e1"], set:{db:["=e1 · $120"]}, hl:["db"]},
      {say:"Before it can send the ack, the consumer process crashes.", set:{cons:["=!crashed before ack"]}, boom:["cons"]},
      {say:"The connection drops. RabbitMQ sees e1 was never acked and puts it back in the queue with <code>redelivered=true</code> (dashed outline).", set:{q:["e1*"], cons:["=restarting..."]}, hl:["q"]},
      {say:"The consumer restarts and receives e1 again.", move:["q","cons","e1"], set:{q:["=~e1 (unacked)"], cons:["e1*"]}, hl:["cons"]},
      {say:"It writes e1 again. With a plain INSERT, the ledger now has the same $120 charge <b>twice</b>. Nothing was lost, but money was double-counted.", move:["cons","db","e1"], set:{db:["=e1 · $120","=!e1 · $120 (again)"], cons:["=ack sent"], q:[]}, boom:["db"]}
    ],
    why: "Ack <b>after</b> the commit, and a crash can only cause a duplicate. Ack <b>before</b> the commit, and a crash between the two loses the message for good, because the broker already deleted it. Duplicates are fixable; lost payments are not. Never use automatic acks (<code>auto_ack=True</code>) for data that matters: the broker treats the message as done the moment it leaves.",
    say: "I ack after the database commit, never before. A crash can then only produce a duplicate, and duplicates I absorb with idempotent writes.",
    hood: [
      "Delivery tags are per channel. Acking on a different channel than the one that received the message is an error that closes the channel.",
      "<code>basic.nack</code> / <code>basic.reject</code> with <code>requeue=true</code> puts the message back (often at the head); with <code>requeue=false</code> it is dead-lettered if a {{DLX}} is configured, otherwise dropped.",
      "The <code>redelivered</code> flag is a hint, not proof of a previous successful write. Only the database can tell you whether the effect already happened.",
      "RabbitMQ enforces a delivery acknowledgement timeout: a consumer that holds an unacked delivery too long gets its channel closed and the message requeued. Long-running work needs a different pattern."
    ],
    code: {file:"rabbitmq-lab/consumer.py", snippet:
`with db.cursor() as cur:
    cur.execute(INSERT_SQL, {**event, "correlation_id": corr})
db.commit()                         # 1. the write is durable
ch.basic_ack(method.delivery_tag)   # 2. only then: "done"`},
    docs: [
      {label:"Consumer acknowledgements", url:RMQ+"confirms#consumer-acknowledgements"},
      {label:"Reliability guide", url:RMQ+"reliability"},
      {label:"Acknowledgement timeout", url:RMQ+"consumers#acknowledgement-timeout"}
    ]
  },

  /* ---------------- 3 ---------------- */
  {
    title: "Duplicates and idempotency",
    lead: "Duplicates are normal: redeliveries after crashes, and partners that resend when they time out. The consumer must make them harmless.",
    analogy: "A bank teller gets the same check twice. A good teller looks at the check number, sees it was already deposited, and politely does nothing. The check number is the {{idempotency key}}.",
    question: {
      q: "The partner times out waiting for our reply and resends payment e2 ($80). Both copies reach the consumer. With a plain <code>INSERT</code>, what is acct-2's balance?",
      options: ["$80", "$160", "The second insert fails and the consumer stops"],
      answer: 1,
      why: "A plain insert has no idea it saw e2 before, so it records $80 twice. With a <code>UNIQUE</code> constraint on the partner's <code>external_id</code>, the second insert becomes a no-op and the balance stays $80."
    },
    nodes: [
      {id:"q", label:"queue", sub:"ledger.entries"},
      {id:"cons", label:"consumer", kind:"actor"},
      {id:"db", label:"database", sub:"ledger_entries", kind:"store"}
    ],
    steps: [
      {say:"Two copies of e2 are in the queue: the original and the partner's resend (orange). Same <code>external_id</code>, same amount.", set:{q:["e2","e2^"], cons:[], db:[]}, hl:["q"]},
      {say:"The first copy is written and acked.", move:["q","cons","e2"], set:{q:["e2^"], cons:["=+stored, acked"], db:["=e2 · acct-2 · $80"]}, hl:["db"]},
      {say:"The resend arrives. With a plain INSERT, it is written again: acct-2 now shows <b>$160</b>. Wrong.", move:["q","cons","e2"], set:{q:[], cons:["=stored, acked"], db:["=e2 · acct-2 · $80","=!e2 · acct-2 · $80","=!balance acct-2: $160"]}, boom:["db"]},
      {say:"Now the fix. The table gets a <code>UNIQUE(external_id)</code> constraint. Let's replay the same two copies.", set:{q:["e2","e2^"], cons:[], db:["=UNIQUE(external_id)"]}, hl:["db"]},
      {say:"First copy: inserted.", move:["q","cons","e2"], set:{q:["e2^"], cons:["=+stored, acked"], db:["=UNIQUE(external_id)","=e2 · acct-2 · $80"]}, hl:["db"]},
      {say:"Second copy: <code>INSERT ... ON CONFLICT (external_id) DO NOTHING</code> inserts zero rows. The consumer still acks, because the work is already done.", move:["q","cons","e2"], set:{q:[], cons:["=+duplicate ignored, acked"], db:["=UNIQUE(external_id)","=e2 · acct-2 · $80","=+balance acct-2: $80"]}, hl:["db","cons"]}
    ],
    why: "Idempotency turns <b>at-least-once</b> delivery into <b>effectively once</b> processing. The key must come from the source (the partner's ID), not from us: if we generated a new ID per message, every duplicate would look new. And the check must live where the effect happens, in the database, inside the same transaction.",
    say: "The partner's external ID is a unique constraint, so a redelivery or a resend is a no-op. That's how at-least-once delivery becomes effectively-once processing.",
    hood: [
      "Dedup in memory or in a cache is a window, not a guarantee: it forgets on restart. The database constraint is the source of truth because it sits next to the effect.",
      "For updates (not inserts), use a version or timestamp: apply the change only if it is newer than what's stored.",
      "The <code>message_id</code> property can carry the idempotency key so it's visible in the management UI and logs.",
      "For side effects outside your database (sending an email, calling a partner API), record the key and the outcome first, and pass the key to the other side if it supports idempotency keys."
    ],
    code: {file:"rabbitmq-lab/consumer.py", snippet:
`INSERT INTO ledger_entries
  (external_id, account_id, kind, amount_cents, correlation_id)
VALUES (...)
ON CONFLICT (external_id) DO NOTHING

inserted = cur.rowcount == 1   # 0 means: duplicate, already done`},
    docs: [
      {label:"Reliability: at-least-once", url:RMQ+"reliability"},
      {label:"Acknowledgements and redelivery", url:RMQ+"confirms#consumer-acknowledgements"}
    ]
  },

  /* ---------------- 4 ---------------- */
  {
    title: "Prefetch, competing consumers and order",
    lead: "One consumer is slow. Add more and they share the queue. But sharing has a cost: order.",
    analogy: "A supermarket opens three checkouts for one line. Everyone gets out faster, but the person who was fifth in line may leave before the person who was second. {{prefetch|Prefetch}} is how many baskets each cashier may have on the belt at once.",
    question: {
      q: "Two consumers share one queue holding e1, e2, e3, e4 in that order. Will the ledger receive them in that order?",
      options: ["Yes, queues are first-in, first-out", "Not guaranteed", "Only if prefetch is 1"],
      answer: 1,
      why: "The queue hands messages out in order, but each consumer works at its own speed, and redeliveries put messages back. Even with prefetch 1, consumer 2 can finish e2 before consumer 1 finishes e1."
    },
    nodes: [
      {id:"q", label:"queue", sub:"ledger.entries"},
      {id:"c1", label:"consumer 1", sub:"prefetch = 1", kind:"actor"},
      {id:"c2", label:"consumer 2", sub:"prefetch = 1", kind:"actor"},
      {id:"db", label:"database", sub:"order of writes", kind:"store"}
    ],
    steps: [
      {say:"Four messages, in order.", set:{q:["e1","e2","e3","e4"], c1:[], c2:[], db:[]}, hl:["q"]},
      {say:"With prefetch 1, each consumer gets one message at a time: e1 to consumer 1, e2 to consumer 2.", set:{q:["e3","e4","=~e1, e2 unacked"], c1:["e1"], c2:["e2"]}, hl:["c1","c2"]},
      {say:"Consumer 1 is slow (a long database call). Consumer 2 finishes first: <b>e2 is written before e1</b>.", move:["c2","db","e2"], set:{db:["=1. e2"], c2:["=+acked"]}, hl:["db"]},
      {say:"Consumer 2 acks and immediately gets e3, and finishes it too.", move:["c2","db","e3"], set:{q:["e4","=~e1 unacked"], c2:["=+acked"], db:["=1. e2","=2. e3"]}, hl:["c2","db"]},
      {say:"Consumer 1 finally writes e1. The ledger received e2, e3, e1. Out of order.", move:["c1","db","e1"], set:{c1:["=+acked"], db:["=1. e2","=2. e3","=~3. e1"]}, hl:["db"]},
      {say:"Our ledger doesn't care: a balance is a sum, and sums don't depend on order. Data that <b>does</b> depend on order (like a status) needs a version check, or a single active consumer.", set:{db:["=1. e2","=2. e3","=~3. e1","=+balances still correct"]}, hl:["db"]}
    ],
    why: "Parallel consumers buy throughput by giving up strict order. The robust answer is to design data so order doesn't matter (append-only entries, sums) or to make it explicit (versions). Prefetch controls back-pressure: too high and one slow consumer hoards messages others could process; too low and consumers wait on network round trips.",
    say: "Competing consumers give throughput but not global order, so I design for order-independence, or use versions or a single active consumer when order matters, and I tune prefetch for back-pressure.",
    hood: [
      "Prefetch (<code>basic.qos</code>) limits unacked deliveries; a common starting point is tens, then tune by measuring consumer utilization.",
      "Single Active Consumer lets several consumers attach to a queue while only one receives messages, with automatic failover: order with high availability, not throughput.",
      "To scale while keeping per-key order, split the stream into several queues by key (for example with the consistent hash exchange plugin), with one consumer per queue.",
      "A message requeued after a nack or a crash can jump ahead of messages behind it, another reason not to rely on order with retries."
    ],
    code: {file:"rabbitmq-lab/consumer.py", snippet:
`ch.basic_qos(prefetch_count=args.prefetch)   # back-pressure
ch.basic_consume(MAIN_Q, on_message_callback=on_message,
                 auto_ack=False)              # manual acks`},
    docs: [
      {label:"Consumer prefetch", url:RMQ+"consumer-prefetch"},
      {label:"Consumers (incl. single active consumer)", url:RMQ+"consumers"},
      {label:"Queues and ordering", url:RMQ+"queues"}
    ]
  },

  /* ---------------- 5 ---------------- */
  {
    title: "Retries with delay queues",
    lead: "The database times out. The message is fine; the moment is bad. Retry, but not right away.",
    analogy: "You call a busy office and get no answer. Redialing every second just jams the line. You call back in 5 minutes, then 20, then an hour. A delay queue is that callback timer.",
    question: {
      q: "Writing e5 fails because the database timed out. What's the best move?",
      options: ["Nack with requeue, so it's retried immediately", "Park it in a delay queue and retry later", "Drop it and log the error"],
      answer: 1,
      why: "An immediate requeue creates a hot loop that hammers a system that is already struggling, and blocks other messages. A delay gives the downstream time to recover. Dropping it loses a payment."
    },
    nodes: [
      {id:"q", label:"main queue", sub:"ledger.entries"},
      {id:"cons", label:"consumer", kind:"actor"},
      {id:"r", label:"delay queue", sub:"retry.5s · TTL 5s · DLX → ledger"},
      {id:"db", label:"database", kind:"store"}
    ],
    steps: [
      {say:"The consumer takes e5 and tries to write it. The database times out.", move:["q","cons","e5"], set:{q:[], cons:["e5"], r:[], db:["=!timeout"]}, boom:["db"]},
      {say:"This is a <b>transient</b> error, so the consumer publishes a copy of e5 to <code>retry.5s</code> with header <code>x-attempt: 2</code>, then acks the original.", move:["cons","r","e5"], set:{cons:["=~retry scheduled"], r:["e5 (attempt 2)"]}, hl:["r"]},
      {say:"The delay queue has <b>no consumers</b>. It only has a {{TTL}} of 5 seconds and a {{DLX|dead letter exchange}} pointing back to the main exchange.", set:{r:["e5 (attempt 2)","=waiting 5s..."]}, hl:["r"]},
      {say:"The TTL expires. RabbitMQ dead-letters e5 back to the main queue and adds an <code>x-death</code> header recording why and how many times.", move:["r","q","e5"], set:{r:[], q:["e5*"], db:[]}, hl:["q"]},
      {say:"The database has recovered. e5 is written and acked.", move:["q","cons","e5"], set:{q:[], cons:["=+stored, acked"], db:["=e5 · $45"]}, hl:["db"]},
      {say:"If it had failed again, the next stop would be <code>retry.20s</code>, then <code>retry.60s</code>. After the last tier: the {{DLQ}}, next chapter.", set:{r:["=tiers: 5s → 20s → 60s → DLQ"]}, hl:["r"]}
    ],
    why: "Retry only what can succeed later (timeouts, 5xx, a database blip), with growing delays and a cap. One queue per delay matters: with a per-message TTL, RabbitMQ only expires messages when they reach the head of the queue, so a long delay at the front would block shorter ones behind it.",
    say: "Transient failures go through delay queues built from TTL and a dead letter exchange, with growing delays and a cap; publish-then-ack means the worst case is a duplicate, which idempotency absorbs.",
    hood: [
      "Order of operations: publish the retry copy first, then ack the original. A crash in between creates a duplicate (absorbed), never a loss.",
      "The <code>x-death</code> header counts dead-lettering events per queue and reason; you can use it instead of a custom attempt header.",
      "Add jitter to retry delays when many messages fail at once, so they don't all come back in the same second.",
      "The community delayed-message exchange plugin is an alternative to TTL tiers; it trades simplicity for a plugin dependency and different durability characteristics."
    ],
    code: {file:"rabbitmq-lab/common.py", snippet:
`for delay in RETRY_TIERS_MS:          # 5s, 20s, 60s
    ch.queue_declare(
        retry_queue_name(delay), durable=True,
        arguments={
            "x-message-ttl": delay,
            "x-dead-letter-exchange": EXCHANGE,
            "x-dead-letter-routing-key": ROUTING_KEY,
        })`},
    docs: [
      {label:"Time-to-live (TTL)", url:RMQ+"ttl"},
      {label:"Dead letter exchanges", url:RMQ+"dlx"}
    ]
  },

  /* ---------------- 6 ---------------- */
  {
    title: "Poison messages, the DLQ and redrive",
    lead: "Some messages will never succeed. Others fail too many times. Both need a safe place to wait for a human, and a way back.",
    analogy: "A letter with an impossible address goes to the dead letter office, not back on the truck every morning. Someone opens the dead letter office, fixes what can be fixed, and sends those letters out again, a few at a time.",
    question: {
      q: "Event e9 arrives with an amount of <b>-5</b>. What should the consumer do?",
      options: ["Retry it with backoff like any failure", "Send it straight to the DLQ with the reason", "Correct the amount to 5 and save it"],
      answer: 1,
      why: "Bad data won't get better with time, so retrying only burns capacity. Silently 'fixing' it is worse: it invents financial data. It goes to the {{DLQ}} with a reason, and the partner corrects it at the source."
    },
    nodes: [
      {id:"q", label:"main queue"},
      {id:"cons", label:"consumer", kind:"actor"},
      {id:"dlq", label:"DLQ", sub:"ledger.entries.dlq", kind:"bad"},
      {id:"db", label:"database", kind:"store"}
    ],
    steps: [
      {say:"e9 (amount -5) and e10 (valid) are waiting.", set:{q:["e9!","e10"], cons:[], dlq:[], db:[]}, hl:["q"]},
      {say:"Validation fails for e9. It's a {{poison message}}: straight to the DLQ with <code>x-last-error: poison: invalid amount</code>, then acked.", move:["q","dlq","e9"], set:{q:["e10"], dlq:["e9!"], cons:["=!poison → DLQ"]}, hl:["dlq"]},
      {say:"e10 is valid, but the database is down. It fails on every retry tier and, after 4 attempts, lands in the DLQ with reason <code>max attempts</code>.", move:["q","dlq","e10"], set:{q:[], dlq:["e9!","e10"], cons:["=!max attempts → DLQ"], db:["=!down"]}, boom:["db"]},
      {say:"An alert fires on DLQ depth. An operator inspects first, without consuming: <i>look before you touch</i>.", set:{dlq:["e9! (poison)","e10 (max attempts)"]}, hl:["dlq"]},
      {say:"The database is fixed. {{redrive|Redrive}} sends e10 back to the main queue, at a limited rate, skipping poison. e9 stays for the partner to correct.", move:["dlq","q","e10"], set:{dlq:["e9!"], q:["e10*"], db:[]}, hl:["q"]},
      {say:"e10 is written. Then {{reconciliation}} compares the partner's list with the ledger: everything valid is present, and e9 is a known, explained exception.", move:["q","db","e10"], set:{q:[], cons:["=+stored, acked"], db:["=e10 · $300","=+reconciled: 1 known exception (e9)"]}, hl:["db"]}
    ],
    why: "The DLQ turns silent failures into a visible inbox. Separating poison (fix the data) from exhausted retries (fix the system, then redrive) tells you who has to act. Redrive gradually, so a backlog doesn't knock over the system you just fixed, and finish with reconciliation as proof.",
    say: "Poison goes straight to the DLQ with the reason; exhausted retries go there too. After the fix I redrive gradually, skip poison, and reconcile to prove nothing is missing.",
    hood: [
      "Quorum queues track deliveries per message and support a delivery limit: messages redelivered too many times are dead-lettered or dropped, which handles poison messages that crash the consumer before it can even decide.",
      "Alert on DLQ depth and on its growth rate, not only on errors in logs.",
      "Keep the original headers and the failure reason on the DLQ copy; redrive should reset the attempt counter but keep the correlation ID.",
      "A DLQ with no owner becomes a graveyard. Decide who reviews it and how often."
    ],
    code: {file:"rabbitmq-lab/redrive.py", snippet:
`if args.skip_poison and reason.startswith("poison"):
    kept += 1          # leave it for a human
    continue
ch.basic_publish(EXCHANGE, ROUTING_KEY, body, props)
ch.basic_ack(method.delivery_tag)   # remove only after confirm
time.sleep(1 / args.rate)           # gradual`},
    docs: [
      {label:"Dead letter exchanges", url:RMQ+"dlx"},
      {label:"Quorum queues: poison message handling", url:RMQ+"quorum-queues#poison-message-handling"}
    ]
  }
  ]
};
