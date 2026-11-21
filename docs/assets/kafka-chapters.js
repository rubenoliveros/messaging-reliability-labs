const KD = "https://kafka.apache.org/44/";

const KAFKA_PATH = {
  key: "kafka",
  title: "The Kafka path",
  repo: "https://github.com/rubenoliveros/messaging-reliability-labs/blob/main",
  glossary: ["producer","consumer","broker","topic","partition","offset","key","commit","consumer group","rebalance","lag","retention","compaction","leader","ISR","acks","idempotent","idempotency key","at-least-once","DLQ","poison message","redrive","reconciliation","correlation ID"],
  chapters: [

  /* ---------------- 1 ---------------- */
  {
    title: "A log, not a queue",
    lead: "The single idea that explains most of Kafka: records are appended to a log and stay there. Readers just move a bookmark.",
    analogy: "Picture a <b>numbered notebook</b>. Writers only add lines at the end. Each reader keeps their own bookmark (the {{offset}} they'll read next). Reading a line doesn't erase it; another reader can read the same line later. Old pages are torn out only by a rule like 'keep 7 days' ({{retention}}).",
    question: {
      q: "Consumer group A reads record e1 and commits. Is e1 deleted from the topic?",
      options: ["Yes, once it's read it's gone", "No, it stays until retention removes it", "Only after every consumer group has read it"],
      answer: 1,
      why: "Kafka doesn't track who read what to decide deletion. Records stay until {{retention}} (time or size) or {{compaction}} removes them. Each {{consumer group}} only stores its own position. That's what makes replay possible."
    },
    nodes: [
      {id:"prod", label:"producer", kind:"actor"},
      {id:"p0", label:"partition 0", sub:"topic ledger.events", span:2},
      {id:"ga", label:"group A", sub:"ledger-writer", kind:"actor"},
      {id:"gb", label:"group B", sub:"analytics", kind:"actor"}
    ],
    steps: [
      {say:"The producer has two records: e1 and e2.", set:{prod:["e1","e2"], p0:[], ga:[], gb:[]}, hl:["prod"]},
      {say:"They're appended to the end of the {{partition}}. Each gets the next {{offset}}: e1 is offset 0, e2 is offset 1.", move:["prod","p0","e1, e2"], set:{prod:[], p0:["0: e1","1: e2"]}, hl:["p0"]},
      {say:"Group A starts reading at offset 0 and processes e1.", move:["p0","ga","e1"], set:{ga:["=reading offset 0: e1"]}, hl:["ga"]},
      {say:"Group A {{commit|commits}} offset <b>1</b>: the <i>next</i> offset to read. That's its bookmark, stored by Kafka. e1 is still in the log.", set:{ga:["=+committed: 1"], p0:["0: e1","1: e2"]}, hl:["ga","p0"]},
      {say:"Group B is new. With <code>auto.offset.reset=earliest</code> it starts at offset 0 and reads e1 too, independently.", move:["p0","gb","e1"], set:{gb:["=reading offset 0: e1","=committed: 0"]}, hl:["gb"]},
      {say:"Each group has its own position. The records stay until retention (for example 7 days) removes them, whether anyone read them or not.", set:{p0:["0: e1","1: e2","=retention: 7 days"]}, hl:["p0"]}
    ],
    why: "In RabbitMQ a delivered-and-acked message is gone. In Kafka, reading changes nothing in the log: only the reader's offset moves. That's why many independent systems can read the same stream, and why you can rewind to reprocess. It also means the consumer, not the broker, is responsible for remembering where it is.",
    say: "Kafka is a partitioned, replicated log: records stay for the retention period, and each consumer group tracks its own committed offset, which is what makes replay and multiple independent consumers possible.",
    hood: [
      "Committed offsets live in an internal topic (<code>__consumer_offsets</code>). The committed value is the next offset to read, not the last one processed.",
      "<code>auto.offset.reset</code> only applies when a group has no committed offset (or it's out of range): <code>earliest</code> or <code>latest</code>.",
      "Retention is per topic, by time (<code>retention.ms</code>) or size (<code>retention.bytes</code>). Your replay window is exactly your retention.",
      "Offsets are per partition. There is no single offset for a whole topic."
    ],
    code: {file:"kafka-lab/consumer.py", snippet:
`c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": args.group,
    "enable.auto.commit": False,      # we decide when it's done
    "auto.offset.reset": "earliest",
})`},
    docs: [
      {label:"Main concepts and terminology", url:KD+"getting-started/introduction/#main-concepts-and-terminology"},
      {label:"Consumer position", url:KD+"design/design/#consumer-position"}
    ]
  },

  /* ---------------- 2 ---------------- */
  {
    title: "Keys, partitions and order",
    lead: "Kafka scales by splitting a topic into partitions. The price: order exists only inside a partition. The {{key}} decides where each record goes.",
    analogy: "A bank with three tellers. If customers line up anywhere, two deposits from the same person can be handled out of order. If each customer always goes to the same teller (by last name), their own transactions stay in order, even though the three tellers work in parallel.",
    question: {
      q: "acct-1 sends a charge, then a payment, with <b>no key</b>. Are they guaranteed to be processed in that order?",
      options: ["Yes, they're in the same topic", "No, they can land in different partitions", "Yes, Kafka orders by timestamp"],
      answer: 1,
      why: "Without a key, the producer spreads records across partitions, and there's no order between partitions. With the account ID as the key, both records hash to the same partition, where order is the offset order."
    },
    nodes: [
      {id:"prod", label:"producer", kind:"actor"},
      {id:"p0", label:"partition 0"},
      {id:"p1", label:"partition 1"},
      {id:"p2", label:"partition 2"}
    ],
    steps: [
      {say:"acct-1 has two events: a charge (c1) and then a payment (p1). First, sent <b>without a key</b>.", set:{prod:["c1","p1"], p0:[], p1:[], p2:[]}, hl:["prod"]},
      {say:"The charge lands in partition 0...", move:["prod","p0","c1"], set:{prod:["p1"], p0:["0: c1"]}, hl:["p0"]},
      {say:"...and the payment in partition 2. Two consumers may read partition 2 first: payment before charge. No ordering guarantee.", move:["prod","p2","p1"], set:{prod:[], p2:["0: p1"]}, boom:["p0","p2"]},
      {say:"Now with <code>key = \"acct-1\"</code>. The producer hashes the key and picks a partition: same key, always the same partition.", set:{prod:["c1 key=acct-1","p1 key=acct-1"], p0:[], p2:[]}, hl:["prod"]},
      {say:"Both go to partition 1, at offsets 0 and 1. Inside a partition, order is the offset order.", move:["prod","p1","acct-1"], set:{prod:[], p1:["0: c1","1: p1","=+order kept for acct-1"]}, hl:["p1"]},
      {say:"Different accounts may share a partition, and that's fine. What matters is that one account never splits across partitions.", set:{p1:["0: c1","1: p1","2: (acct-5)","=+order kept for acct-1"]}, hl:["p1"]}
    ],
    why: "Choose the key from the business rule that needs order. For a ledger, per-account order is what matters, so the account ID is the key. Global order across a busy topic would mean one partition and no parallelism; almost nobody actually needs it.",
    say: "Kafka only guarantees order within a partition, so I key by the entity that needs order, here the account ID, and don't design for global order.",
    hood: [
      "The default partitioner hashes the key (murmur2) modulo the partition count. Adding partitions later changes that mapping, so new records for a key can land in a different partition than its history.",
      "Hot keys (one account with huge volume) overload a single partition; key choice is also a load-balancing decision.",
      "Records without a key use a sticky strategy that fills a batch for one partition before switching, to improve batching.",
      "With idempotence disabled and several in-flight requests, a retry could reorder records even within a partition. The idempotent producer prevents that."
    ],
    code: {file:"kafka-lab/producer.py", snippet:
`p.produce(
    TOPIC,
    key=event["account_id"],      # same account → same partition
    value=json.dumps(event),
    headers=[("correlation_id", correlation_id)],
)`},
    docs: [
      {label:"Topics and partitions", url:KD+"getting-started/introduction/#main-concepts-and-terminology"}
    ]
  },

  /* ---------------- 3 ---------------- */
  {
    title: "Consumer groups and rebalancing",
    lead: "To process faster, start more consumers with the same group ID. Kafka splits the partitions between them, and reshuffles when one joins or dies.",
    analogy: "Three checkout lanes and a team of cashiers. Each open lane needs exactly one cashier. A fourth cashier has no lane and waits. If a cashier leaves, their lane is handed to someone else, who restarts from the last receipt that was filed.",
    question: {
      q: "A topic has 3 partitions. The group has 4 consumers. What does the 4th consumer do?",
      options: ["Shares a partition with another consumer", "Sits idle", "Reads every partition as a backup"],
      answer: 1,
      why: "Within a group, each partition is assigned to exactly one consumer. With more consumers than partitions, the extras get nothing. Partitions are the unit of parallelism, so you size them for your target number of consumers."
    },
    nodes: [
      {id:"c1", label:"consumer 1", kind:"actor"},
      {id:"c2", label:"consumer 2", kind:"actor"},
      {id:"c3", label:"consumer 3", kind:"actor"},
      {id:"c4", label:"consumer 4", kind:"actor"}
    ],
    steps: [
      {say:"One consumer in group <code>ledger-writer</code>. It owns all three partitions.", set:{c1:["p0","p1","p2"], c2:["=not started"], c3:["=not started"], c4:["=not started"]}, hl:["c1"]},
      {say:"Consumer 2 joins. A {{rebalance}} reassigns partitions: consumer 1 keeps p0 and p1, consumer 2 takes p2.", set:{c1:["p0","p1"], c2:["p2"]}, hl:["c1","c2"]},
      {say:"Consumer 3 joins. Another rebalance: one partition each.", set:{c1:["p0"], c2:["p2"], c3:["p1"]}, hl:["c3"]},
      {say:"Consumer 4 joins. There's no partition left: it sits idle.", set:{c4:["=~idle"]}, hl:["c4"]},
      {say:"Consumer 2 crashes. When the group notices (missed heartbeats), p2 is reassigned. The new owner starts from p2's <b>last committed offset</b>, so anything consumer 2 processed but didn't commit is read again.", set:{c2:["=!crashed"], c4:["p2","=~resumes from committed offset"]}, boom:["c2"]},
      {say:"Another trap: if processing between two polls takes longer than <code>max.poll.interval.ms</code>, the consumer is considered stuck and is removed from the group, even though the process is alive.", set:{c1:["p0","=~slow batch: must poll in time"]}, hl:["c1"]}
    ],
    why: "Rebalances are when duplicates happen most: partitions move, and the new owner rereads everything since the last commit. That's normal and safe if processing is idempotent and commits happen after the write. Frequent rebalances, often caused by slow processing, are a performance problem worth monitoring.",
    say: "Partitions are the unit of parallelism; a rebalance moves them between consumers and the new owner resumes from the committed offset, so I commit after processing and keep processing idempotent.",
    hood: [
      "Eager rebalancing stops every consumer; the cooperative sticky assignor moves only the partitions that need to move.",
      "Kafka 4.x adds a new consumer rebalance protocol (KIP-848) where the broker coordinates assignments incrementally (<code>group.protocol=consumer</code>).",
      "Static membership (<code>group.instance.id</code>) lets a consumer restart without triggering a rebalance if it comes back within the session timeout.",
      "Use the revoke callback to commit offsets for work already done before partitions are taken away."
    ],
    code: {file:"kafka-lab/consumer.py", snippet:
`def on_assign(consumer, partitions):
    log("INFO", "partitions assigned (rebalance)", ...)

def on_revoke(consumer, partitions):
    log("INFO", "partitions revoked (rebalance)", ...)

c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)`},
    docs: [
      {label:"Consumer rebalance protocol", url:KD+"operations/consumer-rebalance-protocol/"},
      {label:"partition.assignment.strategy", url:KD+"configuration/consumer-configs/#consumerconfigs_partition.assignment.strategy"},
      {label:"max.poll.interval.ms", url:KD+"configuration/consumer-configs/#consumerconfigs_max.poll.interval.ms"}
    ]
  },

  /* ---------------- 4 ---------------- */
  {
    title: "Committing offsets: after the write",
    lead: "Kafka's version of the ack is the offset {{commit}}. Commit too early and a crash skips data. Commit after the write and a crash only repeats it.",
    analogy: "You're reading a book and move your bookmark before reading the page. You fall asleep. Tomorrow you open at the bookmark and never read that page. Move it after reading, and the worst case is rereading a page.",
    question: {
      q: "The consumer commits offset 5 (\"next is 5\") and then crashes <b>before</b> writing record 4 to the database. After restart, what happens to record 4?",
      options: ["It's read again from offset 4", "It's skipped: lost for the ledger", "Kafka detects the missing write"],
      answer: 1,
      why: "The group's committed position says 5, so the restarted consumer starts at 5. Record 4 is still in the log, but nothing will process it again unless someone resets the offset. Commit after the write instead: a crash means record 4 is read again, and the idempotent write absorbs it."
    },
    nodes: [
      {id:"p0", label:"partition 0", sub:"offsets 3, 4, 5"},
      {id:"cons", label:"consumer", kind:"actor"},
      {id:"off", label:"committed offset", sub:"group bookmark"},
      {id:"db", label:"database", kind:"store"}
    ],
    steps: [
      {say:"The consumer polls record 4 (e4). The committed offset is 4.", move:["p0","cons","e4"], set:{p0:["3: e3","4: e4","5: e5"], cons:["e4"], off:["=4"], db:["=e3"]}, hl:["cons"]},
      {say:"<b>Wrong order.</b> It commits 5 first...", set:{off:["=~5"]}, hl:["off"]},
      {say:"...and crashes before the database write.", set:{cons:["=!crashed"]}, boom:["cons"]},
      {say:"On restart it reads from 5. e4 never reaches the ledger. <b>Lost</b>, silently.", set:{cons:["=reading from 5"], db:["=e3","=!e4 missing"]}, boom:["db"]},
      {say:"<b>Right order.</b> Same situation: poll e4, committed offset is 4.", set:{cons:["e4"], off:["=4"], db:["=e3"]}, hl:["cons"]},
      {say:"Write e4 to the database first; the transaction commits.", move:["cons","db","e4"], set:{db:["=e3","=e4"]}, hl:["db"]},
      {say:"Crash before committing the offset. On restart the consumer reads from 4 again.", set:{cons:["=!crashed, restarting from 4"]}, boom:["cons"]},
      {say:"It reprocesses e4: the unique key ignores the duplicate. Then it commits 5. Nothing lost, nothing double-counted.", move:["cons","db","e4"], set:{cons:["=+duplicate ignored"], off:["=+5"], db:["=e3","=e4"]}, hl:["off","db"]}
    ],
    why: "This is at-least-once delivery, and it's the standard choice: commit after the side effect, make the side effect idempotent. Auto-commit commits on a timer during polling; with simple synchronous processing it usually behaves like at-least-once, but if records are handed to other threads, offsets can be committed before the work is done.",
    say: "Auto-commit off, commit after the database write: a crash re-reads, never skips, and the idempotent write absorbs the repeat.",
    hood: [
      "<code>commitSync</code> blocks until the commit is acknowledged; <code>commitAsync</code> is faster but needs care with retries and ordering of commits.",
      "Committing per record is simple but chatty; committing per batch is common, at the cost of more reprocessing after a crash.",
      "An alternative for an external database: store the offsets in the same database transaction as the data, and on assignment seek to the stored offset. That makes data and position move together.",
      "Message delivery semantics (at-most-once, at-least-once, exactly-once) come down to the order of 'process' and 'save position'."
    ],
    code: {file:"kafka-lab/consumer.py", snippet:
`result = apply(db, event, corr, msg)   # DB transaction commits
c.commit(message=msg, asynchronous=False)  # then the offset`},
    docs: [
      {label:"Message delivery semantics", url:KD+"design/design/#message-delivery-semantics"},
      {label:"enable.auto.commit", url:KD+"configuration/consumer-configs/#consumerconfigs_enable.auto.commit"}
    ]
  },

  /* ---------------- 5 ---------------- */
  {
    title: "Producer durability: acks and replicas",
    lead: "Each partition is copied to several brokers. The producer's {{acks}} setting decides how many copies must exist before a write counts as done.",
    analogy: "You hand a signed contract to an office manager. 'Received' after only the manager filed it is risky: if that office burns down tonight, the contract is gone. 'Received' after two branch offices also have a copy survives one fire.",
    question: {
      q: "With <code>acks=1</code>, the leader stores e7, confirms to the producer, then dies before any follower copies e7. Is e7 safe?",
      options: ["Yes, it was acknowledged", "No, it can be lost", "Kafka never loses acknowledged writes"],
      answer: 1,
      why: "With <code>acks=1</code> only the {{leader}} has the record when it confirms. If a follower without e7 becomes the new leader, e7 is gone even though the producer was told it succeeded. <code>acks=all</code> with <code>min.insync.replicas=2</code> requires at least two copies first."
    },
    nodes: [
      {id:"prod", label:"producer", kind:"actor"},
      {id:"l", label:"broker 1", sub:"leader, partition 0"},
      {id:"f1", label:"broker 2", sub:"follower"},
      {id:"f2", label:"broker 3", sub:"follower"}
    ],
    steps: [
      {say:"<b>acks=1.</b> The producer sends e7 to the partition leader.", move:["prod","l","e7"], set:{prod:[], l:["e7"], f1:[], f2:[]}, hl:["l"]},
      {say:"The leader writes it and immediately confirms. The followers haven't copied it yet.", set:{prod:["=+ack (acks=1)"]}, hl:["prod","l"]},
      {say:"The leader dies. Broker 2 becomes leader, without e7. The producer thinks e7 was saved. <b>Lost.</b>", set:{l:["=!down"], f1:["=new leader","=!no e7"]}, boom:["l","f1"]},
      {say:"<b>acks=all, min.insync.replicas=2.</b> Leader back, fresh start: e8 goes to the leader.", move:["prod","l","e8"], set:{l:["e8"], f1:["=follower"], f2:[], prod:[]}, hl:["l"]},
      {say:"The leader waits until the in-sync followers ({{ISR}}) have copied e8.", move:["l","f1","e8"], set:{f1:["=follower","e8"], f2:["e8"]}, hl:["f1","f2"]},
      {say:"Only now does the producer get its ack. If the leader dies, a follower with e8 takes over. Safe.", set:{prod:["=+ack (acks=all)"], l:["=!down"], f1:["=new leader","e8"]}, hl:["prod","f1"]},
      {say:"Last risk: the network drops the ack and the producer retries e8. The {{idempotent}} producer attaches an ID and sequence number, so the broker discards the second copy.", move:["prod","f1","e8 (retry)"], set:{prod:["=retry e8"], f1:["=new leader","e8","=+retry discarded (same sequence)"]}, hl:["f1"]}
    ],
    why: "For financial data, the producer side is three settings working together: <code>acks=all</code>, <code>min.insync.replicas=2</code> on the topic (with replication factor 3), and the idempotent producer. If too few replicas are in sync, the broker rejects the write with an error instead of accepting a weak one. A loud failure the producer can retry beats a silent loss.",
    say: "Replication factor 3, min.insync.replicas 2, acks=all and the idempotent producer: an acknowledged write survives a broker failure, and producer retries don't create duplicates.",
    hood: [
      "The idempotent producer requires <code>acks=all</code>, retries enabled and at most 5 in-flight requests per connection; it's the default in modern clients when nothing conflicts.",
      "<code>min.insync.replicas</code> is a topic (or broker) setting, and only matters when the producer uses <code>acks=all</code>.",
      "Keep unclean leader election disabled for data you can't lose: otherwise an out-of-sync replica may become leader.",
      "Producer idempotence covers the producer's own retries within a session. It does nothing about a partner sending the same business event twice. That's chapter 6."
    ],
    code: {file:"kafka-lab/producer.py", snippet:
`p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,   # no duplicates from retries
    "acks": "all",                # wait for in-sync replicas
    "linger.ms": 5,
})`},
    docs: [
      {label:"acks", url:KD+"configuration/producer-configs/#producerconfigs_acks"},
      {label:"enable.idempotence", url:KD+"configuration/producer-configs/#producerconfigs_enable.idempotence"},
      {label:"min.insync.replicas", url:KD+"configuration/topic-configs/#topicconfigs_min.insync.replicas"},
      {label:"Replication", url:KD+"design/design/#replication"}
    ]
  },

  /* ---------------- 6 ---------------- */
  {
    title: "Duplicates, versions and 'exactly once'",
    lead: "Kafka advertises exactly-once semantics. It's real, but narrower than it sounds. Your database still needs idempotency.",
    analogy: "A store promises never to scan the same item twice at <i>its own</i> registers. That doesn't stop a customer from bringing the same coupon twice, or your accountant from entering a receipt twice. Each system has to guard its own door.",
    question: {
      q: "Our producer has <code>enable.idempotence=true</code>. The partner resends payment e8 through its API, so we produce it twice. Will Kafka drop the duplicate?",
      options: ["Yes, idempotence removes duplicates", "No, to Kafka these are two different records", "Only if transactions are enabled"],
      answer: 1,
      why: "The idempotent producer only removes duplicates caused by its own retries (same producer, same sequence number). A business-level resend is a new record with a new sequence number. The ledger's <code>UNIQUE(external_id)</code> is what catches it."
    },
    nodes: [
      {id:"p1", label:"partition 1", sub:"acct-2"},
      {id:"cons", label:"consumer", kind:"actor"},
      {id:"led", label:"ledger_entries", sub:"UNIQUE(external_id)", kind:"store"},
      {id:"st", label:"account_status", sub:"versioned upsert", kind:"store"}
    ],
    steps: [
      {say:"The partner's resend produced two records with the same <code>external_id</code>: offsets 10 and 11.", set:{p1:["10: e8","11: e8^"], cons:[], led:[], st:[]}, hl:["p1"]},
      {say:"Offset 10: inserted.", move:["p1","led","e8"], set:{led:["=e8 · $250"], cons:["=+stored"]}, hl:["led"]},
      {say:"Offset 11: <code>ON CONFLICT DO NOTHING</code>. Ignored. The consumer still commits the offset; the work is done.", move:["p1","cons","e8"], set:{cons:["=+duplicate ignored, offset committed"]}, hl:["cons","led"]},
      {say:"State is different from entries. acct-2's status changes: v2 'active', v3 'delinquent'. The partner's retries make them arrive as <b>v3, then v2</b>.", set:{p1:["10: e8","11: e8^","12: status v3","13: status v2"], st:[]}, hl:["p1"]},
      {say:"v3 is applied.", move:["p1","st","v3"], set:{st:["=delinquent (v3)"]}, hl:["st"]},
      {say:"v2 arrives late. The upsert only applies if the incoming version is newer, so v2 is ignored. Without that check, the account would roll back to 'active'.", move:["p1","st","v2"], set:{st:["=delinquent (v3)","=+stale v2 ignored"]}, hl:["st"]},
      {say:"Where exactly-once applies: Kafka transactions plus consumers with <code>isolation.level=read_committed</code> make <i>read from Kafka, process, write to Kafka</i> atomic. Writing to Postgres is outside that transaction, so the database must protect itself.", set:{cons:["=Kafka → Kafka: transactions","=Kafka → database: idempotency"]}, hl:["cons"]}
    ],
    why: "Two kinds of data, two techniques. <b>Entries</b> (facts that happened) are appended once, deduplicated by the source's ID. <b>State</b> (the latest value) is upserted only when newer. Together they make the consumer correct under duplicates, retries, replays and out-of-order arrival.",
    say: "Kafka's exactly-once covers Kafka-to-Kafka with transactions and read_committed. For an external database I rely on idempotent inserts keyed on the source ID and versioned upserts for state.",
    hood: [
      "Transactions use a <code>transactional.id</code>, and consumer offsets can be committed inside the producer's transaction (send offsets to transaction), which is how stream processors get exactly-once.",
      "<code>isolation.level=read_committed</code> hides records from aborted transactions; the default <code>read_uncommitted</code> shows everything.",
      "Version checks need a version that the source increments. Timestamps work if clocks are trustworthy; a sequence from the source is safer.",
      "For the reverse direction (our database change must also produce an event), the outbox pattern writes the event to an outbox table in the same transaction, and a relay publishes it."
    ],
    code: {file:"kafka-lab/consumer.py", snippet:
`INSERT INTO account_status (account_id, status, version)
VALUES (...)
ON CONFLICT (account_id) DO UPDATE
   SET status = EXCLUDED.status, version = EXCLUDED.version
 WHERE account_status.version < EXCLUDED.version`},
    docs: [
      {label:"Message delivery semantics", url:KD+"design/design/#message-delivery-semantics"},
      {label:"isolation.level", url:KD+"configuration/consumer-configs/#consumerconfigs_isolation.level"},
      {label:"enable.idempotence", url:KD+"configuration/producer-configs/#producerconfigs_enable.idempotence"}
    ]
  },

  /* ---------------- 7 ---------------- */
  {
    title: "Retries, DLQ, lag and replay",
    lead: "Putting it together: what happens when a record keeps failing, how to watch for trouble, and how to safely reprocess history.",
    analogy: "A factory line. A part that keeps jamming is pulled onto a side table (the {{DLQ}}) so the line keeps moving. A counter shows how far behind the line is ({{lag}}). After fixing a machine, you can run yesterday's parts through again, as long as the line knows to skip parts it already finished.",
    question: {
      q: "We reset the group's offsets to 0 and re-read the entire topic. What protects the ledger from counting everything twice?",
      options: ["Kafka skips records the group already read", "The consumer's idempotent writes", "Nothing; you should never replay"],
      answer: 1,
      why: "Resetting offsets is exactly 'forget what you read'. Kafka will happily deliver everything again. The unique keys and versioned upserts turn the replay into a no-op for data already applied, and fill in anything that was missing."
    },
    nodes: [
      {id:"p0", label:"partition 0"},
      {id:"cons", label:"consumer", kind:"actor"},
      {id:"dlq", label:"DLQ topic", sub:"ledger.events.dlq", kind:"bad"},
      {id:"db", label:"database", kind:"store"}
    ],
    steps: [
      {say:"e20 fails: the database times out. The consumer retries <b>in place</b>, waiting 0.5s, 1s, 2s (plus jitter). The partition waits, so order is kept.", move:["p0","cons","e20"], set:{p0:["20: e20","21: e21","22: e22"], cons:["e20","=~retry 1, 2, 3..."], dlq:[], db:["=!timeouts"]}, hl:["cons"]},
      {say:"After 4 attempts it gives up on e20: produces it to the DLQ topic with headers (error, attempts, source partition and offset), commits offset 21 and moves on.", move:["cons","dlq","e20"], set:{cons:["=!max attempts → DLQ"], dlq:["e20 (from p0@20)"]}, hl:["dlq"]},
      {say:"Monitoring shows {{lag}} (log end offset minus committed offset) rising while the database was down, and an alert on DLQ volume.", set:{p0:["20: e20","21: e21","22: e22","=~lag: 2"]}, hl:["p0"]},
      {say:"The database recovers. The consumer catches up, lag returns to 0. Then {{redrive}}: e20 is produced back to the main topic, gradually.", move:["dlq","p0","e20"], set:{dlq:[], p0:["20: e20","21: e21","22: e22","23: e20 (redriven)","=+lag: 0"], db:["=e21","=e22","=e20"]}, hl:["p0","db"]},
      {say:"Later, a bug is found in yesterday's processing. Stop the group, move its offsets back to a timestamp (or to 0), restart.", set:{cons:["=offsets reset to 20"]}, hl:["cons"]},
      {say:"Every record is read again. Already-applied entries are ignored by the unique key; anything missing is filled in.", move:["p0","db","replay"], set:{db:["=e21","=e22","=e20","=+replay: 0 new, 4 ignored"]}, hl:["db"]},
      {say:"{{reconciliation|Reconciliation}} compares the partner's records with the ledger: counts, keys and balances match.", set:{db:["=e21","=e22","=e20","=+reconciled"]}, hl:["db"]}
    ],
    why: "Blocking retries keep order but stall a partition, so keep them short and capped. The DLQ keeps one bad record from stopping a whole partition. Lag tells you the system is falling behind before users do. And replay, Kafka's superpower, is only safe because every write is idempotent.",
    say: "Short capped retries in place, then a DLQ topic with the source partition and offset; I alert on lag and DLQ volume, and replays are safe because writes are idempotent, followed by reconciliation.",
    hood: [
      "Non-blocking retry topics (retry-5s, retry-1m) keep the main partition flowing but give up order for the retried record; acceptable for append-only entries, not for state without versions.",
      "Kafka Connect sink connectors have a built-in dead letter queue setting; custom consumers implement it themselves, as in the lab.",
      "Retention defines how far back you can replay; log compaction keeps only the latest record per key, which suits state topics, not a ledger of entries.",
      "Offsets can be reset with the consumer groups tool or the Admin API, only while the group is inactive."
    ],
    code: {file:"kafka-lab/replay.py", snippet:
`admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
futures = admin.alter_consumer_group_offsets(
    [ConsumerGroupTopicPartitions(group, targets)])
# then restart the consumer: idempotent writes make it safe`},
    docs: [
      {label:"Consumer position", url:KD+"design/design/#consumer-position"},
      {label:"Log compaction", url:KD+"design/design/#log-compaction"},
      {label:"Message delivery semantics", url:KD+"design/design/#message-delivery-semantics"}
    ]
  }
  ]
};
