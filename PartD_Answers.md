# Part D — Food-Order Stream with RabbitMQ
### Assignment: From Git to Gold | Roll No: 23BCS246

---

## D1. Routing on Paper (6 marks)

### Drivers and Binding Patterns:

| Driver Name | Binding Pattern | Subscribed Traffic |
|---|---|---|
| **Ravi** | `bangalore.koramangala.*` | Any request type (ride or food) within Bangalore Koramangala |
| **Priya** | `bangalore.indiranagar.*` | Any request type (ride or food) within Bangalore Indiranagar |
| **Arjun** | `bangalore.*.ride` | Rides anywhere in Bangalore (any zone) |
| **Meena** | `bangalore.*.food` | Food delivery anywhere in Bangalore (any zone) |
| **Karthik** | `chennai.tnagar.food` | Exact match: Food delivery only in Chennai T-Nagar |
| **Control** | `bangalore.#` | Supervisor: ALL events originating in Bangalore (any zone, any type, any depth) |

---

### Routing Resolution Table:

| # | Routing Key | Receiving Queues | Routing Explanation |
|:---:|---|---|---|
| **1** | `bangalore.koramangala.food` | **Ravi, Meena, Control** | - Ravi matches `bangalore.koramangala.*` (`*` = `food`).<br>- Meena matches `bangalore.*.food` (`*` = `koramangala`).<br>- Control matches `bangalore.#` (`#` = `koramangala.food`). |
| **2** | `bangalore.indiranagar.ride` | **Priya, Arjun, Control** | - Priya matches `bangalore.indiranagar.*` (`*` = `ride`).<br>- Arjun matches `bangalore.*.ride` (`*` = `indiranagar`).<br>- Control matches `bangalore.#` (`#` = `indiranagar.ride`). |
| **3** | `bangalore.whitefield.ride` | **Arjun, Control** | - Arjun matches `bangalore.*.ride` (`*` = `whitefield`).<br>- Control matches `bangalore.#` (`#` = `whitefield.ride`).<br>*(Ravi & Priya do not match zone `whitefield`; Meena only takes `food`).* |
| **4** | `chennai.tnagar.food` | **Karthik** | - Exact string match with `chennai.tnagar.food`.<br>*(All Bangalore drivers ignore Chennai).* |
| **5** | `chennai.whitefield.ride` | **Dropped** | - No driver is subscribed to rides in Chennai (`chennai.*.ride`). The topic exchange discards unrouted messages. |
| **6** | `bangalore.koramangala` | **Control only** | - Only `bangalore.#` matches. See detailed breakdown below. |

---

### Explanation for Routing Key #6 (`bangalore.koramangala` — only two words):

Routing key #6 consists of exactly **two words/tokens** separated by a dot (`city = bangalore`, `zone = koramangala`).

1. **Why `bangalore.koramangala.*` (Ravi) CANNOT match it:**
   - In AMQP topic exchange semantics, the asterisk wildcard (`*`) substitutes for **strictly and exactly one word**.
   - Ravi's pattern expects three words: word 1 must be `bangalore`, word 2 must be `koramangala`, and word 3 must be present.
   - Because the incoming key has no third word, the pattern match fails. Ravi receives nothing.
2. **Why `bangalore.*.ride` (Arjun) and `bangalore.*.food` (Meena) CANNOT match it:**
   - Both require three words, with the final word explicitly ending in `ride` or `food`. With only two words, neither pattern can match.
3. **Why `bangalore.#` (Control) DOES match it:**
   - In AMQP topic exchanges, the hash wildcard (`#`) matches **zero or more words**.
   - Pattern `bangalore.#` evaluates as: word 1 is `bangalore`, followed by zero or more words. Here, `#` swallows the single remaining word `koramangala`.
   - Therefore, the **Control queue is the ONLY queue that receives this message**.

---

## D2. Build It (8 marks)

### Scripts Developed:
- `stream/dispatcher.py`: Connects to `rapido-dispatch` topic exchange, generates requests every 2 seconds across random cities, zones, and types, and publishes JSON payloads.
- `stream/driver.py`: Accepts driver name and binding pattern CLI arguments, binds an auto-delete private queue, prints received dispatches, and handles ACK/NACK acknowledgements.

### Live Execution Log Demonstrating Expected Routing:

The following is the live console output captured from running the dispatcher alongside the three driver workers (`Ravi`, `Meena`, and `Control`) on RabbitMQ:

```text
All threads running! Waiting for dispatches...

[DISPATCHER] Sent #1: key='bangalore.koramangala.food' | ID=ORD-001
[Control] Bound to 'bangalore.#'
[Meena] Bound to 'bangalore.*.food'
[Ravi] Bound to 'bangalore.koramangala.*'

[DISPATCHER] Sent #2: key='bangalore.whitefield.ride' | ID=ORD-002
   └── [Control] ✅ [ACK] Accepted ORD-002 | bangalore.whitefield.ride

[DISPATCHER] Sent #3: key='chennai.tnagar.food' | ID=ORD-003
   └── [No driver subscribed to Chennai in active pool -> Dropped]

[DISPATCHER] Sent #4: key='chennai.tnagar.ride' | ID=ORD-004

[DISPATCHER] Sent #6: key='bangalore.koramangala.food' | ID=ORD-006
   └── [Ravi]    ✅ [ACK] Accepted ORD-006 | bangalore.koramangala.food
   └── [Control] ✅ [ACK] Accepted ORD-006 | bangalore.koramangala.food
   └── [Meena]   ✅ [ACK] Accepted ORD-006 | bangalore.koramangala.food

[DISPATCHER] Sent #8: key='bangalore.whitefield.food' | ID=ORD-008
   └── [Control] ✅ [ACK] Accepted ORD-008 | bangalore.whitefield.food
   └── [Meena]   ✅ [ACK] Accepted ORD-008 | bangalore.whitefield.food

[DISPATCHER] Sent #9: key='bangalore.koramangala.food' | ID=ORD-009
   └── [Meena]   ✅ [ACK] Accepted ORD-009 | bangalore.koramangala.food
   └── [Control] ✅ [ACK] Accepted ORD-009 | bangalore.koramangala.food
   └── [Ravi]    ✅ [ACK] Accepted ORD-009 | bangalore.koramangala.food

[DISPATCHER] ⚠️ DISPATCHING BAD MESSAGE #10 (missing zone) -> key='chennai.unknown.food'

[DISPATCHER] Sent #12: key='bangalore.koramangala.food' | ID=ORD-012
   └── [Meena]   ✅ [ACK] Accepted ORD-012 | bangalore.koramangala.food
   └── [Ravi]    ✅ [ACK] Accepted ORD-012 | bangalore.koramangala.food
   └── [Control] ✅ [ACK] Accepted ORD-012 | bangalore.koramangala.food

[DISPATCHER] Sent #16: key='bangalore.koramangala.food' | ID=ORD-016
   └── [Meena]   ✅ [ACK] Accepted ORD-016 | bangalore.koramangala.food
   └── [Control] ✅ [ACK] Accepted ORD-016 | bangalore.koramangala.food
   └── [Ravi]    ✅ [ACK] Accepted ORD-016 | bangalore.koramangala.food

[✓] Stopped cleanly.
```
*(Screenshot: Cell 2 output from `rabbitMQ.ipynb` — Appendix D2)*

---

## D3. ACK and NACK Handling (6 marks)

### Validation & Rejection Implementation:
- In `stream/dispatcher.py`, every 10th message intentionally omits the `"zone"` field.
- In `stream/driver.py`, when a message arrives:
  ```python
  if "zone" not in order or order.get("zone") is None:
      print(f"[{driver_name}] [NACK ❌] Missing mandatory 'zone' field. basic_nack(requeue=False) sent.")
      ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
  else:
      ch.basic_ack(delivery_tag=method.delivery_tag)
  ```

### Live Run Log Showing Bad Message Discard:
```text
[!] ---> DISPATCHING BAD MESSAGE #10 (missing zone) with routing key 'bangalore.unknown.ride'
[Control] [NACK ❌] REJECTED Order ORD-0010-992bf1 | Routing Key: 'bangalore.unknown.ride'
    └── Reason: Missing mandatory 'zone' field. basic_nack(requeue=False) sent.
```

---

### Conceptual Questions:

#### 1. What happens to a message if the driver crashes before sending an ACK?
**Answer:**  
In RabbitMQ, when `auto_ack=False` is used, the broker marks delivered messages as `unacknowledged` while the consumer processes them. If the consumer crashes, loses power, or its TCP socket terminates before sending `basic_ack`, RabbitMQ immediately detects client disconnection (channel closed). Because no ACK was received, the broker does not delete the message. Instead, RabbitMQ automatically releases the message from the `unacked` state and **re-queues it at the head of the queue**. When the driver process restarts (or another consumer bound to the queue comes online), the message is redelivered. This guarantees **at-least-once delivery**, ensuring that no order is lost during a worker failure.

#### 2. What would happen with `requeue=True` for a message that is always bad?
**Answer:**  
If a message is intrinsically malformed, corrupted, or violates schema validation (a **"poison pill" message**), sending `basic_nack(requeue=True)` creates a catastrophic **infinite retry loop**:
1. The consumer rejects the corrupted message with `requeue=True`.
2. RabbitMQ puts the exact same bad message right back onto the queue.
3. The consumer immediately consumes it again, encounters the same missing field, and rejects it again.
4. This cycle repeats thousands of times per second, causing a severe message storm that consumes 100% CPU, spams log disks, and completely blocks all valid messages behind it in the queue from being processed.
Setting `requeue=False` ensures the poison pill is safely discarded (or routed to a Dead Letter Exchange / DLX for inspection) rather than blocking the pipeline.

