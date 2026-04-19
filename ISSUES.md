# Issues & Bugs

This document is the result of an in-depth audit of the `amqpx` codebase, with a
specific focus on concurrency hazards and integration issues across the
`Connection → Channel → Queue/Exchange → Producer/Consumer` layers.

Issues are graded:

- **CRITICAL** — wrong behavior, data loss, lifecycle corruption
- **HIGH** — likely to bite users in production
- **MEDIUM** — correctness issues with limited blast radius
- **LOW** — code-quality / robustness nits

---

## 4. Producer

### 4.1 [CRITICAL] republish on confirmed channel

Even when producer is using confirm channel, the internal logic will republish
all the messages that are already confirmed.
