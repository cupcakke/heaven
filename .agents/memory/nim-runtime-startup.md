---
name: Nim runtime startup
description: Environment constraints for starting the Nim service and loading its prompt configuration.
---

The preview workflow must compile Nim with threads disabled and run with SQLite available. The prompt loader also needs to tolerate the project's inline literal-block prompt headers.

**Why:** Nim 2.2 enforces GC-safety on the async HTTP callback, the binary dynamically loads SQLite, and the checked-in prompt file uses an inline block format that the original parser rejected.

**How to apply:** Keep the workflow on `--threads:off`, ensure the SQLite system dependency is present, and restart after source or dependency changes.