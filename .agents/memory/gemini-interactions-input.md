---
name: Gemini Interactions input
description: Request shape required by the Gemini Interactions API integration.
---

The Gemini Interactions API expects conversation history as typed `user_input` or `model_output` steps, each containing a `content` array of typed parts such as text or media. A flat `type: text` item at the top level is rejected.

**Why:** The API accepts the credential but validates the input schema separately, so a successful key check can still hide an integration failure.

**How to apply:** Preserve the typed-step wrapper whenever converting the app's chat messages into Gemini interaction input, and test both text and media paths after changing it.