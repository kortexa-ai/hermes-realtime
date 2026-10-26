# Native approvals on both voice transports

Issue: https://github.com/kortexa-ai/hermes-realtime/issues/87

Bridge Hermes's native execution approval queue to the exact subscribed voice
participant and active turn. Preserve native request identity and offered choices,
including persistent choices only when the guard supports them. Keep execution,
timeouts and persistent grants in Hermes. Do not permit a client YOLO override.

Validate subscriptions, exact owner/session/turn/request binding, replay,
disconnect and expiry against vanilla and Kortexa Hermes. Deliver through exact
plugin pins for the default and Mira profiles while preserving their configs.
