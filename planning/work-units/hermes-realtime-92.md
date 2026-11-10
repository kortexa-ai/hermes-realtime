# Hermes Realtime shared PyAV compatibility

Owner: https://github.com/kortexa-ai/hermes-realtime/issues/92

Hermes and Realtime share one PM environment. Their declared requirements must
admit the same PyAV version, including when Hermes enables `stt-whisper`.
Keep the aiortc transport pin deliberate and the shared PyAV range within its
supported versions. Distribution and native plugin manifests stay aligned.

Validate packaging against vanilla and Kortexa Hermes, then run the transport
suite and restore both profiles through Hermes PM before restarting the gateway.
