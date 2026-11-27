# Hermes Realtime shared ONNX Runtime dependency

[Hermes Realtime issue 93](https://github.com/kortexa-ai/hermes-realtime/issues/93)

Hermes PM resolves both profiles in one environment. Its host lock selects
ONNX Runtime 1.31.0. Realtime 0.5.2 required 1.29.0 exactly and could block
preparation of that environment.

Keep the same bounded range in the native and distribution manifests. Check
the core lock in the packaging test because ONNX Runtime is a transitive
dependency of Hermes. Validate the production dependency selection and both
voice transports after a shared gateway restart.
