# agent-rpc-answer-only 0.1

The host chooses the profile from trusted configuration. This contract does not
grant access to files, commands, MCP servers, tools, agents or hooks. A profile
must have live isolation evidence bound to CLI executable digest, version,
platform, profile ID and revision before production use. Fixture evidence is
only accepted through a test-owned driver injection and never via configuration.

Each invocation has a freshly created process, profile and CLI session. Its only
input is the host-compiled prompt, resolved model and host-owned configuration.
The adapter exposes capabilities, start, events, cancel and close. It has no
tool-result submission or session-resume operation. Every event is one of
text_delta, usage, native_turn_ended or backend_error, using the frozen V2
AdapterEvent payload shapes. Tool activity is an error and must be prevented
before execution by the verified profile. A post-execution event alone does not
prove prevention. Close must finish and confirm cleanup before output is handed
to the client. Cancellation and cleanup retain resource ownership on failure.

With client tools the model output must be exactly one JSON Decision, with no
duplicate keys, extra fields, markdown fence or trailing content. `final` has a
text value; `tool_request` has a declared tool name and object arguments. The
host validates its schema and round policy, allocates public tool IDs and leaves
execution to the client. A syntactically valid call is not authorization. Without
client tools the entire output is plain text and is never interpreted as a call.

Structured tool proposal support is the separate client_tools capability and
tool_choices records its verified policy values. The client result is untrusted
data for a new invocation. A pending chain binds client identity, model, profile,
toolset, conversation and history; a restart loses pending state. No automatic
repair, retry or fallback is allowed. Usage is projected only when verified and
attributable. Byte and deadline bounds are not token limits.

The contract hash is SHA-256 over the exact schema.json bytes, a single LF byte,
and the exact SEMANTICS.md bytes in that order. These files and the hash are
versioned together. CLI harness internals and role priority are not guaranteed.
