# Candidate native v2 seam

`litellm_kiro_rpc.native_v2.KiroNativeDriver` implements a guarded candidate of
the frozen 0.1 native seam. It bundles and verifies the exact contract snapshot.
The public driver **always rejects session creation** with
`kiro_profile_isolation_not_verified`; there is no user-facing flag to activate
it. Existing standalone HTTP and `KiroLLM` retain their text-only v1 behavior.

The candidate launches ACP explicitly with `--agent-engine=v3` and negotiates
ACP protocol 1. It requires the expected restricted mode and an acknowledged
model selection. These checks are not proof of complete isolation: a matching
mode name cannot establish that CLI configuration, hooks, skills, tools,
global state, or credentials are contained. Do not enable the candidate until
version-bound isolation and live acceptance tests are complete.

ACP `tool_call` updates are observations; the inspected protocol does not show
a verified primitive for delivering a client-executed tool result. Requests for
tools, parallel tool calls, reasoning output/control, or output-token limits
are therefore rejected before a process is spawned, and unexpected tool
activity terminates a turn. The driver never interprets thought chunks as
exportable reasoning or context-window usage and credits as token counts.
Its usage event has unknown counts and completeness, rather than fabricated
OpenAI token totals. Completed text sessions are reused only when the submitted
history exactly matches the known prefix plus one user message.

Tests start a synthetic newline-delimited ACP peer, never a real authenticated
Kiro CLI. They exercise model negotiation, output and usage mapping, unexpected
updates, session reuse, deadlines, bounded output, and child cleanup. These
tests do not establish live CLI compatibility, tool safety, billing attribution,
or a production isolation boundary. The host owns authentication, public IDs,
delivery acknowledgements, and pending leases. If transport cleanup fails, the
isolated temporary profile is retained and the host must quarantine its native
lease until process termination is proven.
