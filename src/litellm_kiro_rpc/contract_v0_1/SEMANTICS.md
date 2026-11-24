# agent-rpc 0.1 normative semantics (G0 candidate)

JSON Schema validates structure. It does not validate authentication, history,
capability readiness, usage coverage, event order, or safe native execution.
Consumers MUST enforce this document and the shared conformance cases as well.

## Identity and capabilities

ExecutionContext is supplied by the authenticated host, never decoded from client
metadata. locator is HMAC of canonical [key_scope_id,key_epoch,environment,CID];
session_scope additionally binds profile, alias/config, toolset and effort.
Lookup pending by immutable locator first, compare mutable properties second.
Only live-verified profile/capability evidence can open a production v2 route;
source or fixture verification alone cannot. Legacy local single-owner text mode
retains existing behavior. Multi-key mode requires isolation for every alias.
No raw bearer keys, CLI IDs or native_ref values appear in public HTTP/logs.

## Driver and host ownership

Driver owns process, reader task, transport and native callback futures. Host owns
public session broker, authentication, admission and serialization. Native handles
are opaque in-memory objects. No CLI transport address comes from an API request.
Concrete protocol: async capabilities(profile,model), open_session(context,config),
begin_turn(session,turn_input), events(turn), submit_tool_results(turn,results),
cancel(turn,reason,cleanup_deadline), close_session(session,cleanup_deadline).
events has exactly one bounded consumer. Async/sync facades may marshal onto the
driver owner loop; they cannot create a second owner of the same native turn.

Driver ToolCallStart/Ready contain only native_ref, index, name and arguments
(Ready only); ToolBatchReady contains ordered native_refs after a complete native
boundary. Host allocates public_id, handoff_id, transcript digest and lease and
creates ToolBatch. ToolResults passed to the driver include both public_id and
native_ref after host validation; the driver resolves only its owned native_ref.
A native_ref is unique within the native invocation.
A controlled MCP handler invocation/Future identity may be the native_ref: it
need not equal a provider ToolUseBlock ID. No matching by tool name/arguments or
arrival order. Native observational tool blocks never independently emit a second
public call. Host acceptance of ToolBatchReady requires complete validated JSON object arguments,
known declared name, unique public/native refs, and no local tool execution.
Default serial maximum one pending call. No parallel capability without a native
complete batch boundary. Permission acknowledgement is never a tool result.
native_turn_ended is exclusively a native terminal stop/length/content_filter;
the host emits HTTP tool_calls handoff without ending the native turn.

## Handoff and cancellation

Before returning tool_calls, host owns PREPARED_HANDOFF with finite lease.
Clean producer exhaustion/close detaches, never cancels this pending turn.
Delivery observer ACKs validated successful terminal body (nonstream) or validated
[DONE] frame (SSE), not a later EOF. It monitors actual disconnect/cancellation;
send returning normally after disconnect must not ACK. No ACK after any SSE
error, even if upstream emits [DONE]. Pre-ACK transport failure aborts. Late
disconnect after ACK leaves bounded pending lease, not proof of receipt.
Continuation racing ACK waits at most delivery deadline, not a second invocation.

Tools-enabled initial request requires CID. A continuation supplies the complete
expected assistant-call prefix and all results in one batch. Missing/foreign
locator returns 410; invalid IDs or changed history in an owned batch returns 409
without consuming/mutating it. Used results cannot be submitted again. Final
user message following completed tool history is a new turn, not a continuation.
No automatic retry/fallback/cold replay of expired pending calls.

Tools omitted at continuation inherit the owned schema; supplied tools must match.
round_policy.tool_choice is per HTTP round (default auto); effective changes need
native support before submit. Frozen profile/model/effort changes while pending
conflict. Presentation stream/include_usage can differ without new native session.

Chain deadline, native wall deadline, active generation budget, HTTP delivery
cancellation and tool-wait TTL are distinct. Pending lease is their remaining
minimum. Held generation permit is reused on resume; released permit is reacquired
within hard cap through continuation-priority admission. All-paused saturation
must not deadlock. Cancel/expiry/revoke fences late results; cleanup has a deadline.
Drain admits only authenticated owned continuations, denies new turns; stop waits
for HTTP, native-running, prepared, awaiting-client and cleaning counts to reach 0.

## Reasoning and usage

Reasoning output is off unless configured and supported; only officially exposed
summary/native_exportable text is serializable. Opaque signature refs never leave
owned RAM; hidden/raw reasoning is not a permitted event kind. No fabricated
reasoning, no merging with answer, no effort mapping based on name similarity.

Usage counts nonnegative integers (bool is invalid), null means unknown. Complete
requires input+output known, details consistent with the provider inclusion rules,
and coverage proven for the represented scope. Cached/reasoning details are not
added twice. Delta requires stable event IDs; snapshots replace by sequence in the
same epoch. Same event ID with different contents is an error, not a new count.
Step counts and aggregates covering those steps must not both contribute to the
same total. A covering aggregate can replace a chain projection, while precise
step allocation remains available per HTTP only when proven. Unknown coverage,
counter reset or contradictory aggregates make projection partial/unknown.
Do not infer per-HTTP deltas from a native turn spanning tool rounds without
step boundaries. Failed/cancelled requests preserve observed counters. Ledger
stores metadata only, not prompt/response/tool/reasoning content or credentials.
No inferred subscription price/credits conversion. Output token limits require
native capability; byte/time limits are not token limits.

Host must reject malformed/unsupported input before spawn. Native output violates
protocol => sanitized backend_error, cancellation, no success/[DONE] on HTTP.
Caching of session-affecting response bodies is forbidden; native prompt caching
and its token details are independent. SDK/HTTP serializers consume the same
normalized evidence. Generic LiteLLM estimates never acquire native provenance.
Host sets caching=False and cache={no-cache:true,no-store:true} before dispatch;
caching=False alone does not prevent writes in pinned LiteLLM 1.109.0.
Bare CustomLLM SDK calls have no delivery ACK hook: client tools fail closed unless
a trusted host delivery context exists. The root-owned SDK completion/iterator
wrapper and outer ASGI observer own ACK, cancellation and trusted usage projection.
Native usage must precede native finish in the LiteLLM stream; usage following
finish can be discarded. Missing native terminal must not become synthesized stop.
The dedicated host disables prompt/message and spend logs before LiteLLM callbacks;
outer response projection alone cannot fix callback accounting or logging.

## Schema subset and framing

Client function parameters support type, properties, required, additionalProperties
(boolean), items, enum, description, minimum, maximum, minLength, maxLength,
minItems and maxItems, recursively up to depth16 and total64KiB. No $ref,
remote resolution, arbitrary regex or unevaluated/composition keywords in v0.1.
String/number/integer/boolean/object/array/null and type arrays (nullable) are
accepted only after local validation and adapter capability checks. strict:true
is gated independently; strict:false does not skip argument validation.
Max32 definitions, unique names; max64KiB argument object/call; max128KiB result
batch; max8 calls only if parallel supported. Tighter backend limits win.
Wire JSON duplicate keys/nonfinite numbers, oversize/deep schemas and unknown
fields are rejected. Structured parsing never executes client functions/code.

HTTP/SDK terminal usage: complete attributable input/output -> object; unknown
or unallocatable -> null/omitted (nonstream); requested SSE usage is exactly one
final choices=[] chunk, usage native or null, before [DONE]. Postheaders error
emits error and no successful finish/[DONE]; observed usage remains in ledger.

Exact schema+fixtures+semantics hash matching is required. New optional fields
need a versioned compatibility decision, not an implicit permissive parse.
