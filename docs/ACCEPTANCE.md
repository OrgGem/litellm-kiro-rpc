# Windows live acceptance

Status: pending. Automated tests are not live acceptance.

Record Windows build, CLI/SDK versions, model ID and date; never record secrets,
prompts/responses, raw conversation IDs, OAuth URLs or raw protocol errors.

- [ ] Official CLI login works without token copying or environment API keys.
- [ ] Profile excludes built-in tools, MCP, project/user/managed hooks and skills;
      benign canary attempts cannot read/write/run commands. Verify effective
      configuration, not just a permission denial after attempted execution.
- [ ] Completion and SSE through the actual LiteLLM router; no duplicate text.
- [ ] System/history and text blocks; unsupported fields fail before execution.
- [ ] Exact-history warm turn sends only new user input; measure cold/warm timing.
- [ ] Changed history/system/model boots anew; distinct IDs and absent IDs isolate.
- [ ] Same-ID concurrent requests serialize; capacity, TTL, LRU and restart work.
- [ ] Timeout/disconnect before first chunk and midstream reclaim children/slots.
- [ ] CLI failure/missing terminal/queue overflow never produce a success marker.
- [ ] No gateway retry/fallback; abandoned sessions are not resumed from disk.
- [ ] Usage is per turn or absent; no estimated or fabricated token totals.
- [ ] Own temp dirs removed; document CLI-managed retention separately.

Do not mark the backend released until all applicable checks have evidence.
