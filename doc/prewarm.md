# Speculative prewarming

A pull request workflow usually starts with one cheap gate job — a path filter, a
lint, a `changes` job — and fans out into everything else only once that gate
passes. GARM sees the fanout when GitHub queues it, which is the moment it is
already too late: every one of those jobs now waits for a runner to boot.

Prewarming closes that gap. When the gate job is queued, GARM creates the
runners the fanout is expected to need, so that by the time GitHub queues the
real jobs the runners are already booting or idle.

Nothing about those runners is special. They are ordinary ephemeral JIT runners,
created through the same path, registered the same way, and cleaned up the same
way. The only difference is why they exist and who is allowed to remove them.

> [!NOTE]
> Prewarming is disabled by default and its zero value is a no-op on every code
> path. Nothing changes until you configure a rule and turn it on.

## What it costs and what it buys

A forecast is a bet. When it is right, a job starts without paying for a boot.
When it is wrong, you paid for a runner nobody used.

GARM publishes both sides:

| Metric | Meaning |
| --- | --- |
| `garm_prewarm_requests_total{rule,mode,outcome}` | Forecasts recorded. `outcome` is `created` or `duplicate` |
| `garm_prewarm_target_runners{target,pool}` | Forecast runners still unmet |
| `garm_prewarm_instances_created_total{target,pool}` | Speculative runners created |
| `garm_prewarm_instances_claimed_total{target,pool}` | Speculative runners a real job took — the wins |
| `garm_prewarm_instances_reaped_total{target,pool,reason}` | Speculative runners that died unused — the misses |
| `garm_prewarm_idle_seconds_total{target,pool}` | Seconds those unused runners were alive — the bill |
| `garm_prewarm_reconcile_duration_seconds` | Time taken by a reconcile pass |

The `queued job claimed a prewarmed runner` log line carries
`head_start_seconds`: how long the runner had already been booting when the job
arrived. That is the benefit, per job, in the units that matter.

## Configuration

```toml
[prewarm]
enable = true
mode = "active"                 # "shadow" records forecasts without creating anything
max_speculative_runners = 120   # hard ceiling across every rule and entity
default_ttl = "8m"              # how long an unclaimed runner is kept

[[prewarm.rule]]
id = "spring-pr-tests-attempt-1"   # stable; used in logs, metrics and dedup
repository = "springdotnew/spring"
workflow = "PR Tests"              # exactly as GitHub reports it
trigger_job = "changes"            # the gate job
trigger_action = "queued"          # optional; "queued" is the only useful value
run_attempt = 1                    # optional; 0 or unset matches any attempt

[[prewarm.rule.target]]
labels = ["gcp-2vcpu-arm-spot"]
count = 2

[[prewarm.rule.target]]
labels = ["gcp-4vcpu-spot"]
count = 79
```

A **target** is a label set and how many runners of it the fanout is expected to
need. Label sets are compared order- and case-insensitively, so
`["linux", "X64"]` and `["x64", "Linux"]` are the same target.

Each target must address exactly one enabled pool, or a scale set by name. A
target that matches several pools is refused at reconcile time — putting runners
somewhere you did not choose is worse than not prewarming at all.

### Getting the numbers right

Start in `mode = "shadow"`. Shadow records exactly the forecast active mode
would have acted on, and creates nothing. Let it run over a representative day
and compare `garm_prewarm_target_runners` against what the fanout actually
queued, then set your counts and switch to `active`.

Invalid configuration stops the controller from starting, including while
prewarming is disabled. A typo should surface when you write it, not the first
time somebody flips the switch — and a silently disabled forecast is
indistinguishable from one that simply never matches.

## Behaviour

**The gate job is served first.** A queued job wakes the ordinary queued-job
path before any forecast is recorded. Prediction never outranks real work.

**Real demand shrinks the forecast.** Every queued job consumes one unit of the
forecast for its label set, so the prediction shrinks as the thing it predicted
arrives. A job consumes at most once no matter how many times GitHub delivers
its webhook.

**Existing capacity counts.** The reconciler creates the difference between the
forecast and the capacity a pool already has, so overlapping runs share runners
instead of each sizing itself in isolation. Capacity that is already spoken for
does not count — otherwise the gate job's own runner would cancel out part of
the forecast that same job just created.

**Claiming, not reserving twice.** When a fanout job is queued and a matching
speculative runner exists, GARM hands it to that job instead of creating a
second one. The claim is a single conditional update, so two jobs racing for the
last speculative runner can never both win.

**Nothing that is working is ever removed.** Idle scale-down skips speculative
runners while their window is open. The prewarm reaper removes only runners that
are speculative, unclaimed, past their expiry, and not active — the database
query enforces that, rather than trusting the caller to remember. A runner
GitHub picked up on its own is real work, whatever the forecast intended.

**A claimed runner goes back to being ordinary.** Once a job has claimed it, it
is subject to the same idle scale-down as any other runner.

## Scale sets

Scale sets are prewarmed too, and more simply: a scale set converges on a runner
count rather than reserving a runner per job, so the remaining forecast is just
added to its target. Real demand consumes the forecast as jobs are queued; when
the window closes the target falls back and ordinary scale-down reclaims
whatever is still idle. No claim, no reaper.

A scale set is addressed by name in `runs-on`, so its name is its label set:

```toml
[[prewarm.rule.target]]
labels = ["my-scale-set"]
count = 20
```

## Turning it off in a hurry

```bash
garm-cli controller update --prewarm-paused
```

This takes effect on the next reconcile — no restart, no configuration change.
Rules stay exactly as configured, and scaling for real queued jobs is
completely unaffected. Runners already in flight are still drained, so pausing
never strands capacity. Undo it with `--prewarm-paused=false`.

`garm-cli controller show` reports the current state.

## Related

- [Performance Optimization](/doc/performance.md) — make each boot faster, which
  prewarming does not replace
- [Pools and scaling](/doc/pools-and-scaling.md)
- [Scale sets](/doc/scale-sets.md)
- [Monitoring](/doc/monitoring.md)
