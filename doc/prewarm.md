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

In shadow the gauge is a report, not a commitment: it carries the per-target
forecast for every pool and scale set the rule addresses, while no runner is
created and no scale-set target is raised. Every match also logs its whole
forecast once, on the line that records it:

```
prewarm request recorded rule_id=pr-tests run_id=… mode=shadow outcome=created
  forecast="gcp-2vcpu=17 gcp-2vcpu-arm=2 gcp-4vcpu=81 gcp-8vcpu=10"
```

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

**A forecast is a budget, not a level.** A pool's capacity is shared, so another
run's jobs can take the runners this forecast bought; that must not reopen it. No
request ever creates more than its own target, however the pool got drained
around it — the difference above is capped by what the request has not already
paid for. A cohort the global ceiling truncated still finishes later, because
what is already alive is subtracted as existing capacity rather than as spend.

**Claiming, not reserving twice.** When a fanout job is queued and a matching
speculative runner exists, GARM hands it to that job instead of creating a
second one. The claim is a single conditional update, so two jobs racing for the
last speculative runner can never both win.

**A forecast that is over reads as over.** The window is enforced against the
clock on every pass, not on the reaper's schedule, so an expired forecast stops
buying machines the moment it expires rather than the next time the reaper
happens to run. `garm_prewarm_target_runners` follows it down to zero when the
window closes, when the request is reaped, and when prewarming is paused — a
gauge you size a rule from is only useful if it also tells you when there is
nothing left to serve.

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

## Preemption replacement

Spot capacity is reclaimed with a short notice, and the job on it dies part-way
through. Its retry then starts from zero on a cold runner — the most expensive
thing that can happen to a run, because it lands on the longest path and the
clock has already been running.

A rule cannot help here: there is no gate job ahead of a preemption to forecast
from, and by the time GitHub queues the retry the boot has to happen. The only
warning anyone gets goes to the machine that is about to disappear, so that is
what reports it:

```toml
[prewarm.preemption]
enable = true
ttl = "30m"     # long enough to cover the rest of a run

[[prewarm.preemption.replacement]]
from = ["gcp-4vcpu-spot"]
to   = ["gcp-4vcpu"]

[[prewarm.preemption.replacement]]
from = ["gcp-2vcpu-arm-spot"]
to   = ["gcp-2vcpu-arm"]
```

A runner that receives a preemption notice from its cloud POSTs to
`/api/v1/callbacks/preempted` using the token it already has. GARM looks up the
job it was running and records a one-runner forecast for the *next* attempt of
that run, on the labels the retry will ask for. Everything after that is
ordinary prewarming: the reconciler creates it, the claim path hands it to the
retry when GitHub queues it, and the reaper removes it if the retry never comes.

The `from` set is the preempted runner's labels — a pool's tags, or a scale
set's name. `to` is what the retry will request. A fleet that reruns onto
standard twins maps each spot label to its twin; a fleet without twins maps a
label to itself. A label set with no mapping is left alone: pre-acquiring the
wrong labels buys nothing and costs a machine.

Reporting is idempotent per job, so a watchdog that fires twice — or a retried
POST — pre-acquires one runner. A runner reclaimed before it picked up a job
has no retry coming and is a no-op. `shadow` mode records the replacement
without creating it, the same as any other forecast.

Two counters cover it:

| Metric | Meaning |
| --- | --- |
| `garm_prewarm_preemptions_reported_total` | Notices received, whatever the configuration does with them |
| `garm_prewarm_preemption_replacements_total{target}` | Replacements actually pre-acquired |

The first is incremented even while preemption replacement is disabled, so you
can size the problem before deciding to act on it.

> [!NOTE]
> Delivering the notice is the image's job, not GARM's. On GCE the runner polls
> `metadata.google.internal` for `preempted`; on EC2 it polls the instance
> metadata for a spot interruption notice. GARM only receives the report.

## Turning it off in a hurry

```bash
garm-cli controller update --prewarm-paused
```

This takes effect on the next reconcile — no restart, no configuration change.
Rules stay exactly as configured, and scaling for real queued jobs is
completely unaffected. Runners already in flight are still drained, so pausing
never strands capacity. `garm_prewarm_target_runners` drops to zero on the same
pass, which is how you confirm it took. Undo it with `--prewarm-paused=false`.

`garm-cli controller show` reports the current state.

## Related

- [Performance Optimization](/doc/performance.md) — make each boot faster, which
  prewarming does not replace
- [Pools and scaling](/doc/pools-and-scaling.md)
- [Scale sets](/doc/scale-sets.md)
- [Monitoring](/doc/monitoring.md)
