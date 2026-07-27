// Copyright 2025 Cloudbase Solutions SRL
//
//	Licensed under the Apache License, Version 2.0 (the "License"); you may
//	not use this file except in compliance with the License. You may obtain
//	a copy of the License at
//
//	     https://www.apache.org/licenses/LICENSE-2.0
//
//	Unless required by applicable law or agreed to in writing, software
//	distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//	WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//	License for the specific language governing permissions and limitations
//	under the License.

package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	"github.com/cloudbase/garm/config"
	"github.com/cloudbase/garm/metrics"
	"github.com/cloudbase/garm/params"
	"github.com/cloudbase/garm/runner/common"
)

// prewarmEnabled reports whether this pool manager may act on prewarm rules.
// The controller-wide pause flag is the kill switch: it stops speculative
// creation immediately, without touching configuration and without affecting
// how real queued jobs are scaled.
func (r *basePoolManager) prewarmEnabled() bool {
	if !r.prewarmCfg.Enable {
		return false
	}
	return !r.controllerInfoSnapshot().PrewarmPaused
}

// handlePrewarmForQueuedJob is called for every job GitHub reports as queued.
//
// A queued job plays two roles here. It is real demand, so it always consumes
// one unit of any forecast made for its run and label set — once GitHub has
// queued the work, the ordinary queued-job path owns it and the forecast must
// shrink. It may also be the gate job a rule watches for, in which case it
// starts a new forecast.
func (r *basePoolManager) handlePrewarmForQueuedJob(ctx context.Context, job params.Job) {
	if !r.prewarmEnabled() {
		return
	}

	labelKey := config.NormalizeLabelKey(job.Labels)
	if err := r.store.ConsumePrewarmForecast(
		ctx, r.entity.ID, job.WorkflowJobID, job.RunID, job.RunAttempt, labelKey); err != nil {
		slog.With(slog.Any("error", err)).ErrorContext(
			ctx, "failed to consume prewarm forecast",
			"job_id", job.WorkflowJobID,
			"run_id", job.RunID,
			"label_key", labelKey)
	}

	rule := r.matchPrewarmRule(job)
	if rule == nil {
		return
	}

	if err := r.createPrewarmRequest(ctx, job, rule); err != nil {
		// A forecast is an optimisation. Failing to record one must never fail
		// the webhook or hold up the job that triggered it.
		slog.With(slog.Any("error", err)).ErrorContext(
			ctx, "failed to create prewarm request",
			"rule_id", rule.ID,
			"run_id", job.RunID)
	}
}

// matchPrewarmRule returns the rule matching a queued job, or nil.
func (r *basePoolManager) matchPrewarmRule(job params.Job) *config.PrewarmRule {
	if job.WorkflowName == "" || job.Name == "" {
		return nil
	}

	repository := fmt.Sprintf("%s/%s", job.RepositoryOwner, job.RepositoryName)
	for idx := range r.prewarmCfg.Rules {
		rule := &r.prewarmCfg.Rules[idx]
		if rule.Matches(repository, job.WorkflowName, job.Name, job.Action, job.RunAttempt) {
			return rule
		}
	}
	return nil
}

func (r *basePoolManager) createPrewarmRequest(ctx context.Context, job params.Job, rule *config.PrewarmRule) error {
	state := params.PrewarmRequestActive
	if r.prewarmCfg.Mode == config.PrewarmModeShadow {
		state = params.PrewarmRequestShadow
	}

	targets := make([]params.CreatePrewarmTargetParams, 0, len(rule.Targets))
	for idx := range rule.Targets {
		target := &rule.Targets[idx]
		targets = append(targets, params.CreatePrewarmTargetParams{
			LabelKey:    target.LabelKey(),
			Labels:      target.Labels,
			TargetCount: target.Count,
		})
	}

	request, created, err := r.store.CreatePrewarmRequest(ctx, params.CreatePrewarmRequestParams{
		EntityID:     r.entity.ID,
		EntityType:   string(r.entity.EntityType),
		Repository:   fmt.Sprintf("%s/%s", job.RepositoryOwner, job.RepositoryName),
		WorkflowName: job.WorkflowName,
		RunID:        job.RunID,
		RunAttempt:   job.RunAttempt,
		RuleID:       rule.ID,
		TriggerJobID: job.WorkflowJobID,
		Mode:         string(r.prewarmCfg.Mode),
		State:        state,
		ExpiresAt:    time.Now().Add(rule.TTL.DurationOr(r.prewarmCfg.TTL())),
		Targets:      targets,
	})
	if err != nil {
		return err
	}

	outcome := "duplicate"
	if created {
		outcome = "created"
	}
	metrics.PrewarmRequestsTotal.WithLabelValues(rule.ID, string(r.prewarmCfg.Mode), outcome).Inc()

	slog.InfoContext(
		ctx, "prewarm request recorded",
		"rule_id", rule.ID,
		"request_id", request.ID,
		"repository", request.Repository,
		"workflow", request.WorkflowName,
		"run_id", request.RunID,
		"run_attempt", request.RunAttempt,
		"trigger_job_id", job.WorkflowJobID,
		"mode", request.Mode,
		"outcome", outcome,
		// The forecast itself, on the one line that fires exactly once per
		// forecast. In shadow this is the whole output an operator gets from a
		// rule, so it has to say what was predicted and not merely that
		// something was.
		"forecast", formatForecast(targets))

	if created {
		r.triggerPrewarmReconcile()
	}
	return nil
}

// formatForecast renders a rule's targets as "label=count" pairs, in the order
// the operator wrote them, so the log line reads like the configuration it came
// from.
func formatForecast(targets []params.CreatePrewarmTargetParams) string {
	pairs := make([]string, 0, len(targets))
	for _, target := range targets {
		pairs = append(pairs, fmt.Sprintf("%s=%d", target.LabelKey, target.TargetCount))
	}
	return strings.Join(pairs, " ")
}

func (r *basePoolManager) triggerPrewarmReconcile() {
	select {
	case <-r.ctx.Done():
		return
	case <-r.quit:
		return
	default:
	}

	select {
	case r.prewarmTrigger <- struct{}{}:
	default:
	}
}

// reconcilePrewarm creates the speculative capacity the active forecasts still
// call for. It runs on the regular consolidation loop and on an explicit wake
// after a new request is recorded.
func (r *basePoolManager) reconcilePrewarm() error {
	if !r.prewarmCfg.Enable {
		return nil
	}

	startedAt := time.Now()
	defer func() {
		metrics.PrewarmReconcileDuration.Observe(time.Since(startedAt).Seconds())
	}()

	// The kill switch stops speculation, and the gauge has to say so. A paused
	// controller is holding nothing open for anyone, so a reading left at the
	// last forecast would show unmet demand that nothing is on its way to serve
	// — and would keep showing it until the pause was lifted.
	if r.controllerInfoSnapshot().PrewarmPaused {
		r.retireUnpublishedPrewarmTargets()
		return nil
	}

	requests, err := r.store.ListActivePrewarmRequests(r.ctx, r.entity.ID)
	if err != nil {
		return fmt.Errorf("error listing prewarm requests: %w", err)
	}

	// One instant for every aggregation below, so the shadow report and the
	// thing it rehearses can never disagree about which windows are still open.
	now := time.Now()

	// Shadow forecasts create nothing, but they still have to be visible.
	// Shadow exists so an operator can compare a rule's forecast against the
	// fanout that actually queued before switching it on, and doc/prewarm.md
	// tells them to read garm_prewarm_target_runners to do it. Publishing
	// nothing for every shadow request makes shadow mode useless for the one
	// job it has: a dry run nobody can read is a silent one, not a rehearsal.
	r.observeShadowForecast(aggregatePrewarmDemandInState(requests, shadowRequests, now))

	// Size every target first, then create for all of them at once.
	//
	// Sizing is a few reads and is over in milliseconds; creating is not. Doing
	// both target by target meant a cohort's runway started when the cohort
	// before it had finished being created — measured on the bench controller,
	// the 81-runner target began 19.5 seconds after the 17-runner one, on a
	// runway of about 57 seconds. A third of the head start for the pool that
	// needed it most went to two pools that were already going to make it.
	//
	// The cap is still allocated sequentially in planPrewarmTargets, so nothing
	// about how much gets created changes — only when.
	plans := r.planPrewarmTargets(aggregatePrewarmDemand(requests, now))

	// Every target still in play has just republished itself. Whatever did not
	// is over, and has to read as over — including when there were no requests
	// at all, which is the state a forecast ends in.
	r.retireUnpublishedPrewarmTargets()

	var wg sync.WaitGroup
	for _, plan := range plans {
		wg.Add(1)
		go func(plan prewarmPlan) {
			defer wg.Done()
			r.createPrewarmCohort(plan)
		}(plan)
	}
	wg.Wait()

	return nil
}

// observeShadowForecast publishes what a shadow rule would have prewarmed, and
// creates nothing.
//
// Metric only, exactly like the active path. This runs on every reconcile pass,
// so a log line here would repeat itself every few seconds for the life of the
// forecast; the forecast is logged once, where it is made, by
// createPrewarmRequest.
func (r *basePoolManager) observeShadowForecast(demands []params.PrewarmDemand) {
	for _, demand := range demands {
		pool, isPoolTarget, err := r.resolvePrewarmPool(demand)
		if err != nil {
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to resolve a shadow prewarm target",
				"label_key", demand.LabelKey)
			continue
		}
		if !isPoolTarget {
			// A scale set target, or another entity's. Its own worker reports it.
			continue
		}

		r.publishPrewarmTarget(demand.LabelKey, pool.ID, demand.Remaining)
	}
}

// prewarmSeries identifies one garm_prewarm_target_runners series.
type prewarmSeries struct {
	labelKey string
	poolID   string
}

// publishPrewarmTarget reports how much of a target's forecast is still unmet,
// and remembers the series so the pass that stops publishing it can take it
// back down.
func (r *basePoolManager) publishPrewarmTarget(labelKey, poolID string, remaining uint) {
	metrics.PrewarmTargetRunners.WithLabelValues(labelKey, poolID).Set(float64(remaining))
	r.publishedPrewarmTargets[prewarmSeries{labelKey: labelKey, poolID: poolID}] = r.prewarmPass
}

// retireUnpublishedPrewarmTargets takes every series this pass did not publish
// back to zero, and opens the next pass.
//
// A gauge is only ever read after the fact, so one that keeps reporting the last
// thing it saw is worse than one that was never published: it says a forecast is
// still unmet long after its window closed. doc/prewarm.md asks operators to
// size a rule by comparing this gauge against the fanout that actually queued,
// and a reading that never comes down makes that comparison wrong in the
// direction that buys machines.
//
// Retiring by pass rather than resetting the whole vector is deliberate on both
// counts: the vector is shared with every other entity and with the scale set
// workers, and zeroing at the top of a pass would let a scrape landing mid-pass
// read nothing for a forecast that is still live.
func (r *basePoolManager) retireUnpublishedPrewarmTargets() {
	for series, pass := range r.publishedPrewarmTargets {
		if pass == r.prewarmPass {
			continue
		}
		metrics.PrewarmTargetRunners.WithLabelValues(series.labelKey, series.poolID).Set(0)
		delete(r.publishedPrewarmTargets, series)
	}
	r.prewarmPass++
}

// prewarmPlan is one target, already resolved and already sized: how many
// runners to create, in which pool, against which request.
type prewarmPlan struct {
	pool      params.Pool
	demand    params.PrewarmDemand
	requestID string
	deficit   int64
}

// planPrewarmTargets resolves and sizes every target, in order, and hands out
// the global cap as it goes.
//
// Deliberately sequential and deliberately not concurrent: the cap is a count of
// what is in flight across every entity, so two targets sizing themselves
// against the same reading would each believe the whole headroom was theirs.
// Allocating it here — one reading, decremented per plan — is what makes the
// creation fan-out below safe.
func (r *basePoolManager) planPrewarmTargets(demands []params.PrewarmDemand) []prewarmPlan {
	plans := make([]prewarmPlan, 0, len(demands))
	claimed := int64(0)

	for _, demand := range demands {
		plan, err := r.planPrewarmTarget(demand, claimed)
		if err != nil {
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to reconcile prewarm target",
				"label_key", demand.LabelKey)
			continue
		}
		if plan.deficit <= 0 {
			continue
		}
		claimed += plan.deficit
		plans = append(plans, plan)
	}

	return plans
}

// requestSelector picks which of an entity's live requests an aggregation is
// about. Shadow and active requests are summed the same way and mean different
// things: one is a plan, the other is a commitment.
type requestSelector func(params.PrewarmRequest) bool

func activeRequests(request params.PrewarmRequest) bool { return request.IsActive() }

func shadowRequests(request params.PrewarmRequest) bool { return !request.IsActive() }

// aggregatePrewarmDemand sums the remaining forecast of every active request by
// label set. Overlapping runs add their forecasts, so a second PR opened while
// the first is still fanning out reuses whatever capacity is already on its way
// instead of each run sizing itself in isolation.
func aggregatePrewarmDemand(requests []params.PrewarmRequest, now time.Time) []params.PrewarmDemand {
	return aggregatePrewarmDemandInState(requests, activeRequests, now)
}

func aggregatePrewarmDemandInState(
	requests []params.PrewarmRequest, selected requestSelector, now time.Time,
) []params.PrewarmDemand {
	byLabel := map[string]*params.PrewarmDemand{}
	order := []string{}

	for _, request := range requests {
		// ListActivePrewarmRequests returns everything the reaper has not
		// flipped yet, and the reaper runs on the reap interval while this runs
		// on the consolidation interval — two orders of magnitude apart. The
		// window has to be enforced against the clock here rather than trusted
		// to the row's state, or a forecast outlives itself by minutes and
		// spends the whole of them buying machines.
		if !request.ExpiresAt.After(now) {
			continue
		}
		if !selected(request) {
			continue
		}
		for _, target := range request.Targets {
			remaining := target.RemainingForecast()
			if remaining == 0 {
				continue
			}
			existing, ok := byLabel[target.LabelKey]
			if !ok {
				existing = &params.PrewarmDemand{
					LabelKey: target.LabelKey,
					Labels:   target.Labels,
				}
				byLabel[target.LabelKey] = existing
				order = append(order, target.LabelKey)
			}
			existing.Remaining += remaining
			existing.RequestIDs = append(existing.RequestIDs, request.ID)
			if request.ExpiresAt.After(existing.ExpiresAt) {
				existing.ExpiresAt = request.ExpiresAt
			}
		}
	}

	ret := make([]params.PrewarmDemand, 0, len(order))
	for _, key := range order {
		ret = append(ret, *byLabel[key])
	}
	return ret
}

// planPrewarmTarget sizes one target. claimed is how much of the global cap the
// targets planned before it have already taken.
func (r *basePoolManager) planPrewarmTarget(demand params.PrewarmDemand, claimed int64) (prewarmPlan, error) {
	pool, isPoolTarget, err := r.resolvePrewarmPool(demand)
	if err != nil {
		return prewarmPlan{}, err
	}
	if !isPoolTarget {
		// Nothing here serves this label set. A scale set target looks exactly
		// like this from the pool manager, and its own worker owns it.
		slog.DebugContext(
			r.ctx, "prewarm target does not address a pool of this entity",
			"label_key", demand.LabelKey)
		return prewarmPlan{}, nil
	}

	available, err := r.store.CountPoolAvailableCapacity(r.ctx, pool.ID)
	if err != nil {
		return prewarmPlan{}, fmt.Errorf("error counting available capacity: %w", err)
	}

	r.publishPrewarmTarget(demand.LabelKey, pool.ID, demand.Remaining)

	deficit := int64(demand.Remaining) - available
	if deficit <= 0 {
		slog.DebugContext(
			r.ctx, "prewarm forecast already covered by existing capacity",
			"label_key", demand.LabelKey,
			"pool_id", pool.ID,
			"remaining_forecast", demand.Remaining,
			"available", available)
		return prewarmPlan{}, nil
	}

	deficit = r.capSpeculativeDeficit(deficit, claimed, demand.LabelKey, pool.ID)
	if deficit <= 0 {
		return prewarmPlan{}, nil
	}

	return prewarmPlan{
		pool:   pool,
		demand: demand,
		// Capacity is pooled across every request that wants this label set, so
		// it cannot be attributed to one of them. Accounting follows the most
		// recent request, which is the one that grew the forecast we are acting
		// on.
		requestID: demand.RequestIDs[0],
		deficit:   deficit,
	}, nil
}

// createPrewarmCohort creates one planned cohort. Several of these run at once,
// one per target, so no cohort's head start is spent waiting on another's.
func (r *basePoolManager) createPrewarmCohort(plan prewarmPlan) {
	created := r.createSpeculativeRunners(plan.pool, plan.requestID, plan.demand, plan.deficit)
	if created == 0 {
		return
	}

	if err := r.store.RecordPrewarmInstancesCreated(
		r.ctx, plan.requestID, plan.demand.LabelKey, uint(created)); err != nil {
		slog.With(slog.Any("error", err)).ErrorContext(
			r.ctx, "failed to record prewarm creations",
			"request_id", plan.requestID,
			"label_key", plan.demand.LabelKey)
	}
	metrics.PrewarmInstancesCreated.WithLabelValues(plan.demand.LabelKey, plan.pool.ID).Add(float64(created))

	// New rows are in "pending_create"; wake the creator loop rather than
	// waiting for the next consolidation tick. The whole point is to be early.
	select {
	case r.pendingInstancesTrigger <- struct{}{}:
	default:
	}
}

// resolvePrewarmPool finds the enabled pool a forecast target addresses.
//
// No pool is not an error: the target may name a scale set, or a pool of a
// different entity, and either way this pool manager has nothing to do. Several
// pools is an error, because acting on it would put runners somewhere the
// operator did not choose, and guessing is worse than refusing.
func (r *basePoolManager) resolvePrewarmPool(demand params.PrewarmDemand) (params.Pool, bool, error) {
	pools, err := r.store.FindPoolsMatchingAllTags(r.ctx, r.entity.EntityType, r.entity.ID, demand.Labels)
	if err != nil {
		return params.Pool{}, false, fmt.Errorf("error finding pools for labels: %w", err)
	}

	enabled := make([]params.Pool, 0, len(pools))
	for _, pool := range pools {
		if pool.Enabled {
			enabled = append(enabled, pool)
		}
	}

	switch len(enabled) {
	case 0:
		return params.Pool{}, false, nil
	case 1:
		return enabled[0], true, nil
	default:
		return params.Pool{}, false, fmt.Errorf(
			"prewarm target [%s] resolves to %d enabled pools; it must resolve to exactly one",
			demand.LabelKey, len(enabled))
	}
}

// capSpeculativeDeficit trims a deficit to what the global speculative cap
// still allows. The cap bounds how much unproven forecast may be in flight
// across every entity at once.
//
// claimed is what the targets planned earlier in this same pass have taken but
// not yet created. Without it every target in a pass would size itself against
// the same reading and the cap would only bind one of them.
func (r *basePoolManager) capSpeculativeDeficit(deficit, claimed int64, labelKey, poolID string) int64 {
	inFlight, err := r.store.CountSpeculativeInstances(r.ctx)
	if err != nil {
		slog.With(slog.Any("error", err)).ErrorContext(
			r.ctx, "failed to count speculative instances; skipping prewarm")
		return 0
	}

	headroom := int64(r.prewarmCfg.MaxSpeculativeRunners) - inFlight - claimed
	if headroom <= 0 {
		slog.InfoContext(
			r.ctx, "global speculative cap reached; not prewarming",
			"label_key", labelKey,
			"pool_id", poolID,
			"in_flight", inFlight,
			"claimed_this_pass", claimed,
			"cap", r.prewarmCfg.MaxSpeculativeRunners)
		return 0
	}

	if deficit > headroom {
		slog.InfoContext(
			r.ctx, "trimming prewarm cohort to the global speculative cap",
			"label_key", labelKey,
			"pool_id", poolID,
			"requested", deficit,
			"allowed", headroom)
		return headroom
	}
	return deficit
}

// createSpeculativeRunners creates up to count runners in a pool and returns
// how many were actually created. Provider capacity and quota errors shrink the
// cohort; they are never fatal, because a smaller forecast still helps and the
// real queued-job path remains the safety net.
func (r *basePoolManager) createSpeculativeRunners(pool params.Pool, requestID string, demand params.PrewarmDemand, count int64) int64 {
	// A runner outlives every forecast that wants it, so its expiry is the
	// window of the longest-lived request — not a fresh TTL, which would let
	// capacity drift past the runs that justified it.
	speculative := &params.SpeculativeInstanceParams{
		RequestID: requestID,
		ExpiresAt: demand.ExpiresAt,
	}

	slog.InfoContext(
		r.ctx, "prewarming runners",
		"request_id", requestID,
		"label_key", demand.LabelKey,
		"pool_id", pool.ID,
		"expires_at", demand.ExpiresAt,
		"count", count)

	// Reserve concurrently, exactly as the queued-job path does. A reservation
	// is a database round trip, so creating a cohort one runner at a time costs
	// the cohort its head start: on the bench controller 81 runners took 51.4s
	// to request, and the last one was asked for 18s after GitHub had already
	// queued the fanout it was supposed to be waiting for.
	//
	// poolReservationLimiter exists for this — it is the same limiter, taking
	// the same lock, enforcing the same MaxRunners ceiling, that
	// consumeQueuedJobsWithLimiter fans out over. Sharing its bound keeps one
	// answer to "how hard may this controller push reservations".
	limiter := newPoolReservationLimiter()
	reservations := &errgroup.Group{}
	reservations.SetLimit(queuedJobReservationConcurrency)

	var created atomic.Int64
	var poolFull atomic.Bool
	for i := int64(0); i < count; i++ {
		reservations.Go(func() error {
			// The pool filled up while this reservation waited for a slot.
			// Every one still queued behind it would hit the same wall.
			if poolFull.Load() {
				return nil
			}
			if _, err := r.addRunnerToPoolConcurrently(pool, nil, limiter, speculative); err != nil {
				if errors.Is(err, runnerErrors.ErrNoCapacity) {
					poolFull.Store(true)
					return nil
				}
				slog.With(slog.Any("error", err)).ErrorContext(
					r.ctx, "failed to create speculative runner",
					"pool_id", pool.ID,
					"label_key", demand.LabelKey)
				return nil
			}
			created.Add(1)
			return nil
		})
	}
	// Every goroutine above returns nil; a cohort that comes up short is a
	// smaller forecast, not a failure, and the queued-job path is still the
	// safety net underneath it.
	_ = reservations.Wait()

	if poolFull.Load() {
		slog.InfoContext(
			r.ctx, "pool is full; prewarm cohort trimmed",
			"pool_id", pool.ID,
			"label_key", demand.LabelKey,
			"created", created.Load(),
			"requested", count)
	}
	return created.Load()
}

// claimSpeculativeRunnerForJob tries to satisfy a queued job from capacity that
// was already forecast for it. A successful claim means the job is served by a
// runner that is already booting or idle, and GARM must not create another.
func (r *basePoolManager) claimSpeculativeRunnerForJob(job params.Job, candidates []params.Pool) bool {
	if !r.prewarmEnabled() || len(candidates) == 0 {
		return false
	}

	poolIDs := make([]string, 0, len(candidates))
	for _, pool := range candidates {
		poolIDs = append(poolIDs, pool.ID)
	}

	instance, err := r.store.ClaimSpeculativeInstance(r.ctx, poolIDs, job.WorkflowJobID)
	if err != nil {
		if !errors.Is(err, runnerErrors.ErrNotFound) {
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to claim speculative runner",
				"job_id", job.WorkflowJobID)
		}
		return false
	}

	labelKey := config.NormalizeLabelKey(job.Labels)
	if instance.SpeculativeRequestID != "" {
		if err := r.store.RecordPrewarmInstanceClaimed(r.ctx, instance.SpeculativeRequestID, labelKey); err != nil {
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to record prewarm claim",
				"request_id", instance.SpeculativeRequestID)
		}
	}
	metrics.PrewarmInstancesClaimed.WithLabelValues(labelKey, instance.PoolID).Inc()

	// How long the runner had already been booting when the job arrived is the
	// head start prewarming bought for this job. It is the number the whole
	// feature exists to produce, so record it where it can be measured.
	slog.InfoContext(
		r.ctx, "queued job claimed a prewarmed runner",
		"job_id", job.WorkflowJobID,
		"runner_name", instance.Name,
		"pool_id", instance.PoolID,
		"label_key", labelKey,
		"runner_status", instance.RunnerStatus,
		"head_start_seconds", time.Since(instance.CreatedAt).Seconds())
	return true
}

// speculativeDrainHorizon makes every unclaimed speculative runner look expired
// to the reap query. A TTL buys a forecast the time it needs to pay off; once
// prewarming is disabled it cannot pay off, so waiting the TTL out only bills
// for the wait.
const speculativeDrainHorizon = 100 * 365 * 24 * time.Hour

// startSpeculativeReaper runs the reaper, or drains once in place of it.
//
// Prewarming is off by default and most controllers will never turn it on. The
// reconciler costs nothing to leave looping because it returns before its first
// query, but the reaper's first act is a write, so an unconditional loop would
// put a periodic UPDATE on every controller in the fleet forever, on behalf of
// runners that cannot exist.
//
// Turning prewarming off still has to give back what it was holding: no job can
// claim a speculative runner once the feature is disabled, and one nobody
// reclaims bills by the second until someone notices. So the disabled path
// drains instead — immediately, without waiting out TTLs that no longer mean
// anything — and keeps trying only until a sweep finds nothing left, so the
// steady state on a controller that never prewarms really is silence. Pausing is
// not the same thing: the kill switch leaves the config enabled precisely so the
// loop stays up and drains on its normal schedule.
func (r *basePoolManager) startSpeculativeReaper() {
	if r.prewarmCfg.Enable {
		r.startLoopForFunction(
			r.reapSpeculativeSurplus, common.PoolReapTimeoutInterval, "prewarm_reaper", false, nil)
		return
	}
	r.drainSpeculativeSurplusUntilEmpty()
}

// drainSpeculativeSurplusUntilEmpty sweeps until a sweep leaves nothing behind.
//
// One sweep is almost always enough. It is not guaranteed to be: a runner can
// only be removed once the pool manager is running, and at startup that depends
// on a tools update that may not have succeeded yet. Retrying on the reap
// interval costs nothing in the normal case, where the first sweep finds zero
// runners and returns before the first tick.
func (r *basePoolManager) drainSpeculativeSurplusUntilEmpty() {
	ticker := time.NewTicker(common.PoolReapTimeoutInterval)
	defer ticker.Stop()

	for {
		stranded, err := r.drainSpeculativeSurplus()
		switch {
		case err != nil:
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to drain the speculative runners a previous configuration left behind")
		case stranded == 0:
			return
		default:
			slog.WarnContext(
				r.ctx, "speculative runners survived a drain sweep and are still billing",
				"stranded", stranded)
		}

		select {
		case <-ticker.C:
		case <-r.ctx.Done():
			return
		case <-r.quit:
			return
		}
	}
}

// reapSpeculativeSurplus expires forecasts whose window has passed and removes
// the capacity they left unclaimed.
//
// The safety rule this enforces: only runners that are speculative, unclaimed,
// past their expiry and not active are ever removed. A runner GitHub picked up
// on its own is real work, and the store query excludes it rather than relying
// on this function to remember.
//
// This deliberately keeps working while prewarming is paused: flipping the kill
// switch must drain the runners already in flight, not strand them.
func (r *basePoolManager) reapSpeculativeSurplus() error {
	now := time.Now()
	_, err := r.reapSpeculativeSurplusExpiringBefore(now, now)
	return err
}

// drainSpeculativeSurplus reclaims every unclaimed speculative runner whatever
// is left of its TTL, and reports how many of this entity's it could not
// reclaim. It is the shutdown path for the feature rather than its housekeeping.
func (r *basePoolManager) drainSpeculativeSurplus() (int, error) {
	now := time.Now()
	return r.reapSpeculativeSurplusExpiringBefore(now, now.Add(speculativeDrainHorizon))
}

// reapSpeculativeSurplusExpiringBefore returns the number of this entity's
// runners it listed and failed to remove — zero when there was nothing to do,
// which is what makes it safe to stop calling.
func (r *basePoolManager) reapSpeculativeSurplusExpiringBefore(now, expiringBefore time.Time) (int, error) {
	if _, err := r.store.ExpirePrewarmRequests(r.ctx, now); err != nil {
		return 0, fmt.Errorf("error expiring prewarm requests: %w", err)
	}

	reapable, err := r.store.ListReapableSpeculativeInstances(r.ctx, expiringBefore)
	if err != nil {
		return 0, fmt.Errorf("error listing reapable speculative instances: %w", err)
	}

	stranded := 0
	for _, instance := range reapable {
		pool, ok := r.entityPoolForInstance(instance)
		if !ok {
			// Another entity's pool manager owns this runner.
			continue
		}
		// The pool's tags are the target this runner was created for. The
		// runner's own additional labels are not: a speculative runner is
		// created without any, so reading them here would file every reap
		// under an empty target.
		labelKey := poolLabelKey(pool)

		// Everything this runner was alive for was wasted: it never served a
		// job. That is the price of a forecast that did not pay off, and it is
		// only honest to publish it next to the wins.
		idleSeconds := time.Since(instance.CreatedAt).Seconds()

		slog.InfoContext(
			r.ctx, "reaping unclaimed speculative runner",
			"runner_name", instance.Name,
			"pool_id", instance.PoolID,
			"request_id", instance.SpeculativeRequestID,
			"idle_seconds", idleSeconds,
			"reason", "expired")

		if err := r.DeleteRunner(instance, false, false); err != nil {
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to reap speculative runner",
				"runner_name", instance.Name)
			stranded++
			continue
		}

		if instance.SpeculativeRequestID != "" {
			if err := r.store.RecordPrewarmInstancesReaped(
				r.ctx, instance.SpeculativeRequestID, labelKey, 1); err != nil {
				slog.With(slog.Any("error", err)).ErrorContext(
					r.ctx, "failed to record prewarm reap",
					"request_id", instance.SpeculativeRequestID)
			}
		}
		metrics.PrewarmInstancesReaped.WithLabelValues(labelKey, instance.PoolID, "expired").Inc()
		metrics.PrewarmIdleSeconds.WithLabelValues(labelKey, instance.PoolID).Add(idleSeconds)
	}

	return stranded, nil
}

// isLiveSpeculativeRunner reports whether an instance is speculative capacity
// whose forecast window has not passed yet. Such a runner is idle by design and
// must be left alone by anything that reclaims idle capacity.
func isLiveSpeculativeRunner(instance params.Instance) bool {
	if !instance.Speculative || instance.ReservedForWorkflowJobID != nil {
		return false
	}
	if instance.SpeculativeExpiresAt == nil {
		return true
	}
	return instance.SpeculativeExpiresAt.After(time.Now())
}

// entityPoolForInstance returns the pool a runner belongs to, and false when it
// belongs to a pool this manager does not own.
func (r *basePoolManager) entityPoolForInstance(instance params.Instance) (params.Pool, bool) {
	if instance.PoolID == "" {
		return params.Pool{}, false
	}
	pool, err := r.store.GetPoolByID(r.ctx, instance.PoolID)
	if err != nil {
		return params.Pool{}, false
	}
	if !r.isEntityPool(pool) {
		return params.Pool{}, false
	}
	return pool, true
}

// poolLabelKey is the forecast target a pool serves.
func poolLabelKey(pool params.Pool) string {
	labels := make([]string, 0, len(pool.Tags))
	for _, tag := range pool.Tags {
		labels = append(labels, tag.Name)
	}
	return config.NormalizeLabelKey(labels)
}
