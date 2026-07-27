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
	"time"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	"github.com/cloudbase/garm/config"
	"github.com/cloudbase/garm/metrics"
	"github.com/cloudbase/garm/params"
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
		"outcome", outcome)

	if created {
		r.triggerPrewarmReconcile()
	}
	return nil
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
	if !r.prewarmEnabled() {
		return nil
	}

	startedAt := time.Now()
	defer func() {
		metrics.PrewarmReconcileDuration.Observe(time.Since(startedAt).Seconds())
	}()

	requests, err := r.store.ListActivePrewarmRequests(r.ctx, r.entity.ID)
	if err != nil {
		return fmt.Errorf("error listing prewarm requests: %w", err)
	}
	if len(requests) == 0 {
		return nil
	}

	for _, demand := range aggregatePrewarmDemand(requests) {
		if err := r.reconcilePrewarmTarget(demand); err != nil {
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to reconcile prewarm target",
				"label_key", demand.LabelKey)
		}
	}

	return nil
}

// aggregatePrewarmDemand sums the remaining forecast of every active request by
// label set. Overlapping runs add their forecasts, so a second PR opened while
// the first is still fanning out reuses whatever capacity is already on its way
// instead of each run sizing itself in isolation.
func aggregatePrewarmDemand(requests []params.PrewarmRequest) []params.PrewarmDemand {
	byLabel := map[string]*params.PrewarmDemand{}
	order := []string{}

	for _, request := range requests {
		if !request.IsActive() {
			// Shadow requests record a forecast but never create capacity.
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

func (r *basePoolManager) reconcilePrewarmTarget(demand params.PrewarmDemand) error {
	pool, err := r.resolvePrewarmPool(demand)
	if err != nil {
		return err
	}

	available, err := r.store.CountPoolAvailableCapacity(r.ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("error counting available capacity: %w", err)
	}

	metrics.PrewarmTargetRunners.WithLabelValues(demand.LabelKey, pool.ID).Set(float64(demand.Remaining))

	deficit := int64(demand.Remaining) - available
	if deficit <= 0 {
		slog.DebugContext(
			r.ctx, "prewarm forecast already covered by existing capacity",
			"label_key", demand.LabelKey,
			"pool_id", pool.ID,
			"remaining_forecast", demand.Remaining,
			"available", available)
		return nil
	}

	deficit = r.capSpeculativeDeficit(deficit, demand.LabelKey, pool.ID)
	if deficit <= 0 {
		return nil
	}

	// Capacity is pooled across every request that wants this label set, so it
	// cannot be attributed to one of them. Accounting follows the most recent
	// request, which is the one that grew the forecast we are acting on.
	requestID := demand.RequestIDs[0]
	created := r.createSpeculativeRunners(pool, requestID, demand, deficit)
	if created == 0 {
		return nil
	}

	if err := r.store.RecordPrewarmInstancesCreated(r.ctx, requestID, demand.LabelKey, uint(created)); err != nil {
		slog.With(slog.Any("error", err)).ErrorContext(
			r.ctx, "failed to record prewarm creations",
			"request_id", requestID,
			"label_key", demand.LabelKey)
	}
	metrics.PrewarmInstancesCreated.WithLabelValues(demand.LabelKey, pool.ID).Add(float64(created))

	// New rows are in "pending_create"; wake the creator loop rather than
	// waiting for the next consolidation tick. The whole point is to be early.
	select {
	case r.pendingInstancesTrigger <- struct{}{}:
	default:
	}

	return nil
}

// resolvePrewarmPool finds the single enabled pool a forecast target addresses.
// A target that resolves to zero or several pools is a profile error: acting on
// it would put runners somewhere the operator did not intend, so it is refused
// rather than guessed.
func (r *basePoolManager) resolvePrewarmPool(demand params.PrewarmDemand) (params.Pool, error) {
	pools, err := r.store.FindPoolsMatchingAllTags(r.ctx, r.entity.EntityType, r.entity.ID, demand.Labels)
	if err != nil {
		return params.Pool{}, fmt.Errorf("error finding pools for labels: %w", err)
	}

	enabled := make([]params.Pool, 0, len(pools))
	for _, pool := range pools {
		if pool.Enabled {
			enabled = append(enabled, pool)
		}
	}

	if len(enabled) != 1 {
		return params.Pool{}, fmt.Errorf(
			"prewarm target [%s] resolves to %d enabled pools; it must resolve to exactly one",
			demand.LabelKey, len(enabled))
	}
	return enabled[0], nil
}

// capSpeculativeDeficit trims a deficit to what the global speculative cap
// still allows. The cap bounds how much unproven forecast may be in flight
// across every entity at once.
func (r *basePoolManager) capSpeculativeDeficit(deficit int64, labelKey, poolID string) int64 {
	inFlight, err := r.store.CountSpeculativeInstances(r.ctx)
	if err != nil {
		slog.With(slog.Any("error", err)).ErrorContext(
			r.ctx, "failed to count speculative instances; skipping prewarm")
		return 0
	}

	headroom := int64(r.prewarmCfg.MaxSpeculativeRunners) - inFlight
	if headroom <= 0 {
		slog.InfoContext(
			r.ctx, "global speculative cap reached; not prewarming",
			"label_key", labelKey,
			"pool_id", poolID,
			"in_flight", inFlight,
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

	limiter := newPoolReservationLimiter()
	var created int64
	for i := int64(0); i < count; i++ {
		if _, err := r.addRunnerToPoolConcurrently(pool, nil, limiter, speculative); err != nil {
			// NoCapacity means the pool is at max runners. Every subsequent
			// attempt in this pass would hit the same wall.
			if errors.Is(err, runnerErrors.ErrNoCapacity) {
				slog.InfoContext(
					r.ctx, "pool is full; prewarm cohort trimmed",
					"pool_id", pool.ID,
					"label_key", demand.LabelKey,
					"created", created,
					"requested", count)
				break
			}
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to create speculative runner",
				"pool_id", pool.ID,
				"label_key", demand.LabelKey)
			continue
		}
		created++
	}
	return created
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

	slog.InfoContext(
		r.ctx, "queued job claimed a prewarmed runner",
		"job_id", job.WorkflowJobID,
		"runner_name", instance.Name,
		"pool_id", instance.PoolID,
		"label_key", labelKey,
		"runner_status", instance.RunnerStatus)
	return true
}

// reapSpeculativeSurplus expires forecasts whose window has passed and removes
// the capacity they left unclaimed.
//
// The safety rule this enforces: only runners that are speculative, unclaimed,
// past their expiry and not active are ever removed. A runner GitHub picked up
// on its own is real work, and the store query excludes it rather than relying
// on this function to remember.
//
// This deliberately runs even when prewarming is disabled or paused: flipping
// the kill switch must drain the runners already in flight, not strand them.
func (r *basePoolManager) reapSpeculativeSurplus() error {
	now := time.Now()
	if _, err := r.store.ExpirePrewarmRequests(r.ctx, now); err != nil {
		return fmt.Errorf("error expiring prewarm requests: %w", err)
	}

	reapable, err := r.store.ListReapableSpeculativeInstances(r.ctx, now)
	if err != nil {
		return fmt.Errorf("error listing reapable speculative instances: %w", err)
	}

	for _, instance := range reapable {
		if instance.PoolID == "" || !r.isEntityPoolID(instance.PoolID) {
			// Another entity's pool manager owns this runner.
			continue
		}

		slog.InfoContext(
			r.ctx, "reaping unclaimed speculative runner",
			"runner_name", instance.Name,
			"pool_id", instance.PoolID,
			"request_id", instance.SpeculativeRequestID,
			"reason", "expired")

		if err := r.DeleteRunner(instance, false, false); err != nil {
			slog.With(slog.Any("error", err)).ErrorContext(
				r.ctx, "failed to reap speculative runner",
				"runner_name", instance.Name)
			continue
		}

		labelKey := config.NormalizeLabelKey(instance.AditionalLabels)
		if instance.SpeculativeRequestID != "" {
			if err := r.store.RecordPrewarmInstancesReaped(
				r.ctx, instance.SpeculativeRequestID, labelKey, 1); err != nil {
				slog.With(slog.Any("error", err)).ErrorContext(
					r.ctx, "failed to record prewarm reap",
					"request_id", instance.SpeculativeRequestID)
			}
		}
		metrics.PrewarmInstancesReaped.WithLabelValues(labelKey, instance.PoolID, "expired").Inc()
	}

	return nil
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

func (r *basePoolManager) isEntityPoolID(poolID string) bool {
	pool, err := r.store.GetPoolByID(r.ctx, poolID)
	if err != nil {
		return false
	}
	return r.isEntityPool(pool)
}
