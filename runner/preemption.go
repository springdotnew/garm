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

package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	"github.com/cloudbase/garm/auth"
	"github.com/cloudbase/garm/config"
	"github.com/cloudbase/garm/metrics"
	"github.com/cloudbase/garm/params"
)

// preemptionRuleIDPrefix namespaces the synthetic rule id a preemption
// replacement is recorded under. The job id makes it unique, which is what
// makes the request idempotent: a runner that reports its notice twice, or two
// notices for the same job, record one replacement.
const preemptionRuleIDPrefix = "preempted-job-"

// ReportInstancePreempted records that the calling runner has been given a
// preemption notice by its cloud, and pre-acquires the runner its job's retry
// will need.
//
// This is the one case ordinary prewarming cannot cover. A fanout is forecast
// from a gate job, so there is something to predict from; a preemption has no
// gate ahead of it, and by the time GitHub queues the retry the boot has to
// happen from cold — on the longest path, in the middle of a run that was
// already partly done. The notice is the only warning anyone gets, and only the
// runner receives it, so the runner is what reports it.
//
// Everything after this point is ordinary prewarming: the request is a forecast
// like any other, the entity's reconciler creates the runner, the claim path
// hands it to the retry when it is queued, and the reaper removes it if the
// retry never comes.
func (r *Runner) ReportInstancePreempted(ctx context.Context) error {
	instanceName := auth.InstanceName(ctx)
	if instanceName == "" {
		return runnerErrors.ErrUnauthorized
	}

	// Report it whatever the configuration says. The notice is a fact about the
	// fleet and it is the only chance anyone has to record it; whether GARM
	// acts on it is a separate question, answered below.
	slog.InfoContext(ctx, "runner reported a preemption notice", "runner_name", instanceName)
	metrics.PrewarmPreemptionsReported.Inc()

	if !r.config.Prewarm.Preemption.Enable {
		return nil
	}

	instance, err := r.store.GetInstance(ctx, instanceName)
	if err != nil {
		return fmt.Errorf("error fetching instance: %w", err)
	}

	entity, labels, err := r.preemptedRunnerTarget(ctx, instance)
	if err != nil {
		return fmt.Errorf("error resolving the preempted runner: %w", err)
	}

	replacement, ok := r.config.Prewarm.Preemption.ReplacementFor(labels)
	if !ok {
		slog.DebugContext(ctx, "no replacement configured for the preempted runner",
			"runner_name", instanceName, "labels", labels)
		return nil
	}

	job, err := r.store.GetJobByInstanceID(ctx, instance.ID)
	if err != nil {
		// A runner preempted before it picked up a job costs nothing to lose:
		// there is no retry coming, so there is nothing to pre-acquire.
		if errors.Is(err, runnerErrors.ErrNotFound) {
			slog.DebugContext(ctx, "preempted runner had no job; nothing to replace",
				"runner_name", instanceName)
			return nil
		}
		return fmt.Errorf("error fetching the preempted runner's job: %w", err)
	}

	// The retry is the next attempt of the same run. Recording the forecast
	// against that attempt is what lets the ordinary consumption path shrink it
	// when the retry is finally queued.
	createParams := params.CreatePrewarmRequestParams{
		EntityID:     entity.ID,
		EntityType:   string(entity.EntityType),
		Repository:   fmt.Sprintf("%s/%s", job.RepositoryOwner, job.RepositoryName),
		WorkflowName: job.WorkflowName,
		RunID:        job.RunID,
		RunAttempt:   job.RunAttempt + 1,
		RuleID:       fmt.Sprintf("%s%d", preemptionRuleIDPrefix, job.WorkflowJobID),
		TriggerJobID: job.WorkflowJobID,
		Mode:         string(r.config.Prewarm.Mode),
		State:        params.PrewarmRequestActive,
		ExpiresAt:    time.Now().Add(r.config.Prewarm.Preemption.Duration()),
		Targets: []params.CreatePrewarmTargetParams{{
			LabelKey:    config.NormalizeLabelKey(replacement),
			Labels:      replacement,
			TargetCount: 1,
		}},
	}
	if !r.config.Prewarm.IsActive() {
		createParams.State = params.PrewarmRequestShadow
	}

	_, created, err := r.store.CreatePrewarmRequest(ctx, createParams)
	if err != nil {
		return fmt.Errorf("error recording the replacement forecast: %w", err)
	}
	if !created {
		return nil
	}

	slog.InfoContext(ctx, "pre-acquiring a replacement for a preempted runner",
		"runner_name", instanceName,
		"from_labels", labels,
		"to_labels", replacement,
		"run_id", job.RunID,
		"retry_attempt", job.RunAttempt+1)
	metrics.PrewarmPreemptionReplacements.WithLabelValues(config.NormalizeLabelKey(replacement)).Inc()
	return nil
}

// preemptedRunnerTarget resolves which entity owns a runner and which labels it
// answered to. Pools carry their labels as tags; a scale set is addressed by
// its name, so its name is its label set.
func (r *Runner) preemptedRunnerTarget(ctx context.Context, instance params.Instance) (params.ForgeEntity, []string, error) {
	if instance.ScaleSetID != 0 {
		scaleSet, err := r.store.GetScaleSetByID(ctx, instance.ScaleSetID)
		if err != nil {
			return params.ForgeEntity{}, nil, fmt.Errorf("error fetching scale set: %w", err)
		}
		entity, err := scaleSet.GetEntity()
		if err != nil {
			return params.ForgeEntity{}, nil, fmt.Errorf("error resolving scale set entity: %w", err)
		}
		return entity, []string{scaleSet.Name}, nil
	}

	pool, err := r.store.GetPoolByID(ctx, instance.PoolID)
	if err != nil {
		return params.ForgeEntity{}, nil, fmt.Errorf("error fetching pool: %w", err)
	}
	entity, err := pool.GetEntity()
	if err != nil {
		return params.ForgeEntity{}, nil, fmt.Errorf("error resolving pool entity: %w", err)
	}

	labels := make([]string, 0, len(pool.Tags))
	for _, tag := range pool.Tags {
		labels = append(labels, tag.Name)
	}
	return entity, labels, nil
}
