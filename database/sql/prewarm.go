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

package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/params"
)

// claimAttempts bounds how many times a claim retries after losing a race to
// another job. Each attempt targets a different candidate, so a handful of
// attempts is enough to make progress under the concurrency a single fanout
// produces; exhausting them simply falls through to the cold-runner path.
const claimAttempts = 5

// speculativeClaimableStatuses are the instance states in which a speculative
// runner still represents capacity that a queued job can be told to wait for.
// A runner that is already deleting is not capacity.
var speculativeClaimableStatuses = []commonParams.InstanceStatus{
	commonParams.InstancePendingCreate,
	commonParams.InstanceCreating,
	commonParams.InstanceRunning,
}

// CreatePrewarmRequest inserts a prewarm request and its targets. The insert is
// deduplicated on (entity, repository, workflow, run, attempt, rule): a
// duplicate webhook delivery returns the existing request with created=false
// instead of building a second cohort for the same run.
func (s *sqlDatabase) CreatePrewarmRequest(ctx context.Context, param params.CreatePrewarmRequestParams) (params.PrewarmRequest, bool, error) {
	entityID, err := uuid.Parse(param.EntityID)
	if err != nil {
		return params.PrewarmRequest{}, false, fmt.Errorf("error parsing entity id: %w", err)
	}

	created := false
	var requestID uuid.UUID
	err = s.conn.Transaction(func(tx *gorm.DB) error {
		var existing PrewarmRequest
		q := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("entity_id = ? AND repository = ? AND workflow_name = ? AND run_id = ? AND run_attempt = ? AND rule_id = ?",
				entityID, param.Repository, param.WorkflowName, param.RunID, param.RunAttempt, param.RuleID).
			First(&existing)
		if q.Error == nil {
			requestID = existing.ID
			return nil
		}
		if !errors.Is(q.Error, gorm.ErrRecordNotFound) {
			return fmt.Errorf("error looking up prewarm request: %w", q.Error)
		}

		request := PrewarmRequest{
			EntityID:     entityID,
			EntityType:   param.EntityType,
			Repository:   param.Repository,
			WorkflowName: param.WorkflowName,
			RunID:        param.RunID,
			RunAttempt:   param.RunAttempt,
			RuleID:       param.RuleID,
			TriggerJobID: param.TriggerJobID,
			Mode:         param.Mode,
			State:        string(param.State),
			ExpiresAt:    param.ExpiresAt,
		}
		if err := tx.Create(&request).Error; err != nil {
			return fmt.Errorf("error creating prewarm request: %w", err)
		}

		for _, target := range param.Targets {
			labels, err := json.Marshal(target.Labels)
			if err != nil {
				return fmt.Errorf("error marshalling target labels: %w", err)
			}
			row := PrewarmRequestTarget{
				PrewarmRequestID: request.ID,
				LabelKey:         target.LabelKey,
				Labels:           labels,
				TargetCount:      target.TargetCount,
			}
			if err := tx.Create(&row).Error; err != nil {
				return fmt.Errorf("error creating prewarm request target: %w", err)
			}
		}

		requestID = request.ID
		created = true
		return nil
	})
	if err != nil {
		return params.PrewarmRequest{}, false, err
	}

	request, err := s.getPrewarmRequest(ctx, requestID)
	if err != nil {
		return params.PrewarmRequest{}, false, err
	}
	return request, created, nil
}

func (s *sqlDatabase) getPrewarmRequest(_ context.Context, id uuid.UUID) (params.PrewarmRequest, error) {
	var request PrewarmRequest
	q := s.conn.Preload("Targets").Where("id = ?", id).First(&request)
	if q.Error != nil {
		if errors.Is(q.Error, gorm.ErrRecordNotFound) {
			return params.PrewarmRequest{}, runnerErrors.ErrNotFound
		}
		return params.PrewarmRequest{}, fmt.Errorf("error fetching prewarm request: %w", q.Error)
	}
	return sqlToParamsPrewarmRequest(request)
}

// ListActivePrewarmRequests returns the requests of an entity that may still
// contribute forecast demand: those that have neither expired nor had their
// full forecast observed as real work.
func (s *sqlDatabase) ListActivePrewarmRequests(_ context.Context, entityID string) ([]params.PrewarmRequest, error) {
	asUUID, err := uuid.Parse(entityID)
	if err != nil {
		return nil, fmt.Errorf("error parsing entity id: %w", err)
	}

	var requests []PrewarmRequest
	q := s.conn.Preload("Targets").
		Where("entity_id = ? AND state IN ?", asUUID,
			[]string{string(params.PrewarmRequestShadow), string(params.PrewarmRequestActive)}).
		Order("created_at desc").
		Find(&requests)
	if q.Error != nil {
		return nil, fmt.Errorf("error listing prewarm requests: %w", q.Error)
	}

	ret := make([]params.PrewarmRequest, 0, len(requests))
	for _, request := range requests {
		converted, err := sqlToParamsPrewarmRequest(request)
		if err != nil {
			return nil, err
		}
		ret = append(ret, converted)
	}
	return ret, nil
}

// SumRemainingPrewarmForecast returns how much of the forecast for a label set
// is still unmet across an entity's live requests.
//
// Shadow requests are excluded — they record what would have happened without
// creating anything — and so are requests whose window has closed but whose
// state the reaper has not flipped yet, so a stale forecast can never hold
// capacity open past its expiry.
func (s *sqlDatabase) SumRemainingPrewarmForecast(_ context.Context, entityID, labelKey string) (uint, error) {
	asUUID, err := uuid.Parse(entityID)
	if err != nil {
		return 0, fmt.Errorf("error parsing entity id: %w", err)
	}

	// Rows where demand has caught up with the target are filtered out rather
	// than clamped, which keeps the sum non-negative without a per-row MAX()
	// that sqlite and postgres spell differently.
	var remaining sql.NullInt64
	q := s.conn.Model(&PrewarmRequestTarget{}).
		Joins("JOIN prewarm_requests ON prewarm_requests.id = prewarm_request_targets.prewarm_request_id").
		Where("prewarm_requests.entity_id = ? AND prewarm_requests.state = ? AND prewarm_requests.deleted_at IS NULL",
			asUUID, string(params.PrewarmRequestActive)).
		Where("prewarm_requests.expires_at > ?", time.Now()).
		Where("prewarm_request_targets.label_key = ? AND prewarm_request_targets.observed_demand < prewarm_request_targets.target_count",
			labelKey).
		Select("SUM(prewarm_request_targets.target_count - prewarm_request_targets.observed_demand)").
		Scan(&remaining)
	if q.Error != nil {
		return 0, fmt.Errorf("error summing prewarm forecast: %w", q.Error)
	}
	if !remaining.Valid || remaining.Int64 <= 0 {
		return 0, nil
	}
	return uint(remaining.Int64), nil
}

// ConsumePrewarmForecast records that a real job with the given label set has
// been queued for a run, reducing the remaining forecast for that target by
// one. Real queued work always takes priority over prediction: once GitHub has
// queued a job, the ordinary queued-job path owns it.
//
// A job consumes at most once, however many times GitHub delivers its webhook.
// The claim is a conditional UPDATE on the job row, so redeliveries — and
// concurrent deliveries of the same job — cannot quietly eat the forecast.
func (s *sqlDatabase) ConsumePrewarmForecast(_ context.Context, entityID string, workflowJobID, runID, runAttempt int64, labelKey string) error {
	asUUID, err := uuid.Parse(entityID)
	if err != nil {
		return fmt.Errorf("error parsing entity id: %w", err)
	}

	return s.conn.Transaction(func(tx *gorm.DB) error {
		claim := tx.Model(&WorkflowJob{}).
			Where("workflow_job_id = ? AND prewarm_consumed = ?", workflowJobID, false).
			UpdateColumn("prewarm_consumed", true)
		if claim.Error != nil {
			return fmt.Errorf("error claiming forecast consumption: %w", claim.Error)
		}
		if claim.RowsAffected == 0 {
			// Either this job already consumed, or it is not one of ours.
			return nil
		}

		var requests []PrewarmRequest
		q := tx.Where("entity_id = ? AND run_id = ? AND run_attempt = ?", asUUID, runID, runAttempt).
			Find(&requests)
		if q.Error != nil {
			return fmt.Errorf("error looking up prewarm requests: %w", q.Error)
		}
		if len(requests) == 0 {
			return nil
		}

		ids := make([]uuid.UUID, 0, len(requests))
		for _, request := range requests {
			ids = append(ids, request.ID)
		}

		// A single increment expressed in SQL so concurrent downstream jobs
		// cannot lose an update to a read-modify-write race.
		q = tx.Model(&PrewarmRequestTarget{}).
			Where("prewarm_request_id IN ? AND label_key = ? AND observed_demand < target_count", ids, labelKey).
			UpdateColumn("observed_demand", gorm.Expr("observed_demand + 1"))
		if q.Error != nil {
			return fmt.Errorf("error consuming prewarm forecast: %w", q.Error)
		}
		return nil
	})
}

// ClaimSpeculativeInstance reserves one unclaimed speculative runner from the
// candidate pools for a queued workflow job. It returns ErrNotFound when no
// compatible speculative capacity exists, in which case the caller must fall
// back to creating a cold runner.
//
// The claim is a single conditional UPDATE so that two jobs racing for the last
// speculative runner can never both win: the loser sees zero rows affected and
// retries against a different candidate.
func (s *sqlDatabase) ClaimSpeculativeInstance(ctx context.Context, poolIDs []string, workflowJobID int64) (params.Instance, error) {
	if len(poolIDs) == 0 {
		return params.Instance{}, runnerErrors.ErrNotFound
	}

	poolUUIDs := make([]uuid.UUID, 0, len(poolIDs))
	for _, poolID := range poolIDs {
		asUUID, err := uuid.Parse(poolID)
		if err != nil {
			return params.Instance{}, fmt.Errorf("error parsing pool id: %w", err)
		}
		poolUUIDs = append(poolUUIDs, asUUID)
	}

	for attempt := 0; attempt < claimAttempts; attempt++ {
		claimed, err := s.claimOneSpeculativeInstance(poolUUIDs, workflowJobID)
		if err != nil {
			return params.Instance{}, err
		}
		if claimed != uuid.Nil {
			return s.getInstanceByID(ctx, claimed)
		}
		if !s.hasClaimableSpeculativeInstance(poolUUIDs) {
			break
		}
	}

	return params.Instance{}, runnerErrors.ErrNotFound
}

// claimOneSpeculativeInstance attempts a single conditional claim. It returns
// uuid.Nil when the candidate it picked was taken by somebody else first.
func (s *sqlDatabase) claimOneSpeculativeInstance(poolIDs []uuid.UUID, workflowJobID int64) (uuid.UUID, error) {
	var claimed uuid.UUID
	err := s.conn.Transaction(func(tx *gorm.DB) error {
		var candidate Instance
		q := tx.Where(
			"speculative = ? AND reserved_for_workflow_job_id IS NULL AND pool_id IN ? AND status IN ?",
			true, poolIDs, speculativeClaimableStatuses).
			// An already booted runner serves the job soonest; among equals,
			// the oldest runner has had the most time to come up.
			Order("CASE WHEN runner_status = 'idle' THEN 0 ELSE 1 END, created_at asc").
			First(&candidate)
		if q.Error != nil {
			if errors.Is(q.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return fmt.Errorf("error finding speculative instance: %w", q.Error)
		}

		// The predicate is repeated in the UPDATE so the write itself is the
		// arbiter. Whoever flips the column from NULL wins the runner.
		update := tx.Model(&Instance{}).
			Where("id = ? AND reserved_for_workflow_job_id IS NULL", candidate.ID).
			UpdateColumn("reserved_for_workflow_job_id", workflowJobID)
		if update.Error != nil {
			return fmt.Errorf("error claiming speculative instance: %w", update.Error)
		}
		if update.RowsAffected == 1 {
			claimed = candidate.ID
		}
		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return claimed, nil
}

func (s *sqlDatabase) hasClaimableSpeculativeInstance(poolIDs []uuid.UUID) bool {
	var count int64
	q := s.conn.Model(&Instance{}).Where(
		"speculative = ? AND reserved_for_workflow_job_id IS NULL AND pool_id IN ? AND status IN ?",
		true, poolIDs, speculativeClaimableStatuses).Count(&count)
	return q.Error == nil && count > 0
}

func (s *sqlDatabase) getInstanceByID(_ context.Context, id uuid.UUID) (params.Instance, error) {
	var instance Instance
	q := s.conn.Preload("Pool").Preload("ScaleSet").Preload("Job").Where("id = ?", id).First(&instance)
	if q.Error != nil {
		if errors.Is(q.Error, gorm.ErrRecordNotFound) {
			return params.Instance{}, runnerErrors.ErrNotFound
		}
		return params.Instance{}, fmt.Errorf("error fetching instance: %w", q.Error)
	}
	return s.sqlToParamsInstance(instance)
}

// CountSpeculativeInstances counts every speculative runner that still exists,
// claimed or not. It backs the global cap, which bounds how much unproven
// forecast may be in flight at once across all entities.
func (s *sqlDatabase) CountSpeculativeInstances(_ context.Context) (int64, error) {
	var count int64
	q := s.conn.Model(&Instance{}).
		Where("speculative = ? AND status IN ?", true, speculativeClaimableStatuses).
		Count(&count)
	if q.Error != nil {
		return 0, fmt.Errorf("error counting speculative instances: %w", q.Error)
	}
	return count, nil
}

// CountPoolAvailableCapacity counts the runners of a pool that are provably
// uncommitted, so the prewarm reconciler never stacks speculative capacity on
// top of capacity that already exists.
//
// Two kinds of runner qualify:
//
//   - a speculative runner nobody has claimed, at any stage of its boot; and
//   - an ordinary runner that has finished booting and is sitting idle.
//
// An ordinary runner that is still booting does not qualify. GARM creates those
// in response to a specific queued job, so counting them would let the job that
// triggered a forecast cancel out part of the forecast it triggered.
func (s *sqlDatabase) CountPoolAvailableCapacity(_ context.Context, poolID string) (int64, error) {
	asUUID, err := uuid.Parse(poolID)
	if err != nil {
		return 0, fmt.Errorf("error parsing pool id: %w", err)
	}

	uncommittedSpeculative := s.conn.
		Where("speculative = ? AND reserved_for_workflow_job_id IS NULL AND status IN ? AND runner_status IN ?",
			true, speculativeClaimableStatuses,
			[]params.RunnerStatus{params.RunnerPending, params.RunnerInstalling, params.RunnerIdle}).
		Or("speculative = ? AND status = ? AND runner_status = ?",
			false, commonParams.InstanceRunning, params.RunnerIdle)

	var count int64
	q := s.conn.Model(&Instance{}).
		Where("pool_id = ?", asUUID).
		Where(uncommittedSpeculative).
		Count(&count)
	if q.Error != nil {
		return 0, fmt.Errorf("error counting pool capacity: %w", q.Error)
	}
	return count, nil
}

// RecordPrewarmInstancesCreated bumps the created counter for a target.
func (s *sqlDatabase) RecordPrewarmInstancesCreated(_ context.Context, requestID, labelKey string, count uint) error {
	return s.addToPrewarmCounter(requestID, labelKey, "created_count", count)
}

// RecordPrewarmInstancesReaped bumps the reaped counter for a target.
func (s *sqlDatabase) RecordPrewarmInstancesReaped(_ context.Context, requestID, labelKey string, count uint) error {
	return s.addToPrewarmCounter(requestID, labelKey, "reaped_count", count)
}

// RecordPrewarmInstanceClaimed bumps the claimed counter for a target.
func (s *sqlDatabase) RecordPrewarmInstanceClaimed(_ context.Context, requestID, labelKey string) error {
	return s.addToPrewarmCounter(requestID, labelKey, "claimed_count", 1)
}

func (s *sqlDatabase) addToPrewarmCounter(requestID, labelKey, column string, count uint) error {
	if count == 0 {
		return nil
	}
	asUUID, err := uuid.Parse(requestID)
	if err != nil {
		return fmt.Errorf("error parsing prewarm request id: %w", err)
	}

	q := s.conn.Model(&PrewarmRequestTarget{}).
		Where("prewarm_request_id = ? AND label_key = ?", asUUID, labelKey).
		UpdateColumn(column, gorm.Expr(column+" + ?", count))
	if q.Error != nil {
		return fmt.Errorf("error updating prewarm counter: %w", q.Error)
	}
	return nil
}

// ExpirePrewarmRequests moves every request past its TTL into the expired
// state. Expiring a request only makes its *unclaimed* capacity reapable; a
// runner that has been claimed or has gone active is real work and is left
// alone.
func (s *sqlDatabase) ExpirePrewarmRequests(_ context.Context, now time.Time) (int64, error) {
	q := s.conn.Model(&PrewarmRequest{}).
		Where("state IN ? AND expires_at < ?",
			[]string{string(params.PrewarmRequestShadow), string(params.PrewarmRequestActive)}, now).
		Update("state", string(params.PrewarmRequestExpired))
	if q.Error != nil {
		return 0, fmt.Errorf("error expiring prewarm requests: %w", q.Error)
	}
	return q.RowsAffected, nil
}

// ListReapableSpeculativeInstances returns speculative runners that expired
// without ever being claimed. The query is the safety boundary for cleanup:
// anything claimed, active, or already on its way out is excluded here rather
// than relying on the caller to filter correctly.
func (s *sqlDatabase) ListReapableSpeculativeInstances(_ context.Context, now time.Time) ([]params.Instance, error) {
	var instances []Instance
	q := s.conn.Preload("Pool").Preload("ScaleSet").
		Where("speculative = ? AND reserved_for_workflow_job_id IS NULL", true).
		Where("speculative_expires_at IS NOT NULL AND speculative_expires_at < ?", now).
		Where("status IN ?", speculativeClaimableStatuses).
		Where("runner_status NOT IN ?", []params.RunnerStatus{params.RunnerActive, params.RunnerTerminated}).
		Find(&instances)
	if q.Error != nil {
		return nil, fmt.Errorf("error listing reapable speculative instances: %w", q.Error)
	}

	ret := make([]params.Instance, 0, len(instances))
	for _, instance := range instances {
		converted, err := s.sqlToParamsInstance(instance)
		if err != nil {
			return nil, err
		}
		ret = append(ret, converted)
	}
	return ret, nil
}

func sqlToParamsPrewarmRequest(request PrewarmRequest) (params.PrewarmRequest, error) {
	ret := params.PrewarmRequest{
		ID:           request.ID.String(),
		EntityID:     request.EntityID.String(),
		EntityType:   request.EntityType,
		Repository:   request.Repository,
		WorkflowName: request.WorkflowName,
		RunID:        request.RunID,
		RunAttempt:   request.RunAttempt,
		RuleID:       request.RuleID,
		TriggerJobID: request.TriggerJobID,
		Mode:         request.Mode,
		State:        params.PrewarmRequestState(request.State),
		CreatedAt:    request.CreatedAt,
		UpdatedAt:    request.UpdatedAt,
		ExpiresAt:    request.ExpiresAt,
		Targets:      make([]params.PrewarmRequestTarget, 0, len(request.Targets)),
	}

	for _, target := range request.Targets {
		labels := []string{}
		if len(target.Labels) > 0 {
			if err := json.Unmarshal(target.Labels, &labels); err != nil {
				return params.PrewarmRequest{}, fmt.Errorf("error unmarshalling target labels: %w", err)
			}
		}
		ret.Targets = append(ret.Targets, params.PrewarmRequestTarget{
			ID:             target.ID.String(),
			LabelKey:       target.LabelKey,
			Labels:         labels,
			TargetCount:    target.TargetCount,
			ObservedDemand: target.ObservedDemand,
			CreatedCount:   target.CreatedCount,
			ClaimedCount:   target.ClaimedCount,
			ReapedCount:    target.ReapedCount,
		})
	}

	return ret, nil
}
