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

package params

import "time"

// PrewarmRequestState tracks a forecast through its life.
type PrewarmRequestState string

const (
	// PrewarmRequestShadow is a forecast that was recorded but is not allowed
	// to create instances.
	PrewarmRequestShadow PrewarmRequestState = "shadow"
	// PrewarmRequestActive is a forecast that may create instances.
	PrewarmRequestActive PrewarmRequestState = "active"
	// PrewarmRequestExpired is a forecast whose TTL elapsed. Unclaimed
	// capacity from it is reapable.
	//
	// There is deliberately no "completed" state. A forecast is consumed by
	// having its remaining count drawn down, not by being marked finished, so a
	// terminal state nothing writes would only invite a reader to filter on it.
	PrewarmRequestExpired PrewarmRequestState = "expired"
)

// PrewarmRequest is one matched prewarm rule: a prediction that a workflow
// run's gate job is about to unblock a known fanout.
type PrewarmRequest struct {
	ID         string `json:"id,omitempty"`
	EntityID   string `json:"entity_id,omitempty"`
	EntityType string `json:"entity_type,omitempty"`

	Repository   string `json:"repository,omitempty"`
	WorkflowName string `json:"workflow_name,omitempty"`
	RunID        int64  `json:"run_id,omitempty"`
	RunAttempt   int64  `json:"run_attempt,omitempty"`
	RuleID       string `json:"rule_id,omitempty"`
	TriggerJobID int64  `json:"trigger_job_id,omitempty"`

	Mode  string              `json:"mode,omitempty"`
	State PrewarmRequestState `json:"state,omitempty"`

	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	// ArmedAt is when the gate job that produced this forecast was finished
	// with by the queued-job consumer. Nil means the forecast exists but may not
	// be served yet; every speculative reader filters on it.
	ArmedAt *time.Time `json:"armed_at,omitempty"`

	Targets []PrewarmRequestTarget `json:"targets,omitempty"`
}

// IsActive reports whether the request may still create speculative capacity.
func (p PrewarmRequest) IsActive() bool {
	return p.State == PrewarmRequestActive
}

// PrewarmRequestTarget is one pool's worth of forecast demand within a
// request.
type PrewarmRequestTarget struct {
	ID       string   `json:"id,omitempty"`
	LabelKey string   `json:"label_key,omitempty"`
	Labels   []string `json:"labels,omitempty"`

	TargetCount    uint `json:"target_count,omitempty"`
	ObservedDemand uint `json:"observed_demand,omitempty"`
	CreatedCount   uint `json:"created_count,omitempty"`
	ClaimedCount   uint `json:"claimed_count,omitempty"`
	ReapedCount    uint `json:"reaped_count,omitempty"`
}

// RemainingForecast is the demand still predicted but not yet seen as real
// queued work. Once GitHub has queued the jobs, they are handled by the
// ordinary queued-job path and must not be forecast a second time.
//
// This is the prediction, deliberately unaware of how much capacity has been
// bought against it — shadow mode reports it without buying anything at all.
// What bounds the buying is UnspentBudget below.
func (p PrewarmRequestTarget) RemainingForecast() uint {
	if p.ObservedDemand >= p.TargetCount {
		return 0
	}
	return p.TargetCount - p.ObservedDemand
}

// UnspentBudget is how much of this target's forecast has not been bought yet.
//
// A forecast is a budget spent once, not a level held for the length of the
// window, and this is what is left of it. The reconciler sizes each pass as
// `remaining forecast - available capacity`, but `available` is pool-wide while
// the forecast is per-request: when a *different* run's jobs drained the shared
// pool, a request saw a deficit against a forecast its own demand had never
// touched, and refilled it. With several pull requests in flight the requests
// refill each other's consumption and the fleet grows without any of them
// exceeding its own forecast on paper. Observed in production 2026-08-02: a
// 10-runner `gcp-8vcpu` target created 22, still buying three minutes after its
// own run had finished.
//
// Capping each pass by this makes `CreatedCount <= TargetCount` an invariant per
// target, which is the property that bounds the burst — while still letting a
// cohort that was truncated by the global ceiling finish later, because what is
// already alive is subtracted through `available` rather than through here.
func (p PrewarmRequestTarget) UnspentBudget() uint {
	if p.CreatedCount >= p.TargetCount {
		return 0
	}
	return p.TargetCount - p.CreatedCount
}

// SpeculativeInstanceParams marks a runner being created as speculative and
// carries the forecast it belongs to. A nil value means an ordinary runner.
type SpeculativeInstanceParams struct {
	RequestID string
	ExpiresAt time.Time
}

// CreatePrewarmRequestParams is the deduplicated insert of a matched rule.
type CreatePrewarmRequestParams struct {
	EntityID     string
	EntityType   string
	Repository   string
	WorkflowName string
	RunID        int64
	RunAttempt   int64
	RuleID       string
	TriggerJobID int64
	Mode         string
	State        PrewarmRequestState
	ExpiresAt    time.Time
	// ArmedAt makes the forecast servable at creation. It is nil for a gate
	// job's forecast, which must wait until the queued-job consumer has served
	// that job, and set for a forecast that has nothing to wait for — a
	// preemption replacement is for a job that was dispatched long ago, so
	// holding it back would delay a retry rather than protect anything.
	ArmedAt *time.Time
	Targets []CreatePrewarmTargetParams
}

// CreatePrewarmTargetParams is one forecast target of a new request.
type CreatePrewarmTargetParams struct {
	LabelKey    string
	Labels      []string
	TargetCount uint
}

// PrewarmDemand is the outstanding forecast for one label set, aggregated
// across every active request of an entity. Overlapping runs sum their
// remaining forecasts.
type PrewarmDemand struct {
	LabelKey string
	Labels   []string
	// Remaining is the summed remaining forecast across requests.
	Remaining uint
	// Unspent is the summed unbought budget across the same requests, and is
	// the ceiling on how much one reconcile pass may create. Remaining says how
	// much work is still predicted; Unspent says how much of it has not already
	// been paid for. Without the second number a request re-buys whatever any
	// other run consumes from the shared pool.
	Unspent uint
	// RequestIDs are the requests that contributed to Remaining, most recent
	// first. New capacity is attributed to the first one.
	RequestIDs []string
	// ExpiresAt is the latest expiry among the contributing requests. Capacity
	// is shared, so it must outlive every forecast that still wants it.
	ExpiresAt time.Time
}
