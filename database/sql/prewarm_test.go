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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/database/watcher"
	garmTesting "github.com/cloudbase/garm/internal/testing"
	"github.com/cloudbase/garm/params"
)

// spotLabelKey is the label set of the suite's pool. Speculative runners are
// only ever matched to a pool by label, so most assertions are about this key.
const spotLabelKey = "gcp-4vcpu-spot"

// largeLabelKey belongs to no pool in this suite. It is the second target of
// every request, and exists to prove that targets are accounted separately.
const largeLabelKey = "gcp-8vcpu"

type PrewarmTestSuite struct {
	suite.Suite

	store    common.Store
	adminCtx context.Context
	entity   params.ForgeEntity
	pool     params.Pool
}

func (s *PrewarmTestSuite) SetupTest() {
	ctx := context.Background()
	watcher.InitWatcher(ctx)
	db := newTestDB(s.T())
	s.store = db

	s.adminCtx = garmTesting.ImpersonateAdminContext(ctx, db, s.T())

	githubEndpoint := garmTesting.CreateDefaultGithubEndpoint(s.adminCtx, db, s.T())
	creds := garmTesting.CreateTestGithubCredentials(s.adminCtx, "prewarm-creds", db, s.T(), githubEndpoint)

	org, err := s.store.CreateOrganization(
		s.adminCtx, "prewarm-org", creds, "test-webhookSecret", params.PoolBalancerTypeRoundRobin, false)
	s.Require().NoError(err)

	entity, err := org.GetEntity()
	s.Require().NoError(err)
	s.entity = entity

	pool, err := s.store.CreateEntityPool(s.adminCtx, entity, params.CreatePoolParams{
		ProviderName:   "test-provider",
		MaxRunners:     200,
		MinIdleRunners: 0,
		Image:          "test-image",
		Flavor:         "test-flavor",
		OSType:         "linux",
		OSArch:         "amd64",
		Tags:           []string{spotLabelKey},
		Enabled:        true,
	})
	s.Require().NoError(err)
	s.pool = pool
}

func (s *PrewarmTestSuite) TearDownTest() {
	watcher.CloseWatcher()
}

func (s *PrewarmTestSuite) createRequestParams() params.CreatePrewarmRequestParams {
	return params.CreatePrewarmRequestParams{
		EntityID:     s.entity.ID,
		EntityType:   string(s.entity.EntityType),
		Repository:   "springdotnew/spring",
		WorkflowName: "PR Tests",
		RunID:        30263041273,
		RunAttempt:   1,
		RuleID:       "spring-pr-tests-attempt-1",
		TriggerJobID: 4242,
		Mode:         "active",
		State:        params.PrewarmRequestActive,
		ExpiresAt:    time.Now().Add(8 * time.Minute),
		Targets: []params.CreatePrewarmTargetParams{
			{LabelKey: spotLabelKey, Labels: []string{spotLabelKey}, TargetCount: 5},
			{LabelKey: largeLabelKey, Labels: []string{largeLabelKey}, TargetCount: 2},
		},
	}
}

// queueJob records a queued job for the suite's run. Consumption is claimed
// against the job row, so a job has to exist before it can consume anything.
func (s *PrewarmTestSuite) queueJob(workflowJobID int64) params.Job {
	s.T().Helper()

	orgID, err := uuid.Parse(s.entity.ID)
	s.Require().NoError(err)

	job, err := s.store.CreateOrUpdateJob(s.adminCtx, params.Job{
		WorkflowJobID: workflowJobID,
		RunID:         30263041273,
		RunAttempt:    1,
		WorkflowName:  "PR Tests",
		Name:          "build",
		Action:        "queued",
		Status:        "queued",
		Labels:        []string{spotLabelKey},
		OrgID:         &orgID,
	})
	s.Require().NoError(err)
	return job
}

// consume queues a job and consumes its unit of the forecast.
func (s *PrewarmTestSuite) consume(workflowJobID int64, request params.PrewarmRequest, labelKey string) error {
	s.T().Helper()
	s.queueJob(workflowJobID)
	return s.store.ConsumePrewarmForecast(
		s.adminCtx, s.entity.ID, workflowJobID, request.RunID, request.RunAttempt, labelKey)
}

// createSpeculativeInstance adds a speculative runner to the suite's pool.
func (s *PrewarmTestSuite) createSpeculativeInstance(name, requestID string, expiresAt time.Time) params.Instance {
	s.T().Helper()
	instance, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name:                 name,
		OSType:               "linux",
		OSArch:               "amd64",
		Status:               commonParams.InstanceRunning,
		RunnerStatus:         params.RunnerIdle,
		Speculative:          true,
		SpeculativeRequestID: requestID,
		SpeculativeExpiresAt: &expiresAt,
	})
	s.Require().NoError(err)
	return instance
}

func (s *PrewarmTestSuite) TestCreatePrewarmRequestPersistsTargets() {
	request, created, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)
	s.Require().True(created)

	s.Require().Equal("springdotnew/spring", request.Repository)
	s.Require().Equal("PR Tests", request.WorkflowName)
	s.Require().Equal(int64(30263041273), request.RunID)
	s.Require().Equal(int64(1), request.RunAttempt)
	s.Require().Equal(params.PrewarmRequestActive, request.State)
	s.Require().Len(request.Targets, 2)

	for _, target := range request.Targets {
		s.Require().Zero(target.ObservedDemand)
		s.Require().Equal(target.TargetCount, target.RemainingForecast())
	}
}

// GitHub retries webhook deliveries. A second delivery of the same trigger must
// return the existing cohort rather than doubling the forecast.
func (s *PrewarmTestSuite) TestDuplicateDeliveryCreatesOneRequest() {
	first, created, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)
	s.Require().True(created)

	second, created, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)
	s.Require().False(created, "a duplicate delivery must not create a second cohort")
	s.Require().Equal(first.ID, second.ID)
	s.Require().Len(second.Targets, 2, "targets must not be duplicated either")

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(active, 1)
}

// Concurrent duplicate deliveries are the same hazard with a tighter window.
func (s *PrewarmTestSuite) TestConcurrentDuplicateDeliveriesCreateOneRequest() {
	const deliveries = 8

	var (
		wg           sync.WaitGroup
		mux          sync.Mutex
		createdCount int
		ids          = map[string]struct{}{}
	)

	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request, created, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
			mux.Lock()
			defer mux.Unlock()
			if err != nil {
				return
			}
			if created {
				createdCount++
			}
			ids[request.ID] = struct{}{}
		}()
	}
	wg.Wait()

	s.Require().Equal(1, createdCount, "exactly one delivery may create the cohort")
	s.Require().Len(ids, 1, "every delivery must resolve to the same request")
}

func (s *PrewarmTestSuite) TestConsumeForecastReducesRemaining() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	for i := int64(0); i < 3; i++ {
		s.Require().NoError(s.consume(500+i, request, spotLabelKey))
	}

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(active, 1)

	for _, target := range active[0].Targets {
		switch target.LabelKey {
		case spotLabelKey:
			s.Require().Equal(uint(3), target.ObservedDemand)
			s.Require().Equal(uint(2), target.RemainingForecast())
		case largeLabelKey:
			s.Require().Zero(target.ObservedDemand, "an unrelated label set must not be consumed")
			s.Require().Equal(uint(2), target.RemainingForecast())
		}
	}
}

// Underprediction: more real jobs arrive than were forecast. The remaining
// forecast floors at zero and never goes negative.
func (s *PrewarmTestSuite) TestConsumeForecastFloorsAtZero() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	for i := int64(0); i < 12; i++ {
		s.Require().NoError(s.consume(600+i, request, spotLabelKey))
	}

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)

	for _, target := range active[0].Targets {
		if target.LabelKey == spotLabelKey {
			s.Require().Equal(uint(5), target.ObservedDemand, "observed demand is capped at the target")
			s.Require().Zero(target.RemainingForecast())
		}
	}
}

// Concurrent downstream jobs must not lose an increment to a read-modify-write
// race.
func (s *PrewarmTestSuite) TestConcurrentForecastConsumptionDoesNotLoseUpdates() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	for i := int64(0); i < 5; i++ {
		s.queueJob(700 + i)
	}

	var wg sync.WaitGroup
	for i := int64(0); i < 5; i++ {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			_ = s.store.ConsumePrewarmForecast(
				s.adminCtx, s.entity.ID, jobID, request.RunID, request.RunAttempt, spotLabelKey)
		}(700 + i)
	}
	wg.Wait()

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	for _, target := range active[0].Targets {
		if target.LabelKey == spotLabelKey {
			s.Require().Equal(uint(5), target.ObservedDemand)
		}
	}
}

// GitHub redelivers webhooks. A job that consumed once must never consume
// again, or a redelivery would silently shrink the forecast it belongs to.
func (s *PrewarmTestSuite) TestRedeliveredJobConsumesForecastOnce() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	for range 4 {
		s.Require().NoError(s.consume(801, request, spotLabelKey))
	}

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	for _, target := range active[0].Targets {
		if target.LabelKey == spotLabelKey {
			s.Require().Equal(uint(1), target.ObservedDemand)
			s.Require().Equal(uint(4), target.RemainingForecast())
		}
	}
}

func (s *PrewarmTestSuite) TestConcurrentRedeliveriesConsumeForecastOnce() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)
	s.queueJob(802)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.store.ConsumePrewarmForecast(
				s.adminCtx, s.entity.ID, 802, request.RunID, request.RunAttempt, spotLabelKey)
		}()
	}
	wg.Wait()

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	for _, target := range active[0].Targets {
		if target.LabelKey == spotLabelKey {
			s.Require().Equal(uint(1), target.ObservedDemand)
		}
	}
}

// A job GARM never recorded cannot consume anything. This is what keeps a
// webhook for an unrelated entity from eating another entity's forecast.
func (s *PrewarmTestSuite) TestUnknownJobConsumesNothing() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	s.Require().NoError(s.store.ConsumePrewarmForecast(
		s.adminCtx, s.entity.ID, 999999, request.RunID, request.RunAttempt, spotLabelKey))

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	for _, target := range active[0].Targets {
		s.Require().Zero(target.ObservedDemand)
	}
}

// Scale sets converge on a runner count rather than reserving per job, so the
// only thing they need from a forecast is how much of it is still unmet.
func (s *PrewarmTestSuite) TestSumRemainingPrewarmForecast() {
	remaining, err := s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, spotLabelKey)
	s.Require().NoError(err)
	s.Require().Zero(remaining, "no forecast means no speculative capacity")

	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	remaining, err = s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, spotLabelKey)
	s.Require().NoError(err)
	s.Require().Equal(uint(5), remaining)

	// Overlapping runs add their forecasts for the same label set.
	second := s.createRequestParams()
	second.RunID = request.RunID + 1
	_, _, err = s.store.CreatePrewarmRequest(s.adminCtx, second)
	s.Require().NoError(err)

	remaining, err = s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, spotLabelKey)
	s.Require().NoError(err)
	s.Require().Equal(uint(10), remaining)

	// Real demand shrinks it.
	s.Require().NoError(s.consume(901, request, spotLabelKey))
	remaining, err = s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, spotLabelKey)
	s.Require().NoError(err)
	s.Require().Equal(uint(9), remaining)

	// A different label set is accounted separately.
	remaining, err = s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, largeLabelKey)
	s.Require().NoError(err)
	s.Require().Equal(uint(4), remaining)
}

func (s *PrewarmTestSuite) TestShadowForecastHoldsNoCapacity() {
	shadow := s.createRequestParams()
	shadow.State = params.PrewarmRequestShadow
	_, _, err := s.store.CreatePrewarmRequest(s.adminCtx, shadow)
	s.Require().NoError(err)

	remaining, err := s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, spotLabelKey)
	s.Require().NoError(err)
	s.Require().Zero(remaining, "shadow mode records a forecast without holding capacity")
}

// The reaper flips expired requests on its own schedule. Until it does, an
// expired forecast must already have stopped holding capacity open.
func (s *PrewarmTestSuite) TestExpiredForecastHoldsNoCapacity() {
	expired := s.createRequestParams()
	expired.ExpiresAt = time.Now().Add(-time.Minute)
	_, _, err := s.store.CreatePrewarmRequest(s.adminCtx, expired)
	s.Require().NoError(err)

	remaining, err := s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, spotLabelKey)
	s.Require().NoError(err)
	s.Require().Zero(remaining)
}

func (s *PrewarmTestSuite) TestForecastIsScopedToItsEntity() {
	_, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	other := uuid.New().String()
	remaining, err := s.store.SumRemainingPrewarmForecast(s.adminCtx, other, spotLabelKey)
	s.Require().NoError(err)
	s.Require().Zero(remaining, "one entity's forecast must not size another's capacity")
}

func (s *PrewarmTestSuite) TestClaimSpeculativeInstance() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	expiry := time.Now().Add(8 * time.Minute)
	s.createSpeculativeInstance("spec-1", request.ID, expiry)
	s.createSpeculativeInstance("spec-2", request.ID, expiry)

	first, err := s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 111)
	s.Require().NoError(err)
	second, err := s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 222)
	s.Require().NoError(err)

	s.Require().NotEqual(first.Name, second.Name, "two jobs must never claim the same runner")
	s.Require().Equal(int64(111), *first.ReservedForWorkflowJobID)
	s.Require().Equal(int64(222), *second.ReservedForWorkflowJobID)

	// Capacity is exhausted; the caller must fall back to a cold runner.
	_, err = s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 333)
	s.Require().True(errors.Is(err, runnerErrors.ErrNotFound))
}

// Measured on a live benchmark before it was fixed: a job claimed a speculative
// runner the forge had already dispatched other work to, GARM therefore did not
// create a runner for it, and the job waited 661 seconds — eight times the worst
// wait the same fanout saw with prewarming switched off. A runner that is
// already working is not capacity, whatever the provider says about it.
func (s *PrewarmTestSuite) TestClaimIgnoresSpeculativeRunnersTheForgeIsAlreadyUsing() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	expiry := time.Now().Add(8 * time.Minute)
	taken := s.createSpeculativeInstance("spec-taken", request.ID, expiry)
	_, err = s.store.UpdateInstance(s.adminCtx, taken.Name, params.UpdateInstanceParams{
		RunnerStatus: params.RunnerActive,
	})
	s.Require().NoError(err)

	_, err = s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 111)
	s.Require().True(errors.Is(err, runnerErrors.ErrNotFound),
		"a speculative runner already running a job must never be claimed again")

	// And the pool must not count it as capacity either, or the next forecast
	// will build a cohort that is short by exactly the runners it cannot use.
	capacity, err := s.store.CountPoolAvailableCapacity(s.adminCtx, s.pool.ID)
	s.Require().NoError(err)
	s.Require().Equal(int64(0), capacity)
}

// The previous test proves a runner the forge is *already* using is filtered
// out by the SELECT. This one covers the window the SELECT cannot see: the forge
// can hand the candidate a job between the read and the write, and a claim whose
// UPDATE re-asserts only the reservation column takes it anyway. The queued job
// is then reported as served by a runner that is busy, and waits out the
// ten-minute unlock — 661 seconds, in the run that first surfaced it.
//
// The forge's write is simulated by a one-shot callback that fires after the
// candidate SELECT, inside the claim's own transaction, which is precisely where
// the real race lands.
func (s *PrewarmTestSuite) TestClaimRejectsARunnerTheForgeTookMidClaim() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	expiry := time.Now().Add(8 * time.Minute)
	instance := s.createSpeculativeInstance("spec-raced", request.ID, expiry)

	store, ok := s.store.(*sqlDatabase)
	s.Require().True(ok, "the race can only be staged against the sql store")

	const hookName = "test:forge_takes_candidate_mid_claim"
	taken := false
	s.Require().NoError(store.conn.Callback().Query().After("gorm:query").Register(
		hookName,
		func(tx *gorm.DB) {
			if taken {
				return
			}
			if _, isInstance := tx.Statement.Dest.(*Instance); !isInstance {
				return
			}
			taken = true
			tx.Session(&gorm.Session{NewDB: true}).Model(&Instance{}).
				Where("id = ?", instance.ID).
				UpdateColumn("runner_status", params.RunnerActive)
		}))
	defer func() {
		s.Require().NoError(store.conn.Callback().Query().Remove(hookName))
	}()

	_, err = s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 111)
	s.Require().True(taken, "the race was never staged; the test proves nothing")
	s.Require().True(errors.Is(err, runnerErrors.ErrNotFound),
		"a runner the forge took mid-claim must not be handed to a queued job")

	// And it must not be left reserved to a job it never served.
	after, err := s.store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Require().Zero(after.ReservedForWorkflowJobID)
}

func (s *PrewarmTestSuite) TestClaimIgnoresNonSpeculativeInstances() {
	_, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name:         "ordinary-runner",
		OSType:       "linux",
		OSArch:       "amd64",
		Status:       commonParams.InstanceRunning,
		RunnerStatus: params.RunnerIdle,
	})
	s.Require().NoError(err)

	_, err = s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 111)
	s.Require().True(errors.Is(err, runnerErrors.ErrNotFound),
		"an ordinary runner must never be claimed as speculative capacity")
}

// The central concurrency guarantee: a fanout of jobs racing for a limited
// speculative cohort must produce distinct claims and no double-assignment.
func (s *PrewarmTestSuite) TestConcurrentClaimsAreDistinct() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	const available = 5
	const claimers = 12

	expiry := time.Now().Add(8 * time.Minute)
	for i := 0; i < available; i++ {
		s.createSpeculativeInstance(fmt.Sprintf("spec-%d", i), request.ID, expiry)
	}

	var (
		wg       sync.WaitGroup
		mux      sync.Mutex
		claimed  = map[string]int64{}
		notFound int
	)

	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			instance, err := s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, jobID)
			mux.Lock()
			defer mux.Unlock()
			if err != nil {
				notFound++
				return
			}
			claimed[instance.Name] = jobID
		}(int64(1000 + i))
	}
	wg.Wait()

	s.Require().Len(claimed, available, "every speculative runner must be claimed exactly once")
	s.Require().Equal(claimers-available, notFound, "the rest must fall through to the cold path")
}

func (s *PrewarmTestSuite) TestReapableExcludesClaimedAndUnexpired() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(8 * time.Minute)

	s.createSpeculativeInstance("expired-unclaimed", request.ID, past)
	s.createSpeculativeInstance("expired-claimed", request.ID, past)
	s.createSpeculativeInstance("live-unclaimed", request.ID, future)

	_, err = s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 999)
	s.Require().NoError(err)

	reapable, err := s.store.ListReapableSpeculativeInstances(s.adminCtx, time.Now())
	s.Require().NoError(err)

	names := make([]string, 0, len(reapable))
	for _, instance := range reapable {
		names = append(names, instance.Name)
		s.Require().Nil(instance.ReservedForWorkflowJobID, "a claimed runner must never be reapable")
	}
	s.Require().NotContains(names, "live-unclaimed", "an unexpired runner must never be reapable")
}

// The one failure with no acceptable rate: cleanup must never touch a runner
// that is executing a job.
func (s *PrewarmTestSuite) TestActiveRunnerIsNeverReapable() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	past := time.Now().Add(-time.Minute)
	instance := s.createSpeculativeInstance("went-active", request.ID, past)

	// GitHub picked the runner up on its own, without GARM claiming it first.
	_, err = s.store.UpdateInstance(s.adminCtx, instance.Name, params.UpdateInstanceParams{
		RunnerStatus: params.RunnerActive,
	})
	s.Require().NoError(err)

	reapable, err := s.store.ListReapableSpeculativeInstances(s.adminCtx, time.Now())
	s.Require().NoError(err)
	for _, candidate := range reapable {
		s.Require().NotEqual("went-active", candidate.Name,
			"a speculative runner that went active is real work and must survive cleanup")
	}
}

func (s *PrewarmTestSuite) TestExpirePrewarmRequests() {
	param := s.createRequestParams()
	param.ExpiresAt = time.Now().Add(-time.Minute)
	_, _, err := s.store.CreatePrewarmRequest(s.adminCtx, param)
	s.Require().NoError(err)

	affected, err := s.store.ExpirePrewarmRequests(s.adminCtx, time.Now())
	s.Require().NoError(err)
	s.Require().Equal(int64(1), affected)

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Empty(active, "an expired request no longer contributes forecast demand")
}

func (s *PrewarmTestSuite) TestCountSpeculativeInstances() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	count, err := s.store.CountSpeculativeInstances(s.adminCtx)
	s.Require().NoError(err)
	s.Require().Zero(count)

	expiry := time.Now().Add(8 * time.Minute)
	s.createSpeculativeInstance("spec-a", request.ID, expiry)
	s.createSpeculativeInstance("spec-b", request.ID, expiry)

	count, err = s.store.CountSpeculativeInstances(s.adminCtx)
	s.Require().NoError(err)
	s.Require().Equal(int64(2), count)
}

// Uncommitted capacity must reduce the deficit the reconciler creates,
// otherwise every forecast stacks on top of the last. Capacity that is already
// spoken for must not, or a run would cancel out the forecast it triggered.
func (s *PrewarmTestSuite) TestCountPoolAvailableCapacity() {
	available, err := s.store.CountPoolAvailableCapacity(s.adminCtx, s.pool.ID)
	s.Require().NoError(err)
	s.Require().Zero(available)

	// Booted and idle: available to anyone.
	_, err = s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name: "idle-runner", OSType: "linux", OSArch: "amd64",
		Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle,
	})
	s.Require().NoError(err)

	// Booting in response to a queued job: already spoken for.
	_, err = s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name: "job-response-runner", OSType: "linux", OSArch: "amd64",
		Status: commonParams.InstancePendingCreate, RunnerStatus: params.RunnerPending,
	})
	s.Require().NoError(err)

	available, err = s.store.CountPoolAvailableCapacity(s.adminCtx, s.pool.ID)
	s.Require().NoError(err)
	s.Require().Equal(int64(1), available)

	// Speculative and unclaimed: available even while it boots.
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)
	speculative := s.createSpeculativeInstance("spec-a", request.ID, time.Now().Add(8*time.Minute))

	available, err = s.store.CountPoolAvailableCapacity(s.adminCtx, s.pool.ID)
	s.Require().NoError(err)
	s.Require().Equal(int64(2), available)

	// Claimed by a job: no longer available to the next forecast.
	_, err = s.store.ClaimSpeculativeInstance(s.adminCtx, []string{s.pool.ID}, 909)
	s.Require().NoError(err)
	s.Require().NotEmpty(speculative.Name)

	available, err = s.store.CountPoolAvailableCapacity(s.adminCtx, s.pool.ID)
	s.Require().NoError(err)
	s.Require().Equal(int64(1), available)
}

func (s *PrewarmTestSuite) TestPrewarmCounters() {
	request, _, err := s.store.CreatePrewarmRequest(s.adminCtx, s.createRequestParams())
	s.Require().NoError(err)

	s.Require().NoError(s.store.RecordPrewarmInstancesCreated(s.adminCtx, request.ID, spotLabelKey, 4))
	s.Require().NoError(s.store.RecordPrewarmInstanceClaimed(s.adminCtx, request.ID, spotLabelKey))
	s.Require().NoError(s.store.RecordPrewarmInstancesReaped(s.adminCtx, request.ID, spotLabelKey, 2))

	active, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	for _, target := range active[0].Targets {
		if target.LabelKey == spotLabelKey {
			s.Require().Equal(uint(4), target.CreatedCount)
			s.Require().Equal(uint(1), target.ClaimedCount)
			s.Require().Equal(uint(2), target.ReapedCount)
		}
	}
}

func TestPrewarmTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PrewarmTestSuite))
}
