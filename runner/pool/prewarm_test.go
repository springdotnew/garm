// Copyright 2025 Cloudbase Solutions SRL
//
//    Licensed under the Apache License, Version 2.0 (the "License"); you may
//    not use this file except in compliance with the License. You may obtain
//    a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//    License for the specific language governing permissions and limitations
//    under the License.

//go:build testing

package pool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/cache"
	"github.com/cloudbase/garm/config"
	"github.com/cloudbase/garm/database"
	dbCommon "github.com/cloudbase/garm/database/common"
	garmTesting "github.com/cloudbase/garm/internal/testing"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/metrics"
	"github.com/cloudbase/garm/params"
	"github.com/cloudbase/garm/runner/common"
	runnerCommonMocks "github.com/cloudbase/garm/runner/common/mocks"
)

const (
	prewarmTestWorkflow  = "PR Tests"
	prewarmTestGateJob   = "changes"
	prewarmTestRuleID    = "spring-pr-tests"
	prewarmTestCohortLen = 4
	prewarmTestQueued    = "queued"
)

// prewarmTestLabels is both the pool's tag set and the forecast target, which
// is what makes a speculative runner eligible for the fanout jobs.
var prewarmTestLabels = []string{"self-hosted", "linux", "x64"}

const (
	// prewarmTestBriefTTL is the shortest window a test can ask for and still
	// have the run it is testing happen inside it. A reconcile pass enforces the
	// window as it reads it, so a TTL shorter than the pass itself is not a fast
	// test — it is a forecast that was already over before anything looked at
	// it, which no real configuration can produce.
	prewarmTestBriefTTL = config.PrewarmTTL("300ms")
	prewarmTestPastTTL  = 400 * time.Millisecond
)

type PrewarmPoolTestSuite struct {
	suite.Suite

	store          dbCommon.Store
	adminCtx       context.Context
	entity         params.ForgeEntity
	pool           params.Pool
	controllerInfo params.ControllerInfo

	mgr          *basePoolManager
	providerMock *runnerCommonMocks.Provider
	ghcliMock    *runnerCommonMocks.GithubClient
}

func (s *PrewarmPoolTestSuite) SetupTest() {
	dbCfg := garmTesting.GetTestSqliteDBConfig(s.T())
	db, err := database.NewDatabase(context.Background(), dbCfg)
	s.Require().NoError(err)

	s.store = db
	s.adminCtx = garmTesting.ImpersonateAdminContext(context.Background(), db, s.T())

	endpoint := garmTesting.CreateDefaultGithubEndpoint(s.adminCtx, db, s.T())
	creds := garmTesting.CreateTestGithubCredentials(s.adminCtx, "prewarm-creds", db, s.T(), endpoint)

	repo, err := db.CreateRepository(
		s.adminCtx, "test-owner", "test-repo", creds,
		"test-webhook-secret", params.PoolBalancerTypeRoundRobin, false)
	s.Require().NoError(err)

	entity, err := repo.GetEntity()
	s.Require().NoError(err)
	s.entity = entity

	s.providerMock = runnerCommonMocks.NewProvider(s.T())
	s.ghcliMock = runnerCommonMocks.NewGithubClient(s.T())

	pool, err := db.CreateEntityPool(s.adminCtx, entity, params.CreatePoolParams{
		ProviderName:   "test-provider",
		MaxRunners:     50,
		MinIdleRunners: 0,
		Image:          "test-image",
		Flavor:         "test-flavor",
		OSType:         "linux",
		OSArch:         "amd64",
		Tags:           prewarmTestLabels,
		Enabled:        true,
	})
	s.Require().NoError(err)
	s.pool = pool

	cache.SetEntity(entity)
	cache.SetEntityPool(entity.ID, pool)

	controllerInfo, err := db.InitController()
	s.Require().NoError(err)
	s.controllerInfo = controllerInfo

	s.mgr = s.newPoolManager()

	s.providerMock.On("DisableJITConfig").Return(true).Maybe()
	s.ghcliMock.On("GetEntityJITConfig",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(map[string]string{}, nil, nil).Maybe()
	s.ghcliMock.On("RemoveEntityRunner", mock.Anything, mock.Anything).Return(nil).Maybe()
}

// newPoolManager builds a pool manager over the suite's store. Building a
// second one is how a test restarts the controller: the same database, and no
// memory of anything that happened before.
func (s *PrewarmPoolTestSuite) newPoolManager() *basePoolManager {
	s.T().Helper()

	backoff, err := locking.NewInstanceDeleteBackoff(context.Background())
	s.Require().NoError(err)

	return &basePoolManager{
		ctx:                     s.adminCtx,
		consumerID:              "prewarm-consumer",
		entity:                  s.entity,
		store:                   s.store,
		controllerInfo:          s.controllerInfo,
		providers:               map[string]common.Provider{"test-provider": s.providerMock},
		jobs:                    make(map[int64]params.Job),
		checkedJobs:             make(map[int64]time.Time),
		quit:                    make(chan struct{}),
		consumer:                &garmTesting.MockConsumer{},
		wg:                      &sync.WaitGroup{},
		backoff:                 backoff,
		ghcli:                   s.ghcliMock,
		managerIsRunning:        true,
		pendingInstancesTrigger: make(chan struct{}, 1),
		queuedJobsTrigger:       make(chan struct{}, 1),
		prewarmTrigger:          make(chan struct{}, 1),
		prewarmCfg:              s.prewarmConfig(config.PrewarmModeActive),
		publishedPrewarmTargets: make(map[prewarmSeries]uint64),
		pendingPrewarmWakes:     make(map[int64]struct{}),
	}
}

func (s *PrewarmPoolTestSuite) TearDownTest() {
	s.mgr.mux.Lock()
	s.mgr.stopQueuedJobsWakeLocked()
	s.mgr.mux.Unlock()
}

func (s *PrewarmPoolTestSuite) prewarmConfig(mode config.PrewarmMode) config.Prewarm {
	return config.Prewarm{
		Enable:                true,
		Mode:                  mode,
		MaxSpeculativeRunners: 100,
		Rules: []config.PrewarmRule{
			{
				ID:         prewarmTestRuleID,
				Repository: "test-owner/test-repo",
				Workflow:   prewarmTestWorkflow,
				TriggerJob: prewarmTestGateJob,
				Targets: []config.PrewarmTarget{
					{Labels: prewarmTestLabels, Count: prewarmTestCohortLen},
				},
			},
		},
	}
}

// gateJob is the webhook that a rule watches for: the first job of the run,
// whose completion is what releases the fanout.
func (s *PrewarmPoolTestSuite) gateJob(jobID, runID int64) params.WorkflowJob {
	return s.workflowJob(jobID, runID, prewarmTestGateJob, prewarmTestWorkflow)
}

func (s *PrewarmPoolTestSuite) workflowJob(jobID, runID int64, name, workflow string) params.WorkflowJob {
	wj := params.WorkflowJob{Action: prewarmTestQueued}
	wj.WorkflowJob.ID = jobID
	wj.WorkflowJob.RunID = runID
	wj.WorkflowJob.RunAttempt = 1
	wj.WorkflowJob.Name = name
	wj.WorkflowJob.WorkflowName = workflow
	wj.WorkflowJob.Status = prewarmTestQueued
	wj.WorkflowJob.Labels = prewarmTestLabels
	wj.Repository.Name = "test-repo"
	wj.Repository.Owner.Login = "test-owner"
	wj.Repository.HTMLURL = "https://github.com/test-owner/test-repo"
	return wj
}

func (s *PrewarmPoolTestSuite) speculativeInstances() []params.Instance {
	instances, err := s.store.ListPoolInstances(s.adminCtx, s.pool.ID, false)
	s.Require().NoError(err)

	speculative := make([]params.Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.Speculative {
			speculative = append(speculative, instance)
		}
	}
	return speculative
}

func (s *PrewarmPoolTestSuite) allInstances() []params.Instance {
	instances, err := s.store.ListPoolInstances(s.adminCtx, s.pool.ID, false)
	s.Require().NoError(err)
	return instances
}

// prewarmWithExpiredCohort prewarms a cohort whose forecast window has already
// closed by the time it returns. The TTL is real rather than backdated, so the
// reaper is exercised through the same expiry path production uses.
func (s *PrewarmPoolTestSuite) prewarmWithExpiredCohort(jobID, runID int64) []params.Instance {
	s.T().Helper()

	s.mgr.prewarmCfg.DefaultTTL = prewarmTestBriefTTL
	s.recordGateJob(jobID, runID)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	speculative := s.speculativeInstances()
	s.Require().Len(speculative, prewarmTestCohortLen)

	for _, instance := range speculative {
		s.Require().NotNil(instance.SpeculativeExpiresAt)
	}
	time.Sleep(prewarmTestPastTTL)

	return speculative
}

// bootRunnerToIdle walks a runner through the status transitions the agent
// reports as it comes up. The store rejects shortcuts, so the test cannot take
// one either.
func (s *PrewarmPoolTestSuite) bootRunnerToIdle(name string) {
	s.T().Helper()

	for _, step := range []params.UpdateInstanceParams{
		{Status: commonParams.InstanceCreating},
		{Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerInstalling},
		{RunnerStatus: params.RunnerIdle},
	} {
		_, err := s.store.UpdateInstance(s.adminCtx, name, step)
		s.Require().NoError(err)
	}
}

func instanceNames(instances []params.Instance) []string {
	names := make([]string, 0, len(instances))
	for _, instance := range instances {
		names = append(names, instance.Name)
	}
	return names
}

// pendingPrewarmWakes reports the forecasts still waiting on their trigger job.
func (s *PrewarmPoolTestSuite) pendingPrewarmWakes() map[int64]struct{} {
	s.mgr.pendingPrewarmWakesMux.Lock()
	defer s.mgr.pendingPrewarmWakesMux.Unlock()

	pending := make(map[int64]struct{}, len(s.mgr.pendingPrewarmWakes))
	for jobID := range s.mgr.pendingPrewarmWakes {
		pending[jobID] = struct{}{}
	}
	return pending
}

// recordGateJob delivers a gate job's webhook and then arms the forecast it
// produced, which together are what actually happens in production: the webhook
// records the forecast, and the queued-job consumer arms it once it has given
// that job a runner.
//
// Tests that care about the window *between* those two steps — a forecast that
// exists but is not yet servable — call HandleWorkflowJob directly and do not
// arm. That window is the whole ordering guarantee, so it is worth being able to
// write both.
func (s *PrewarmPoolTestSuite) recordGateJob(jobID, runID int64) {
	s.T().Helper()
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(jobID, runID)))
	s.armForecastsFor(jobID)
}

// armForecastsFor does what the consumer's deferred release does, without
// needing a full consume pass.
func (s *PrewarmPoolTestSuite) armForecastsFor(triggerJobID int64) {
	s.T().Helper()
	s.Require().NoError(
		s.store.ArmPrewarmRequests(s.adminCtx, s.entity.ID, triggerJobID, time.Now()))
}

// syncJobsFromDB mirrors what the watcher does for the queued-job consumer.
func (s *PrewarmPoolTestSuite) syncJobsFromDB() {
	allJobs, err := s.store.ListAllJobs(s.adminCtx)
	s.Require().NoError(err)

	s.mgr.mux.Lock()
	defer s.mgr.mux.Unlock()
	for k := range s.mgr.jobs {
		delete(s.mgr.jobs, k)
	}
	for _, j := range allJobs {
		s.mgr.jobs[j.WorkflowJobID] = j
	}
}

func (s *PrewarmPoolTestSuite) TestGateJobPrewarmsTheForecastCohort() {
	s.recordGateJob(1, 100)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1)
	s.Equal(prewarmTestRuleID, requests[0].RuleID)
	s.Equal(params.PrewarmRequestActive, requests[0].State)
	s.Require().Len(requests[0].Targets, 1)
	s.EqualValues(prewarmTestCohortLen, requests[0].Targets[0].TargetCount)

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen)
}

// The gate job is real work. It must be served by the ordinary queued-job path
// before any speculation happens, never out of the cohort it just forecast.
func (s *PrewarmPoolTestSuite) TestGateJobIsServedBeforeSpeculation() {
	s.recordGateJob(1, 100)

	s.syncJobsFromDB()
	s.Require().NoError(s.mgr.consumeQueuedJobs())

	instances := s.allInstances()
	s.Require().Len(instances, 1, "gate job should be served by exactly one runner")
	s.False(instances[0].Speculative, "gate job must not consume its own forecast")

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen)
	s.Len(s.allInstances(), prewarmTestCohortLen+1)
}

// The previous test hand-sequences the two halves, so it proves the *outcome*
// is right when the consumer happens to run first — not that the runtime makes
// it run first. It did not: the real path is woken through the job backoff
// (a second on our controllers) while speculation was woken inline, so on a live
// controller the first speculative provider call beat the gate job's
// reservation by ~800ms. This pins the ordering itself.
func (s *PrewarmPoolTestSuite) TestSpeculationWaitsForTheGateJobsPass() {
	s.mgr.mux.Lock()
	s.mgr.controllerInfo.MinimumJobAgeBackoff = 3600
	s.mgr.mux.Unlock()

	// Deliberately not recordGateJob: this test is about the window before the
	// forecast is armed, so arming it up front would assert nothing.
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

	s.Empty(s.mgr.prewarmTrigger, "recording a forecast must not wake speculation by itself")
	s.Len(s.pendingPrewarmWakes(), 1, "the forecast should be waiting on its gate job")

	// The wake is only an optimisation, so the guarantee cannot rest on it. A
	// reconcile that fires for any other reason — the free-running consolidation
	// ticker, another entity's wake — must still create nothing.
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.speculativeInstances(), "an unarmed forecast must be invisible to the reconcile loop")

	// A pass that sees the gate job but leaves it inside its backoff has not
	// served it, so the forecast stays unarmed.
	s.syncJobsFromDB()
	s.Require().NoError(s.mgr.consumeQueuedJobs())
	s.Empty(s.mgr.prewarmTrigger, "speculation must not be woken while the gate job is still backed off")
	s.Len(s.pendingPrewarmWakes(), 1)

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.speculativeInstances(), "a backed-off gate job must not release its own forecast")

	// The pass that reaches the job releases it.
	s.mgr.mux.Lock()
	s.mgr.controllerInfo.MinimumJobAgeBackoff = 0
	s.mgr.mux.Unlock()

	s.Require().NoError(s.mgr.consumeQueuedJobs())
	s.Len(s.mgr.prewarmTrigger, 1, "serving the gate job must wake speculation")
	s.Empty(s.pendingPrewarmWakes())

	instances := s.allInstances()
	s.Require().Len(instances, 1, "gate job should be served by exactly one runner")
	s.False(instances[0].Speculative, "gate job must not consume its own forecast")
}

// A forecast whose trigger job never comes back — a cancelled run, a job that
// completed while the webhook was in flight — must not pin speculation forever.
func (s *PrewarmPoolTestSuite) TestForecastIsReleasedWhenTheGateJobVanishes() {
	s.recordGateJob(1, 100)
	s.Require().Len(s.pendingPrewarmWakes(), 1)

	// The consumer sees an empty queue: whatever happened to the job, it is not
	// waiting on this pass.
	s.mgr.mux.Lock()
	for k := range s.mgr.jobs {
		delete(s.mgr.jobs, k)
	}
	s.mgr.mux.Unlock()

	s.Require().NoError(s.mgr.consumeQueuedJobs())
	s.Empty(s.pendingPrewarmWakes(), "a forecast must not stay pinned to a job that is gone")
	s.Len(s.mgr.prewarmTrigger, 1)
}

// The scale-set worker never sees the pool manager's wake map — it autoscales
// off its own ticker, reading SumRemainingPrewarmForecast straight from the
// store. That is how a live controller scheduled ten scale-set creations 1.462s
// *before* the gate job's runner was reserved, on a binary whose pool half was
// correctly ordered. The guarantee therefore has to live in the row, and this
// pins the query the scale set actually calls.
func (s *PrewarmPoolTestSuite) TestScaleSetForecastIsInvisibleUntilArmed() {
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

	target := s.mgr.prewarmCfg.Rules[0].Targets[0]
	labelKey := target.LabelKey()

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Empty(requests, "an unarmed forecast must not be listed as active")

	remaining, err := s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, labelKey)
	s.Require().NoError(err)
	s.Zero(remaining, "scale sets must see no forecast before it is armed")

	s.armForecastsFor(1)

	remaining, err = s.store.SumRemainingPrewarmForecast(s.adminCtx, s.entity.ID, labelKey)
	s.Require().NoError(err)
	s.EqualValues(prewarmTestCohortLen, remaining,
		"arming has to make the whole forecast visible, or this test proves nothing")
}

// Arming is a store write precisely so it survives the process that made it. A
// controller that dies after arming must resume serving the cohort rather than
// stranding it.
func (s *PrewarmPoolTestSuite) TestArmingSurvivesARestart() {
	s.recordGateJob(1, 100)

	restarted := s.newPoolManager()
	s.Require().NoError(restarted.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen,
		"a restarted controller must serve a forecast that was already armed")
}

func (s *PrewarmPoolTestSuite) TestDuplicateWebhookDeliveriesCreateOneCohort() {
	gate := s.gateJob(1, 100)
	for range 3 {
		s.Require().NoError(s.mgr.HandleWorkflowJob(gate))
	}
	s.armForecastsFor(1)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1)

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestConcurrentWebhookDeliveriesCreateOneCohort() {
	job, err := s.mgr.paramsWorkflowJobToParamsJob(s.gateJob(1, 100))
	s.Require().NoError(err)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.mgr.handlePrewarmForQueuedJob(s.adminCtx, job)
		}()
	}
	wg.Wait()
	s.armForecastsFor(1)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Len(requests, 1)
}

// Reconcile is a convergence loop, not a create loop: it fills the gap between
// the forecast and the capacity a pool already has. This is also what makes a
// controller restart safe — the loop simply re-measures.
func (s *PrewarmPoolTestSuite) TestExistingCapacityReducesTheDeficit() {
	for i := range 2 {
		_, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
			Name:         fmt.Sprintf("idle-runner-%d", i),
			OSType:       "linux",
			OSArch:       "amd64",
			Status:       commonParams.InstanceRunning,
			RunnerStatus: params.RunnerIdle,
		})
		s.Require().NoError(err)
	}

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Len(s.speculativeInstances(), prewarmTestCohortLen-2)
	s.Len(s.allInstances(), prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestReconcileIsIdempotent() {
	s.recordGateJob(1, 100)

	for range 3 {
		s.Require().NoError(s.mgr.reconcilePrewarm())
	}

	s.Len(s.speculativeInstances(), prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestShadowModeCreatesNoRunners() {
	s.mgr.prewarmCfg = s.prewarmConfig(config.PrewarmModeShadow)

	s.recordGateJob(1, 100)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1, "shadow mode still records the forecast it would have acted on")
	s.Equal(params.PrewarmRequestShadow, requests[0].State)

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.allInstances(), "shadow mode must not create a single VM")
}

// In shadow the request log line is the whole of what an operator gets, so it
// has to carry the forecast rather than merely announce that there was one.
func (s *PrewarmPoolTestSuite) TestForecastIsRenderedForTheRequestLogLine() {
	s.Equal("", formatForecast(nil))
	s.Equal("linux,self-hosted,x64=4", formatForecast([]params.CreatePrewarmTargetParams{
		{LabelKey: "linux,self-hosted,x64", TargetCount: 4},
	}))
	// Configuration order, not sorted: the line should read like the rule it
	// came from.
	s.Equal("gcp-2vcpu=17 gcp-4vcpu=81 gcp-8vcpu=10", formatForecast(
		[]params.CreatePrewarmTargetParams{
			{LabelKey: "gcp-2vcpu", TargetCount: 17},
			{LabelKey: "gcp-4vcpu", TargetCount: 81},
			{LabelKey: "gcp-8vcpu", TargetCount: 10},
		}))
}

// Shadow mode exists to be read: an operator compares its forecast against the
// fanout that actually queued before switching a rule on. A shadow request that
// publishes nothing is a silent dry run, which is no use to anyone.
func (s *PrewarmPoolTestSuite) TestShadowModePublishesTheForecastItWouldHaveActedOn() {
	s.mgr.prewarmCfg = s.prewarmConfig(config.PrewarmModeShadow)

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Empty(s.allInstances())
	s.EqualValues(prewarmTestCohortLen,
		s.targetRunnersMetric(config.NormalizeLabelKey(prewarmTestLabels), s.pool.ID))
}

// targetRunnersMetric reads the gauge an operator is told to compare against
// the real fanout. Read through dto rather than prometheus' testutil, which is
// not vendored and is not worth vendoring for one assertion.
func (s *PrewarmPoolTestSuite) targetRunnersMetric(labelKey, poolID string) float64 {
	s.T().Helper()

	var metric dto.Metric
	s.Require().NoError(
		metrics.PrewarmTargetRunners.WithLabelValues(labelKey, poolID).Write(&metric))
	return metric.GetGauge().GetValue()
}

// A forecast whose window has closed is not a forecast. The reconciler runs on
// the consolidation interval and the reaper — the only thing that flips an
// expired request out of the list — runs on the reap interval, so between the
// two there is a window minutes wide in which an expired request still reads as
// live. Buying machines in it spends real money on a prediction that has
// already run out.
func (s *PrewarmPoolTestSuite) TestExpiredForecastCreatesNothing() {
	s.mgr.prewarmCfg.DefaultTTL = prewarmTestBriefTTL

	s.recordGateJob(1, 100)
	time.Sleep(prewarmTestPastTTL)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1, "the reaper has not run, so the request is still listed as live")

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.allInstances())
}

// The gauge is only ever read after the fact, so one that keeps reporting the
// last thing it saw is worse than one that was never published: it says a
// forecast is still unmet long after its window closed. doc/prewarm.md asks
// operators to size a rule by comparing this gauge against the fanout that
// actually queued, and a reading that never comes down makes that comparison
// wrong in the direction that costs money.
func (s *PrewarmPoolTestSuite) TestForecastGaugeFallsBackToZeroWhenTheWindowCloses() {
	s.mgr.prewarmCfg = s.prewarmConfig(config.PrewarmModeShadow)
	s.mgr.prewarmCfg.DefaultTTL = prewarmTestBriefTTL
	labelKey := config.NormalizeLabelKey(prewarmTestLabels)

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Require().EqualValues(prewarmTestCohortLen, s.targetRunnersMetric(labelKey, s.pool.ID))

	time.Sleep(prewarmTestPastTTL)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Zero(s.targetRunnersMetric(labelKey, s.pool.ID))
}

// The row being reaped is the same event as the window closing, one reap
// interval later, and the gauge has to survive both. Once the request is gone
// there is nothing left to publish from, which is exactly when a gauge that
// only ever gets raised is stuck for good.
func (s *PrewarmPoolTestSuite) TestForecastGaugeFallsBackToZeroOnceTheRequestIsReaped() {
	s.mgr.prewarmCfg = s.prewarmConfig(config.PrewarmModeShadow)
	s.mgr.prewarmCfg.DefaultTTL = prewarmTestBriefTTL
	labelKey := config.NormalizeLabelKey(prewarmTestLabels)

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Require().EqualValues(prewarmTestCohortLen, s.targetRunnersMetric(labelKey, s.pool.ID))

	time.Sleep(prewarmTestPastTTL)
	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Empty(requests, "the reaper has flipped the request out of the live list")

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Zero(s.targetRunnersMetric(labelKey, s.pool.ID))
}

func (s *PrewarmPoolTestSuite) TestDisabledPrewarmIsANoOp() {
	s.mgr.prewarmCfg = config.Prewarm{}

	s.recordGateJob(1, 100)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Empty(requests)

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.allInstances())
}

// startSpeculativeReaperOrFail runs the reaper entry point and fails the test if
// it does not return. With prewarming disabled it must drain and stop; anything
// that keeps ticking would hang here for the reap interval instead of reporting
// the defect, which is the opposite of what this suite is for.
func (s *PrewarmPoolTestSuite) startSpeculativeReaperOrFail() {
	s.T().Helper()

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s.mgr.startSpeculativeReaper()
	}()

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		s.FailNow("startSpeculativeReaper did not return with prewarming disabled; it is looping")
	}
}

// Every controller in the fleet runs this code with the feature switched off.
// The reap loop's first act is a write, so leaving it up on a controller that
// never turns prewarming on would be a periodic database write forever on
// behalf of runners that cannot exist.
func (s *PrewarmPoolTestSuite) TestDisabledPrewarmStartsNoReapLoop() {
	s.mgr.prewarmCfg = config.Prewarm{}

	s.startSpeculativeReaperOrFail()
}

// The other half of that trade: a controller whose configuration turned
// prewarming off has to hand back what the old configuration was holding. These
// runners are nowhere near their expiry and no job will ever claim them again,
// so waiting the TTL out is time nobody uses and money nobody gets back.
func (s *PrewarmPoolTestSuite) TestDisablingPrewarmDrainsTheRunnersItLeftBehind() {
	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	for _, instance := range s.speculativeInstances() {
		s.Require().NotNil(instance.SpeculativeExpiresAt)
		s.Require().True(instance.SpeculativeExpiresAt.After(time.Now()), "the TTL must still be running")
	}

	s.mgr.prewarmCfg = config.Prewarm{}
	s.startSpeculativeReaperOrFail()

	speculative := s.speculativeInstances()
	s.Require().Len(speculative, prewarmTestCohortLen)
	for _, instance := range speculative {
		s.Equal(commonParams.InstancePendingDelete, instance.Status)
	}
}

// A drain that could not remove a runner must say so and try again rather than
// return and leave it billing. The pool manager not being running yet is the
// real case: removal is refused until the entity's first tools update lands.
func (s *PrewarmPoolTestSuite) TestDrainReportsRunnersItCouldNotReclaim() {
	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Require().Len(s.speculativeInstances(), prewarmTestCohortLen)

	s.mgr.prewarmCfg = config.Prewarm{}
	s.mgr.SetPoolRunningState(false, "not running yet")

	stranded, err := s.mgr.drainSpeculativeSurplus()
	s.Require().NoError(err)
	s.Equal(prewarmTestCohortLen, stranded, "a runner that could not be removed is not drained")

	s.mgr.SetPoolRunningState(true, "")
	stranded, err = s.mgr.drainSpeculativeSurplus()
	s.Require().NoError(err)
	s.Zero(stranded, "the retry reclaims what the first sweep could not")
	for _, instance := range s.speculativeInstances() {
		s.Equal(commonParams.InstancePendingDelete, instance.Status)
	}
}

func (s *PrewarmPoolTestSuite) TestKillSwitchStopsCreationWithoutAConfigChange() {
	s.recordGateJob(1, 100)

	paused := true
	updated, err := s.store.UpdateController(params.UpdateControllerParams{
		PrewarmPaused: &paused,
	})
	s.Require().NoError(err)
	s.Require().True(updated.PrewarmPaused)

	s.mgr.mux.Lock()
	s.mgr.controllerInfo = updated
	s.mgr.mux.Unlock()

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.allInstances(), "a paused controller must not create speculative capacity")
	s.True(s.mgr.prewarmCfg.Enable, "the kill switch must not need a config change")
}

// The switch is only a kill switch if it kills the right thing, and the test
// above does not check that: asserting prewarmCfg.Enable is a statement about a
// config field, not about whether CI still gets runners. The distinction is the
// whole risk. An operator pulls this during an incident, and a switch that also
// stops the ordinary queued-job path turns a capacity problem into an outage.
func (s *PrewarmPoolTestSuite) TestKillSwitchLeavesRealQueuedJobsAlone() {
	paused := true
	updated, err := s.store.UpdateController(params.UpdateControllerParams{
		PrewarmPaused: &paused,
	})
	s.Require().NoError(err)
	s.Require().True(updated.PrewarmPaused)

	s.mgr.mux.Lock()
	// Take the flag, not the whole row. The stored controller carries a 30s
	// MinimumJobAgeBackoff and the manager's copy does not, so adopting the row
	// wholesale would park both jobs inside the backoff — and this test would
	// then pass for the wrong reason: nothing created because nothing was due,
	// rather than because the switch was thrown. The assertion below pins the
	// precondition so a future change to either default fails here loudly.
	s.Require().Zero(s.mgr.controllerInfo.MinimumJobAgeBackoff)
	s.mgr.controllerInfo.PrewarmPaused = updated.PrewarmPaused
	s.mgr.mux.Unlock()

	// The gate job is what would have armed speculation and the fanout job is
	// what speculation would have served. Paused, both are ordinary work and both
	// have to be served as such.
	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.workflowJob(2, 100, "build", prewarmTestWorkflow)))

	s.syncJobsFromDB()
	s.Require().NoError(s.mgr.consumeQueuedJobs())

	instances := s.allInstances()
	s.Require().Len(instances, 2, "both queued jobs must still be given a runner while prewarming is paused")
	for _, instance := range instances {
		s.False(instance.Speculative, "a paused controller must serve real jobs from the ordinary path")
	}

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.speculativeInstances(), "pausing has to hold across a reconcile too")
}

// Pausing is meant to be legible: an operator who has just pulled the emergency
// switch reads the same gauge to confirm it took. One still reporting the last
// forecast says capacity is on its way that nothing is going to create.
func (s *PrewarmPoolTestSuite) TestKillSwitchTakesTheForecastGaugeDown() {
	labelKey := config.NormalizeLabelKey(prewarmTestLabels)

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Require().EqualValues(prewarmTestCohortLen, s.targetRunnersMetric(labelKey, s.pool.ID))

	paused := true
	updated, err := s.store.UpdateController(params.UpdateControllerParams{
		PrewarmPaused: &paused,
	})
	s.Require().NoError(err)

	s.mgr.mux.Lock()
	s.mgr.controllerInfo = updated
	s.mgr.mux.Unlock()

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Zero(s.targetRunnersMetric(labelKey, s.pool.ID))
}

func (s *PrewarmPoolTestSuite) TestGlobalCapBoundsTheCohort() {
	s.mgr.prewarmCfg.MaxSpeculativeRunners = 2

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Len(s.speculativeInstances(), 2)

	// A second run must not push past the cap either.
	s.recordGateJob(2, 200)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Len(s.speculativeInstances(), 2)
}

// The runners of one cohort are reserved concurrently, not one after another.
// Observed rather than timed: a reservation holds its slot across the JIT-config
// call, so several of those being open at once is only reachable if several
// reservations are in flight. If they were serialized, one would park and the
// rest would never arrive.
func (s *PrewarmPoolTestSuite) TestCohortReservesConcurrently() {
	const cohort = queuedJobReservationConcurrency

	// Fresh mocks: the suite's own expectations are unlimited, so they would
	// win over anything registered here.
	provider := runnerCommonMocks.NewProvider(s.T())
	provider.On("DisableJITConfig").Return(false).Maybe()
	s.mgr.providers["test-provider"] = provider

	entered := make(chan struct{}, cohort)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	ghcli := runnerCommonMocks.NewGithubClient(s.T())
	ghcli.On("GetEntityJITConfig",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(mock.Arguments) {
		entered <- struct{}{}
		<-release
	}).Return(map[string]string{"encoded_jit_config": "test"}, nil, nil).Maybe()
	ghcli.On("RemoveEntityRunner", mock.Anything, mock.Anything).Return(nil).Maybe()
	s.mgr.ghcli = ghcli

	s.mgr.prewarmCfg.Rules[0].Targets = []config.PrewarmTarget{
		{Labels: prewarmTestLabels, Count: cohort},
	}

	s.recordGateJob(1, 100)

	reconciled := make(chan error, 1)
	go func() { reconciled <- s.mgr.reconcilePrewarm() }()

	for held := 0; held < cohort; held++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			releaseOnce.Do(func() { close(release) })
			s.FailNowf("the cohort is reserving serially",
				"only %d of %d reservations were in flight at once", held, cohort)
		}
	}
	releaseOnce.Do(func() { close(release) })
	s.Require().NoError(<-reconciled)
	s.Len(s.speculativeInstances(), cohort)
}

// MaxRunners is enforced inside poolReservationLimiter, which the cohort now
// takes from several goroutines at once. A forecast larger than the pool must
// still stop at the ceiling instead of racing past it.
func (s *PrewarmPoolTestSuite) TestCohortStopsAtTheMaxRunnersCeiling() {
	const ceiling = 3

	cappedLabels := []string{"self-hosted", "linux", "capped"}
	capped, err := s.store.CreateEntityPool(s.adminCtx, s.entity, params.CreatePoolParams{
		ProviderName:   "test-provider",
		MaxRunners:     ceiling,
		MinIdleRunners: 0,
		Image:          "test-image",
		Flavor:         "test-flavor",
		OSType:         "linux",
		OSArch:         "amd64",
		Tags:           cappedLabels,
		Enabled:        true,
	})
	s.Require().NoError(err)
	cache.SetEntityPool(s.entity.ID, capped)

	s.mgr.prewarmCfg.Rules[0].Targets = []config.PrewarmTarget{
		{Labels: cappedLabels, Count: 4 * ceiling},
	}

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	instances, err := s.store.ListPoolInstances(s.adminCtx, capped.ID, false)
	s.Require().NoError(err)
	s.Len(instances, ceiling, "the cohort raced past the pool's MaxRunners")
}

// Cohorts are created concurrently, one goroutine per target, so the global cap
// is the one thing that has to be settled before any of them starts. Two targets
// sizing themselves against the same in-flight count would each take the whole
// headroom, and the cap would bind neither.
func (s *PrewarmPoolTestSuite) TestGlobalCapBindsAcrossConcurrentTargets() {
	secondLabels := []string{"self-hosted", "linux", "arm64"}
	second, err := s.store.CreateEntityPool(s.adminCtx, s.entity, params.CreatePoolParams{
		ProviderName:   "test-provider",
		MaxRunners:     50,
		MinIdleRunners: 0,
		Image:          "test-image",
		Flavor:         "test-flavor",
		OSType:         "linux",
		OSArch:         "arm64",
		Tags:           secondLabels,
		Enabled:        true,
	})
	s.Require().NoError(err)
	cache.SetEntityPool(s.entity.ID, second)

	cfg := s.prewarmConfig(config.PrewarmModeActive)
	cfg.MaxSpeculativeRunners = 5
	cfg.Rules[0].Targets = []config.PrewarmTarget{
		{Labels: prewarmTestLabels, Count: prewarmTestCohortLen},
		{Labels: secondLabels, Count: prewarmTestCohortLen},
	}
	s.mgr.prewarmCfg = cfg

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	total, err := s.store.CountSpeculativeInstances(s.adminCtx)
	s.Require().NoError(err)
	s.EqualValues(5, total, "two targets asking for %d each must still stop at the cap", prewarmTestCohortLen)

	// And the cap keeps binding once it is reached, rather than being a
	// first-pass-only budget.
	s.Require().NoError(s.mgr.reconcilePrewarm())
	total, err = s.store.CountSpeculativeInstances(s.adminCtx)
	s.Require().NoError(err)
	s.EqualValues(5, total)
}

// A controller that goes down partway through a cohort must finish it on the
// way back up, not start it again. Nothing in memory survives the restart, so
// the only thing standing between a resumed forecast and a doubled bill is that
// the runners already created count against the pool's capacity.
func (s *PrewarmPoolTestSuite) TestRestartMidCohortResumesWithoutDuplicating() {
	// Interrupt the cohort halfway, which is what a controller dying mid-cohort
	// leaves behind: some runners created, the forecast still asking for all of
	// them.
	const interrupted = prewarmTestCohortLen / 2
	s.mgr.prewarmCfg.MaxSpeculativeRunners = interrupted

	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Require().Len(s.speculativeInstances(), interrupted)

	resumed := s.newPoolManager()
	s.Require().NoError(resumed.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen,
		"the restarted controller re-created runners it already had")

	// And it settles there rather than topping up again on every pass.
	s.Require().NoError(resumed.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestOverlappingRunsShareCapacity() {
	s.recordGateJob(1, 100)
	s.recordGateJob(2, 200)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Len(s.speculativeInstances(), 2*prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestQueuedFanoutJobClaimsPrewarmedCapacity() {
	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Require().Len(s.speculativeInstances(), prewarmTestCohortLen)

	before := len(s.allInstances())

	fanout := s.workflowJob(2, 100, "build", prewarmTestWorkflow)
	s.Require().NoError(s.mgr.HandleWorkflowJob(fanout))
	s.syncJobsFromDB()
	s.Require().NoError(s.mgr.consumeQueuedJobs())

	s.Len(s.allInstances(), before, "a claimed forecast must not be topped up with a cold runner")

	claimed := 0
	for _, instance := range s.speculativeInstances() {
		if instance.ReservedForWorkflowJobID != nil && *instance.ReservedForWorkflowJobID == 2 {
			claimed++
		}
	}
	s.Equal(1, claimed, "exactly one speculative runner should be reserved for the job")
}

// Each queued job consumes one unit of the forecast made for it, so the fanout
// GitHub actually queues shrinks the speculation rather than adding to it.
func (s *PrewarmPoolTestSuite) TestQueuedFanoutConsumesTheForecast() {
	s.recordGateJob(1, 100)

	for jobID := int64(2); jobID <= 3; jobID++ {
		s.Require().NoError(s.mgr.HandleWorkflowJob(s.workflowJob(jobID, 100, "build", prewarmTestWorkflow)))
	}

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1)
	s.Require().Len(requests[0].Targets, 1)
	s.EqualValues(2, requests[0].Targets[0].ObservedDemand)
	s.EqualValues(prewarmTestCohortLen-2, requests[0].Targets[0].RemainingForecast())

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen-2)
}

func (s *PrewarmPoolTestSuite) TestScaleDownLeavesLiveSpeculativeRunnersAlone() {
	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())

	for _, instance := range s.speculativeInstances() {
		s.bootRunnerToIdle(instance.Name)
	}

	s.Require().NoError(s.mgr.scaleDownOnePool(s.adminCtx, s.pool))

	for _, instance := range s.speculativeInstances() {
		s.NotEqual(commonParams.InstancePendingDelete, instance.Status,
			"scale down must not reclaim capacity that is idle on purpose")
	}
}

func (s *PrewarmPoolTestSuite) TestReaperLeavesUnexpiredRunnersAlone() {
	s.recordGateJob(1, 100)
	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Require().Len(s.speculativeInstances(), prewarmTestCohortLen)

	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	for _, instance := range s.speculativeInstances() {
		s.NotEqual(commonParams.InstancePendingDelete, instance.Status,
			"capacity inside its forecast window must be left alone")
	}
}

func (s *PrewarmPoolTestSuite) TestReaperNeverRemovesAnActiveRunner() {
	speculative := s.prewarmWithExpiredCohort(1, 100)

	// A runner GitHub picked up is real work, whatever the forecast intended.
	active := speculative[0]
	_, err := s.store.UpdateInstance(s.adminCtx, active.Name, params.UpdateInstanceParams{
		RunnerStatus: params.RunnerActive,
	})
	s.Require().NoError(err)

	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	survivor, err := s.store.GetInstance(s.adminCtx, active.Name)
	s.Require().NoError(err)
	s.NotEqual(commonParams.InstancePendingDelete, survivor.Status,
		"an active runner must never be reaped")

	reaped := 0
	for _, instance := range speculative[1:] {
		current, err := s.store.GetInstance(s.adminCtx, instance.Name)
		s.Require().NoError(err)
		if current.Status == commonParams.InstancePendingDelete {
			reaped++
		}
	}
	s.Equal(prewarmTestCohortLen-1, reaped, "every other expired runner should be reaped")
}

func (s *PrewarmPoolTestSuite) TestReaperNeverRemovesAClaimedRunner() {
	speculative := s.prewarmWithExpiredCohort(1, 100)

	claimed, err := s.store.ClaimSpeculativeInstance(
		s.adminCtx, []string{s.pool.ID}, 4242)
	s.Require().NoError(err)
	s.Require().Contains(instanceNames(speculative), claimed.Name)

	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	survivor, err := s.store.GetInstance(s.adminCtx, claimed.Name)
	s.Require().NoError(err)
	s.NotEqual(commonParams.InstancePendingDelete, survivor.Status,
		"a runner a job is waiting on must never be reaped")
}

// A reap has to be filed under the target the runner was created for. A
// speculative runner carries no additional labels of its own, so the only
// honest source for that is the pool it lives in.
func (s *PrewarmPoolTestSuite) TestReapIsRecordedAgainstItsTarget() {
	s.recordGateJob(1, 100)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1)

	past := time.Now().Add(-time.Hour)
	for i := range 2 {
		_, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
			Name:                 fmt.Sprintf("stale-speculative-%d", i),
			OSType:               "linux",
			OSArch:               "amd64",
			Status:               commonParams.InstanceRunning,
			RunnerStatus:         params.RunnerIdle,
			Speculative:          true,
			SpeculativeRequestID: requests[0].ID,
			SpeculativeExpiresAt: &past,
		})
		s.Require().NoError(err)
	}

	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	requests, err = s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1, "the request window itself has not closed")
	s.Require().Len(requests[0].Targets, 1)
	s.EqualValues(2, requests[0].Targets[0].ReapedCount)
}

// The reaper has to keep working after prewarming is switched off in the
// config, otherwise disabling it would strand the runners already in flight.
//
// Named for what it does. It used to be called ...AfterThePrewarmIsPaused,
// which described the test below instead — a reader checking that the kill
// switch drains would have found this, seen a green test, and moved on.
func (s *PrewarmPoolTestSuite) TestReaperDrainsAfterPrewarmIsDisabled() {
	s.prewarmWithExpiredCohort(1, 100)

	s.mgr.prewarmCfg.Enable = false
	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	for _, instance := range s.speculativeInstances() {
		s.Equal(commonParams.InstancePendingDelete, instance.Status)
	}
}

// And the same for the kill switch, which is a different control: the config
// still says enable = true, only the controller row says paused.
func (s *PrewarmPoolTestSuite) TestReaperDrainsAfterTheKillSwitchIsThrown() {
	s.prewarmWithExpiredCohort(1, 100)

	paused := true
	updated, err := s.store.UpdateController(params.UpdateControllerParams{
		PrewarmPaused: &paused,
	})
	s.Require().NoError(err)

	s.mgr.mux.Lock()
	s.mgr.controllerInfo = updated
	s.mgr.mux.Unlock()

	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	for _, instance := range s.speculativeInstances() {
		s.Equal(commonParams.InstancePendingDelete, instance.Status)
	}
	s.True(s.mgr.prewarmCfg.Enable, "the kill switch must not need a config change")
}

// A scale set target looks like this from the pool manager: a label set no
// pool serves. Its own worker owns it, so this must be a quiet skip rather than
// a failure that shows up as an error on every reconcile.
func (s *PrewarmPoolTestSuite) TestTargetThatAddressesNoPoolIsLeftToItsOwner() {
	cfg := s.prewarmConfig(config.PrewarmModeActive)
	cfg.Rules[0].Targets = []config.PrewarmTarget{
		{Labels: []string{"bench-scaleset"}, Count: prewarmTestCohortLen},
	}
	s.mgr.prewarmCfg = cfg

	s.recordGateJob(1, 100)

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1, "the forecast is still recorded for whoever serves it")

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.allInstances(), "the pool manager must not invent a pool for it")
}

// Putting runners somewhere the operator did not choose is worse than not
// prewarming, so an ambiguous target is refused rather than guessed.
func (s *PrewarmPoolTestSuite) TestAmbiguousTargetIsRefused() {
	_, err := s.store.CreateEntityPool(s.adminCtx, s.entity, params.CreatePoolParams{
		ProviderName:   "test-provider",
		MaxRunners:     10,
		MinIdleRunners: 0,
		Image:          "other-image",
		Flavor:         "other-flavor",
		OSType:         "linux",
		OSArch:         "amd64",
		Tags:           prewarmTestLabels,
		Enabled:        true,
	})
	s.Require().NoError(err)

	_, resolved, err := s.mgr.resolvePrewarmPool(params.PrewarmDemand{
		LabelKey: config.NormalizeLabelKey(prewarmTestLabels),
		Labels:   prewarmTestLabels,
	})
	s.Require().Error(err)
	s.False(resolved)
}

func (s *PrewarmPoolTestSuite) TestUnknownWorkflowIsNotPrewarmed() {
	s.Require().NoError(s.mgr.HandleWorkflowJob(
		s.workflowJob(1, 100, prewarmTestGateJob, "Some Other Workflow")))
	s.Require().NoError(s.mgr.HandleWorkflowJob(
		s.workflowJob(2, 200, "not-the-gate", prewarmTestWorkflow)))

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Empty(requests)
}

func TestPrewarmPoolTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PrewarmPoolTestSuite))
}
