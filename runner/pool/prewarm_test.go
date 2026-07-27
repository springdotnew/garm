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

type PrewarmPoolTestSuite struct {
	suite.Suite

	store        dbCommon.Store
	adminCtx     context.Context
	entity       params.ForgeEntity
	pool         params.Pool
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

	backoff, err := locking.NewInstanceDeleteBackoff(context.Background())
	s.Require().NoError(err)

	s.mgr = &basePoolManager{
		ctx:                     s.adminCtx,
		consumerID:              "prewarm-consumer",
		entity:                  entity,
		store:                   db,
		controllerInfo:          controllerInfo,
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
	}

	s.providerMock.On("DisableJITConfig").Return(true).Maybe()
	s.ghcliMock.On("GetEntityJITConfig",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(map[string]string{}, nil, nil).Maybe()
	s.ghcliMock.On("RemoveEntityRunner", mock.Anything, mock.Anything).Return(nil).Maybe()
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

	s.mgr.prewarmCfg.DefaultTTL = config.PrewarmTTL("1ms")
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(jobID, runID)))
	s.Require().NoError(s.mgr.reconcilePrewarm())

	speculative := s.speculativeInstances()
	s.Require().Len(speculative, prewarmTestCohortLen)

	for _, instance := range speculative {
		s.Require().NotNil(instance.SpeculativeExpiresAt)
	}
	time.Sleep(5 * time.Millisecond)

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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

	s.syncJobsFromDB()
	s.Require().NoError(s.mgr.consumeQueuedJobs())

	instances := s.allInstances()
	s.Require().Len(instances, 1, "gate job should be served by exactly one runner")
	s.False(instances[0].Speculative, "gate job must not consume its own forecast")

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Len(s.speculativeInstances(), prewarmTestCohortLen)
	s.Len(s.allInstances(), prewarmTestCohortLen+1)
}

func (s *PrewarmPoolTestSuite) TestDuplicateWebhookDeliveriesCreateOneCohort() {
	gate := s.gateJob(1, 100)
	for range 3 {
		s.Require().NoError(s.mgr.HandleWorkflowJob(gate))
	}

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

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Len(s.speculativeInstances(), prewarmTestCohortLen-2)
	s.Len(s.allInstances(), prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestReconcileIsIdempotent() {
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

	for range 3 {
		s.Require().NoError(s.mgr.reconcilePrewarm())
	}

	s.Len(s.speculativeInstances(), prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestShadowModeCreatesNoRunners() {
	s.mgr.prewarmCfg = s.prewarmConfig(config.PrewarmModeShadow)

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	s.Require().Len(requests, 1, "shadow mode still records the forecast it would have acted on")
	s.Equal(params.PrewarmRequestShadow, requests[0].State)

	s.Require().NoError(s.mgr.reconcilePrewarm())
	s.Empty(s.allInstances(), "shadow mode must not create a single VM")
}

// Shadow mode exists to be read: an operator compares its forecast against the
// fanout that actually queued before switching a rule on. A shadow request that
// publishes nothing is a silent dry run, which is no use to anyone.
func (s *PrewarmPoolTestSuite) TestShadowModePublishesTheForecastItWouldHaveActedOn() {
	s.mgr.prewarmCfg = s.prewarmConfig(config.PrewarmModeShadow)

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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

func (s *PrewarmPoolTestSuite) TestDisabledPrewarmIsANoOp() {
	s.mgr.prewarmCfg = config.Prewarm{}

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

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

func (s *PrewarmPoolTestSuite) TestGlobalCapBoundsTheCohort() {
	s.mgr.prewarmCfg.MaxSpeculativeRunners = 2

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Len(s.speculativeInstances(), 2)

	// A second run must not push past the cap either.
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(2, 200)))
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

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

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

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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

func (s *PrewarmPoolTestSuite) TestOverlappingRunsShareCapacity() {
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(2, 200)))
	s.Require().NoError(s.mgr.reconcilePrewarm())

	s.Len(s.speculativeInstances(), 2*prewarmTestCohortLen)
}

func (s *PrewarmPoolTestSuite) TestQueuedFanoutJobClaimsPrewarmedCapacity() {
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))
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
	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

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

// The reaper has to keep working after the kill switch is thrown, otherwise
// pausing prewarm would strand the runners already in flight.
func (s *PrewarmPoolTestSuite) TestReaperDrainsAfterThePrewarmIsPaused() {
	s.prewarmWithExpiredCohort(1, 100)

	s.mgr.prewarmCfg.Enable = false
	s.Require().NoError(s.mgr.reapSpeculativeSurplus())

	for _, instance := range s.speculativeInstances() {
		s.Equal(commonParams.InstancePendingDelete, instance.Status)
	}
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

	s.Require().NoError(s.mgr.HandleWorkflowJob(s.gateJob(1, 100)))

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
