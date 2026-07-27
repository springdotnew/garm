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
	"testing"

	"github.com/stretchr/testify/suite"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/auth"
	"github.com/cloudbase/garm/config"
	"github.com/cloudbase/garm/database"
	dbCommon "github.com/cloudbase/garm/database/common"
	garmTesting "github.com/cloudbase/garm/internal/testing"
	"github.com/cloudbase/garm/params"
)

const (
	preemptedRunnerName = "garm-preempted-runner"
	preemptedJobID      = int64(918273)
	preemptedRunID      = int64(645342)
)

// preemptedLabels is the spot pool's tag set; preemptionReplacementLabels is
// the standard twin the rerun is routed to.
var (
	preemptedLabels              = []string{"self-hosted", "linux", "gcp-4vcpu-spot"}
	preemptionReplacementLabels  = []string{"self-hosted", "linux", "gcp-4vcpu"}
	preemptionUnmappedPoolLabels = []string{"self-hosted", "linux", "gcp-8vcpu-spot"}
)

type PreemptionTestSuite struct {
	suite.Suite

	store    dbCommon.Store
	adminCtx context.Context
	entity   params.ForgeEntity
	pool     params.Pool
	runner   *Runner
}

func (s *PreemptionTestSuite) SetupTest() {
	dbCfg := garmTesting.GetTestSqliteDBConfig(s.T())
	db, err := database.NewDatabase(context.Background(), dbCfg)
	s.Require().NoError(err)

	s.store = db
	s.adminCtx = garmTesting.ImpersonateAdminContext(context.Background(), db, s.T())

	endpoint := garmTesting.CreateDefaultGithubEndpoint(s.adminCtx, db, s.T())
	creds := garmTesting.CreateTestGithubCredentials(s.adminCtx, "preemption-creds", db, s.T(), endpoint)

	repo, err := db.CreateRepository(
		s.adminCtx, "test-owner", "test-repo", creds,
		"test-webhook-secret", params.PoolBalancerTypeRoundRobin, false)
	s.Require().NoError(err)

	entity, err := repo.GetEntity()
	s.Require().NoError(err)
	s.entity = entity

	s.pool = s.createPool(preemptedLabels)

	s.runner = &Runner{
		ctx:    s.adminCtx,
		store:  db,
		config: config.Config{Prewarm: s.prewarmConfig()},
	}
}

func (s *PreemptionTestSuite) createPool(tags []string) params.Pool {
	pool, err := s.store.CreateEntityPool(s.adminCtx, s.entity, params.CreatePoolParams{
		ProviderName:   "test-provider",
		MaxRunners:     50,
		MinIdleRunners: 0,
		Image:          "test-image",
		Flavor:         "test-flavor",
		OSType:         "linux",
		OSArch:         "amd64",
		Tags:           tags,
		Enabled:        true,
	})
	s.Require().NoError(err)
	return pool
}

func (s *PreemptionTestSuite) prewarmConfig() config.Prewarm {
	return config.Prewarm{
		Enable:                true,
		Mode:                  config.PrewarmModeActive,
		MaxSpeculativeRunners: 100,
		Rules: []config.PrewarmRule{
			{
				ID:         "test-rule",
				Repository: "test-owner/test-repo",
				Workflow:   "PR Tests",
				TriggerJob: "changes",
				Targets:    []config.PrewarmTarget{{Labels: preemptedLabels, Count: 4}},
			},
		},
		Preemption: config.PrewarmPreemption{
			Enable: true,
			Replacements: []config.PrewarmReplacement{
				{From: preemptedLabels, To: preemptionReplacementLabels},
			},
		},
	}
}

// createRunner puts an instance in a pool the way the pool manager would, so
// the preemption path has something to resolve from its name.
func (s *PreemptionTestSuite) createRunner(poolID, name string) params.Instance {
	instance, err := s.store.CreateInstance(s.adminCtx, poolID, params.CreateInstanceParams{
		Name:   name,
		OSType: "linux",
		OSArch: "amd64",
		Status: commonParams.InstanceRunning,
	})
	s.Require().NoError(err)
	return instance
}

func (s *PreemptionTestSuite) createJob(runnerName string, attempt int64) params.Job {
	job, err := s.store.CreateOrUpdateJob(s.adminCtx, params.Job{
		WorkflowJobID:   preemptedJobID,
		RunID:           preemptedRunID,
		RunAttempt:      attempt,
		Status:          string(params.JobStatusInProgress),
		Name:            "typecheck",
		WorkflowName:    "PR Tests",
		RunnerName:      runnerName,
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	})
	s.Require().NoError(err)
	return job
}

// instanceCtx is what the callback middleware builds after authenticating a
// runner's own token: the only thing the handler knows is which runner called.
func (s *PreemptionTestSuite) instanceCtx(name string) context.Context {
	return auth.SetInstanceName(s.adminCtx, name)
}

func (s *PreemptionTestSuite) activeRequests() []params.PrewarmRequest {
	requests, err := s.store.ListActivePrewarmRequests(s.adminCtx, s.entity.ID)
	s.Require().NoError(err)
	return requests
}

func TestPreemptionTestSuite(t *testing.T) {
	suite.Run(t, new(PreemptionTestSuite))
}

func (s *PreemptionTestSuite) TestReportPreemptionPreAcquiresTheReplacement() {
	s.createRunner(s.pool.ID, preemptedRunnerName)
	s.createJob(preemptedRunnerName, 1)

	err := s.runner.ReportInstancePreempted(s.instanceCtx(preemptedRunnerName))
	s.Require().NoError(err)

	requests := s.activeRequests()
	s.Require().Len(requests, 1)

	request := requests[0]
	s.Require().Equal(params.PrewarmRequestActive, request.State)
	s.Require().Equal(preemptedRunID, request.RunID)
	// The forecast is recorded against the attempt the retry will run under,
	// which is what lets the ordinary consumption path shrink it when GitHub
	// finally queues that retry.
	s.Require().Equal(int64(2), request.RunAttempt)
	s.Require().Equal("test-owner/test-repo", request.Repository)
	s.Require().Equal("PR Tests", request.WorkflowName)
	s.Require().Equal(preemptedJobID, request.TriggerJobID)
	s.Require().Equal("preempted-job-918273", request.RuleID)

	s.Require().Len(request.Targets, 1)
	s.Require().Equal(config.NormalizeLabelKey(preemptionReplacementLabels), request.Targets[0].LabelKey)
	s.Require().Equal(uint(1), request.Targets[0].TargetCount)
}

// Two notices for the same job — a retried POST, or a watchdog that fires
// twice — must pre-acquire one replacement, not two.
func (s *PreemptionTestSuite) TestReportPreemptionIsIdempotent() {
	s.createRunner(s.pool.ID, preemptedRunnerName)
	s.createJob(preemptedRunnerName, 1)

	ctx := s.instanceCtx(preemptedRunnerName)
	s.Require().NoError(s.runner.ReportInstancePreempted(ctx))
	s.Require().NoError(s.runner.ReportInstancePreempted(ctx))

	requests := s.activeRequests()
	s.Require().Len(requests, 1)
	s.Require().Len(requests[0].Targets, 1)
	s.Require().Equal(uint(1), requests[0].Targets[0].TargetCount)
}

// In shadow mode the notice is still recorded — that is how the forecast is
// scored — but it must not be allowed to create capacity.
func (s *PreemptionTestSuite) TestReportPreemptionInShadowModeRecordsWithoutCreating() {
	s.runner.config.Prewarm.Mode = config.PrewarmModeShadow
	s.createRunner(s.pool.ID, preemptedRunnerName)
	s.createJob(preemptedRunnerName, 1)

	s.Require().NoError(s.runner.ReportInstancePreempted(s.instanceCtx(preemptedRunnerName)))

	requests := s.activeRequests()
	s.Require().Len(requests, 1)
	s.Require().Equal(params.PrewarmRequestShadow, requests[0].State)
}

func (s *PreemptionTestSuite) TestReportPreemptionDisabledIsANoOp() {
	s.runner.config.Prewarm.Preemption.Enable = false
	s.createRunner(s.pool.ID, preemptedRunnerName)
	s.createJob(preemptedRunnerName, 1)

	s.Require().NoError(s.runner.ReportInstancePreempted(s.instanceCtx(preemptedRunnerName)))
	s.Require().Empty(s.activeRequests())
}

// A runner reclaimed before it ever picked up a job has no retry coming, so
// there is nothing to pre-acquire.
func (s *PreemptionTestSuite) TestReportPreemptionWithoutAJobIsANoOp() {
	s.createRunner(s.pool.ID, preemptedRunnerName)

	s.Require().NoError(s.runner.ReportInstancePreempted(s.instanceCtx(preemptedRunnerName)))
	s.Require().Empty(s.activeRequests())
}

// A pool nobody wrote a replacement for is left alone rather than guessed at:
// pre-acquiring the wrong label set buys nothing and costs a machine.
func (s *PreemptionTestSuite) TestReportPreemptionWithoutAReplacementIsANoOp() {
	unmappedPool := s.createPool(preemptionUnmappedPoolLabels)
	s.createRunner(unmappedPool.ID, preemptedRunnerName)
	s.createJob(preemptedRunnerName, 1)

	s.Require().NoError(s.runner.ReportInstancePreempted(s.instanceCtx(preemptedRunnerName)))
	s.Require().Empty(s.activeRequests())
}

func (s *PreemptionTestSuite) TestReportPreemptionRequiresAnAuthenticatedRunner() {
	err := s.runner.ReportInstancePreempted(s.adminCtx)
	s.Require().ErrorIs(err, runnerErrors.ErrUnauthorized)
}

func (s *PreemptionTestSuite) TestReportPreemptionFromAnUnknownRunnerFails() {
	err := s.runner.ReportInstancePreempted(s.instanceCtx("garm-does-not-exist"))
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "error fetching instance")
}

// A rerun that is itself preempted forecasts the attempt after it, so a fleet
// losing machines repeatedly keeps staying ahead of its own retries.
func (s *PreemptionTestSuite) TestReportPreemptionForecastsTheNextAttempt() {
	s.createRunner(s.pool.ID, preemptedRunnerName)
	s.createJob(preemptedRunnerName, 3)

	s.Require().NoError(s.runner.ReportInstancePreempted(s.instanceCtx(preemptedRunnerName)))

	requests := s.activeRequests()
	s.Require().Len(requests, 1)
	s.Require().Equal(int64(4), requests[0].RunAttempt)
}
