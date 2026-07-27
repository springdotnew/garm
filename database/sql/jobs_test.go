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

package sql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/database/watcher"
	garmTesting "github.com/cloudbase/garm/internal/testing"
	"github.com/cloudbase/garm/params"
)

type JobsTestSuite struct {
	suite.Suite
	Store    dbCommon.Store
	adminCtx context.Context
	entity   params.ForgeEntity
}

func (s *JobsTestSuite) SetupTest() {
	ctx := context.Background()
	watcher.InitWatcher(ctx)

	// Create testing sqlite database
	db := newTestDB(s.T())
	s.Store = db

	adminCtx := garmTesting.ImpersonateAdminContext(ctx, db, s.T())
	s.adminCtx = adminCtx

	endpoint := garmTesting.CreateDefaultGithubEndpoint(adminCtx, db, s.T())
	creds := garmTesting.CreateTestGithubCredentials(adminCtx, "jobs-creds", db, s.T(), endpoint)
	repo, err := db.CreateRepository(
		adminCtx, "test-owner", "test-repo", creds,
		"test-webhook-secret", params.PoolBalancerTypeRoundRobin, false)
	s.Require().NoError(err)

	entity, err := repo.GetEntity()
	s.Require().NoError(err)
	s.entity = entity
}

func (s *JobsTestSuite) TearDownTest() {
	watcher.CloseWatcher()
}

func TestJobsTestSuite(t *testing.T) {
	suite.Run(t, new(JobsTestSuite))
}

// TestDeleteInactionableJobs verifies the deletion logic for jobs
func (s *JobsTestSuite) TestDeleteInactionableJobs() {
	db := s.Store.(*sqlDatabase)

	// Create mix of jobs to test all conditions:
	// 1. Queued jobs (should NOT be deleted)
	queuedJob := params.Job{
		WorkflowJobID:   12345,
		RunID:           67890,
		Action:          "test-action",
		Status:          string(params.JobStatusQueued),
		Name:            "queued-job",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err := s.Store.CreateOrUpdateJob(s.adminCtx, queuedJob)
	s.Require().NoError(err)

	// 2. In-progress job without instance (should be deleted)
	inProgressNoInstance := params.Job{
		WorkflowJobID:   12346,
		RunID:           67890,
		Action:          "test-action",
		Status:          string(params.JobStatusInProgress),
		Name:            "inprogress-no-instance",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err = s.Store.CreateOrUpdateJob(s.adminCtx, inProgressNoInstance)
	s.Require().NoError(err)

	// 3. Completed job without instance (should be deleted)
	completedNoInstance := params.Job{
		WorkflowJobID:   12347,
		RunID:           67890,
		Action:          "test-action",
		Status:          string(params.JobStatusCompleted),
		Conclusion:      "success",
		Name:            "completed-no-instance",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err = s.Store.CreateOrUpdateJob(s.adminCtx, completedNoInstance)
	s.Require().NoError(err)

	// Count total jobs before deletion
	var countBefore int64
	err = db.conn.Model(&WorkflowJob{}).Count(&countBefore).Error
	s.Require().NoError(err)
	s.Require().Equal(int64(3), countBefore, "Should have 3 jobs before deletion")

	// Run deletion
	err = s.Store.DeleteInactionableJobs(s.adminCtx, 0)
	s.Require().NoError(err)

	// Count remaining jobs - should only have the queued job
	var countAfter int64
	err = db.conn.Model(&WorkflowJob{}).Count(&countAfter).Error
	s.Require().NoError(err)
	s.Require().Equal(int64(1), countAfter, "Should have 1 job remaining (queued)")

	// Verify the remaining job is the queued one
	var remaining WorkflowJob
	err = db.conn.Where("workflow_job_id = ?", 12345).First(&remaining).Error
	s.Require().NoError(err)
	s.Require().Equal("queued", remaining.Status)
}

// TestDeleteInactionableJobs_AllScenarios verifies all deletion rules
func (s *JobsTestSuite) TestDeleteInactionableJobs_AllScenarios() {
	db := s.Store.(*sqlDatabase)

	// Rule 1: Queued jobs are NEVER deleted (regardless of instance_id)
	queuedNoInstance := params.Job{
		WorkflowJobID:   20001,
		RunID:           67890,
		Status:          string(params.JobStatusQueued),
		Name:            "queued-no-instance",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err := s.Store.CreateOrUpdateJob(s.adminCtx, queuedNoInstance)
	s.Require().NoError(err)

	// Rule 2: Non-queued jobs WITHOUT instance_id ARE deleted
	inProgressNoInstance := params.Job{
		WorkflowJobID:   20002,
		RunID:           67890,
		Status:          string(params.JobStatusInProgress),
		Name:            "inprogress-no-instance",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err = s.Store.CreateOrUpdateJob(s.adminCtx, inProgressNoInstance)
	s.Require().NoError(err)

	completedNoInstance := params.Job{
		WorkflowJobID:   20003,
		RunID:           67890,
		Status:          string(params.JobStatusCompleted),
		Conclusion:      "success",
		Name:            "completed-no-instance",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err = s.Store.CreateOrUpdateJob(s.adminCtx, completedNoInstance)
	s.Require().NoError(err)

	// Count jobs before deletion
	var countBefore int64
	err = db.conn.Model(&WorkflowJob{}).Count(&countBefore).Error
	s.Require().NoError(err)
	s.Require().Equal(int64(3), countBefore)

	// Run deletion
	err = s.Store.DeleteInactionableJobs(s.adminCtx, 0)
	s.Require().NoError(err)

	// After deletion, only queued job should remain
	var countAfter int64
	err = db.conn.Model(&WorkflowJob{}).Count(&countAfter).Error
	s.Require().NoError(err)
	s.Require().Equal(int64(1), countAfter, "Only queued job should remain")

	// Verify it's the queued job that remains
	var jobs []WorkflowJob
	err = db.conn.Find(&jobs).Error
	s.Require().NoError(err)
	s.Require().Len(jobs, 1)
	s.Require().Equal(string(params.JobStatusQueued), jobs[0].Status)
}

// TestDeleteInactionableJobs_WithDuration verifies the duration-based filtering
func (s *JobsTestSuite) TestDeleteInactionableJobs_WithDuration() {
	db := s.Store.(*sqlDatabase)

	// Create an inactionable job (completed, no instance) with recent created_at
	recentJob := params.Job{
		WorkflowJobID:   30001,
		RunID:           67890,
		Status:          string(params.JobStatusCompleted),
		Name:            "recent-completed",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err := s.Store.CreateOrUpdateJob(s.adminCtx, recentJob)
	s.Require().NoError(err)

	// Create an inactionable job and backdate its created_at to 2 hours ago
	oldJob := params.Job{
		WorkflowJobID:   30002,
		RunID:           67890,
		Status:          string(params.JobStatusCompleted),
		Name:            "old-completed",
		RepositoryName:  "test-repo",
		RepositoryOwner: "test-owner",
	}
	_, err = s.Store.CreateOrUpdateJob(s.adminCtx, oldJob)
	s.Require().NoError(err)

	// Backdate the old job's created_at
	err = db.conn.Model(&WorkflowJob{}).
		Where("workflow_job_id = ?", 30002).
		Update("created_at", time.Now().Add(-2*time.Hour)).Error
	s.Require().NoError(err)

	var countBefore int64
	err = db.conn.Model(&WorkflowJob{}).Count(&countBefore).Error
	s.Require().NoError(err)
	s.Require().Equal(int64(2), countBefore)

	// Delete inactionable jobs older than 1 hour — should only delete the old job
	err = s.Store.DeleteInactionableJobs(s.adminCtx, 1*time.Hour)
	s.Require().NoError(err)

	var countAfter int64
	err = db.conn.Model(&WorkflowJob{}).Count(&countAfter).Error
	s.Require().NoError(err)
	s.Require().Equal(int64(1), countAfter, "Only the recent job should remain")

	// Verify the remaining job is the recent one
	var remaining WorkflowJob
	err = db.conn.Where("workflow_job_id = ?", 30001).First(&remaining).Error
	s.Require().NoError(err)
	s.Require().Equal("recent-completed", remaining.Name)
}

// createJobRunner registers a runner the way a pool manager would, so a job
// can be attached to it by name.
func (s *JobsTestSuite) createJobRunner(name string) params.Instance {
	pool, err := s.Store.CreateEntityPool(s.adminCtx, s.entity, params.CreatePoolParams{
		ProviderName:   "test-provider",
		MaxRunners:     10,
		MinIdleRunners: 0,
		Image:          "test-image",
		Flavor:         "test-flavor",
		OSType:         "linux",
		OSArch:         "amd64",
		Tags:           []string{"self-hosted", name},
		Enabled:        true,
	})
	s.Require().NoError(err)

	instance, err := s.Store.CreateInstance(s.adminCtx, pool.ID, params.CreateInstanceParams{
		Name:   name,
		OSType: "linux",
		OSArch: "amd64",
		Status: commonParams.InstanceRunning,
	})
	s.Require().NoError(err)
	return instance
}

// TestGetJobByInstanceID verifies the lookup a preemption notice depends on:
// the runner reports itself, and GARM has to find the job it is running to
// know which retry to pre-acquire for.
func (s *JobsTestSuite) TestGetJobByInstanceID() {
	instance := s.createJobRunner("garm-abc123")

	_, err := s.Store.CreateOrUpdateJob(s.adminCtx, params.Job{
		WorkflowJobID:   40001,
		RunID:           67890,
		RunAttempt:      1,
		Status:          string(params.JobStatusInProgress),
		Name:            "typecheck",
		WorkflowName:    "PR Tests",
		RunnerName:      instance.Name,
		RepositoryName:  "spring",
		RepositoryOwner: "springdotnew",
	})
	s.Require().NoError(err)

	fetched, err := s.Store.GetJobByInstanceID(s.adminCtx, instance.ID)
	s.Require().NoError(err)
	s.Require().Equal(int64(40001), fetched.WorkflowJobID)
	s.Require().Equal(int64(67890), fetched.RunID)
	s.Require().Equal(int64(1), fetched.RunAttempt)
	s.Require().Equal("PR Tests", fetched.WorkflowName)
	s.Require().Equal("springdotnew", fetched.RepositoryOwner)
	s.Require().Equal("spring", fetched.RepositoryName)
	s.Require().Equal("garm-abc123", fetched.RunnerName)
}

// A runner preempted before it picked anything up has no job, and the caller
// needs to be able to tell that apart from a failure.
func (s *JobsTestSuite) TestGetJobByInstanceIDNotFound() {
	instance := s.createJobRunner("garm-never-claimed")

	_, err := s.Store.GetJobByInstanceID(s.adminCtx, instance.ID)
	s.Require().ErrorIs(err, runnerErrors.ErrNotFound)
}

func (s *JobsTestSuite) TestGetJobByInstanceIDRejectsAMalformedID() {
	_, err := s.Store.GetJobByInstanceID(s.adminCtx, "not-a-uuid")
	s.Require().ErrorIs(err, runnerErrors.ErrBadRequest)
}

// A runner can be recorded against more than one job row over its life; the
// newest is the one it is on now.
func (s *JobsTestSuite) TestGetJobByInstanceIDReturnsMostRecent() {
	db := s.Store.(*sqlDatabase)
	instance := s.createJobRunner("garm-recycled")

	for _, id := range []int64{40002, 40003} {
		_, err := s.Store.CreateOrUpdateJob(s.adminCtx, params.Job{
			WorkflowJobID:   id,
			RunID:           67891,
			Status:          string(params.JobStatusInProgress),
			Name:            "shard",
			RunnerName:      instance.Name,
			RepositoryName:  "spring",
			RepositoryOwner: "springdotnew",
		})
		s.Require().NoError(err)
	}

	// Backdate the first row so the ordering is unambiguous.
	err := db.conn.Model(&WorkflowJob{}).
		Where("workflow_job_id = ?", 40002).
		Update("updated_at", time.Now().Add(-1*time.Hour)).Error
	s.Require().NoError(err)

	fetched, err := s.Store.GetJobByInstanceID(s.adminCtx, instance.ID)
	s.Require().NoError(err)
	s.Require().Equal(int64(40003), fetched.WorkflowJobID)
}
