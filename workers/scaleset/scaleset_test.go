// Copyright 2025 Cloudbase Solutions SRL
//
//	Licensed under the Apache License, Version 2.0 (the "License"); you may
//	not use this file except in compliance with the License. You may obtain
//	a copy of the License at
//
//	     http://www.apache.org/licenses/LICENSE-2.0
//
//	Unless required by applicable law or agreed to in writing, software
//	distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//	WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//	License for the specific language governing permissions and limitations
//	under the License.
package scaleset

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/cache"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
	commonMocks "github.com/cloudbase/garm/runner/common/mocks"
	githubScaleSets "github.com/cloudbase/garm/util/github/scalesets"
)

type testConsumer struct {
	events chan dbCommon.ChangePayload
}

func (c *testConsumer) Watch() <-chan dbCommon.ChangePayload {
	return c.events
}

func (c *testConsumer) IsClosed() bool {
	return false
}

func (c *testConsumer) Close() {}

func (c *testConsumer) SetFilters(_ ...dbCommon.PayloadFilterFunc) {}

func TestGetScaleSetClientReusesCurrentCacheGeneration(t *testing.T) {
	const entityID = "entity-id"
	githubClient := commonMocks.NewGithubClient(t)
	cache.SetGithubClient(entityID, githubClient)
	t.Cleanup(func() { cache.DeleteGithubClient(entityID) })

	w := &Worker{scaleSet: params.ScaleSet{OrgID: entityID}}
	const callers = 16
	clients := make(chan *githubScaleSets.ScaleSetClient, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			client, err := w.GetScaleSetClient()
			if err != nil {
				t.Errorf("GetScaleSetClient() error = %v", err)
				return
			}
			clients <- client
		}()
	}
	callersDone.Wait()
	close(clients)

	var first *githubScaleSets.ScaleSetClient
	for client := range clients {
		if first == nil {
			first = client
			continue
		}
		if client != first {
			t.Fatal("concurrent callers received different scale-set clients")
		}
	}
}

func TestGetScaleSetClientRebuildsAfterGithubClientReplacement(t *testing.T) {
	const entityID = "entity-id"
	cache.SetGithubClient(entityID, commonMocks.NewGithubClient(t))
	t.Cleanup(func() { cache.DeleteGithubClient(entityID) })

	w := &Worker{scaleSet: params.ScaleSet{OrgID: entityID}}
	first, err := w.GetScaleSetClient()
	if err != nil {
		t.Fatalf("GetScaleSetClient() error = %v", err)
	}

	cache.SetGithubClient(entityID, commonMocks.NewGithubClient(t))
	second, err := w.GetScaleSetClient()
	if err != nil {
		t.Fatalf("GetScaleSetClient() after replacement error = %v", err)
	}
	if second == first {
		t.Fatal("GitHub client replacement reused stale scale-set client")
	}
}

func TestRunnerCountIncludesActiveButNotTerminatedCapacity(t *testing.T) {
	w := &Worker{
		runners: map[string]params.Instance{
			"pending":              {Status: commonParams.InstanceCreating, RunnerStatus: params.RunnerPending},
			"idle":                 {Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle},
			"active":               {Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerActive},
			"terminated":           {Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerTerminated},
			"pending-delete":       {Status: commonParams.InstancePendingDelete},
			"pending-force-delete": {Status: commonParams.InstancePendingForceDelete},
			"deleting":             {Status: commonParams.InstanceDeleting},
			"deleted":              {Status: commonParams.InstanceDeleted},
		},
	}

	if got, want := w.runnerCount(), 3; got != want {
		t.Fatalf("runnerCount() = %d, want %d", got, want)
	}
}

func TestRunnerCanScaleDownOnlyAfterIdleGrace(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		runner params.Instance
		want   bool
	}{
		{
			name:   "idle past grace",
			runner: params.Instance{Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, UpdatedAt: now.Add(-scaleDownIdleGrace)},
			want:   true,
		},
		{
			name:   "idle within grace",
			runner: params.Instance{Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, UpdatedAt: now.Add(-scaleDownIdleGrace + time.Second)},
		},
		{
			name:   "installing",
			runner: params.Instance{Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerInstalling, UpdatedAt: now.Add(-time.Minute)},
		},
		{
			name:   "pending",
			runner: params.Instance{Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerPending, UpdatedAt: now.Add(-time.Minute)},
		},
		{
			name:   "active",
			runner: params.Instance{Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerActive, UpdatedAt: now.Add(-time.Minute)},
		},
		{
			name:   "deleting",
			runner: params.Instance{Status: commonParams.InstanceDeleting, RunnerStatus: params.RunnerIdle, UpdatedAt: now.Add(-time.Minute)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runnerCanScaleDown(tt.runner, now); got != tt.want {
				t.Fatalf("runnerCanScaleDown() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestRunnerCreationsToStart(t *testing.T) {
	tests := []struct {
		name          string
		target        int
		current       int
		inFlight      int
		wantCreations int
	}{
		{name: "full burst", target: 32, wantCreations: 32},
		{name: "demand grows during first wave", target: 32, inFlight: 24, wantCreations: 8},
		{name: "existing and in flight satisfy target", target: 32, current: 20, inFlight: 12},
		{name: "concurrency cap", target: 40, wantCreations: 32},
		{name: "one slot remains", target: 40, current: 8, inFlight: 31, wantCreations: 1},
		{name: "target decreased", target: 4, current: 4, inFlight: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runnerCreationsToStart(tt.target, tt.current, tt.inFlight); got != tt.wantCreations {
				t.Fatalf("runnerCreationsToStart() = %d, want %d", got, tt.wantCreations)
			}
		})
	}
}

func TestStartRunnerCreationsDoesNotWaitForFirstWave(t *testing.T) {
	const runnerCount = 32
	started := make(chan struct{}, runnerCount)
	release := make(chan struct{})

	startRunnerCreations(runnerCount, func() {
		started <- struct{}{}
		<-release
	})

	for range runnerCount {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("runner creations did not fan out without waiting for the first wave")
		}
	}
	close(release)
}

func TestRunnerCreationStillNeededFailsClosed(t *testing.T) {
	newWorker := func() *Worker {
		return &Worker{
			scaleSet: params.ScaleSet{
				ID:                 1,
				Enabled:            true,
				MaxRunners:         8,
				DesiredRunnerCount: 8,
			},
			runners:           make(map[string]params.Instance),
			creationsInFlight: 8,
			quit:              make(chan struct{}),
		}
	}

	w := newWorker()
	if !w.runnerCreationStillNeeded(1, w.quit) {
		t.Fatal("exactly reserved creation was rejected")
	}

	w = newWorker()
	w.scaleSet.DesiredRunnerCount = 7
	if w.runnerCreationStillNeeded(1, w.quit) {
		t.Fatal("creation survived a target decrease")
	}

	w = newWorker()
	w.scaleSet.Enabled = false
	if w.runnerCreationStillNeeded(1, w.quit) {
		t.Fatal("creation survived scale-set disable")
	}

	w = newWorker()
	if w.runnerCreationStillNeeded(2, w.quit) {
		t.Fatal("creation survived scale-set replacement")
	}

	w = newWorker()
	workerQuit := w.quit
	close(workerQuit)
	w.quit = make(chan struct{})
	if w.runnerCreationStillNeeded(1, workerQuit) {
		t.Fatal("creation survived its worker generation stopping")
	}
}

func TestCreationCompletionDoesNotOverwriteNewerRunnerSnapshot(t *testing.T) {
	createdAt := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	pendingCreate := params.Instance{
		ID:        "runner-id",
		Name:      "runner",
		Status:    commonParams.InstancePendingCreate,
		UpdatedAt: createdAt,
	}
	running := pendingCreate
	running.Status = commonParams.InstanceRunning
	running.RunnerStatus = params.RunnerIdle
	running.UpdatedAt = createdAt.Add(time.Second)

	w := &Worker{
		ctx:               context.Background(),
		runners:           make(map[string]params.Instance),
		runnerSnapshots:   make(map[string]time.Time),
		creationsInFlight: 1,
		autoScaleWake:     make(chan struct{}, 1),
	}

	w.handleInstanceEntityEvent(dbCommon.ChangePayload{
		EntityType: dbCommon.InstanceEntityType,
		Operation:  dbCommon.UpdateOperation,
		Payload:    running,
	})
	w.finishRunnerCreation(&pendingCreate)
	w.handleInstanceEntityEvent(dbCommon.ChangePayload{
		EntityType: dbCommon.InstanceEntityType,
		Operation:  dbCommon.CreateOperation,
		Payload:    pendingCreate,
	})

	got := w.runners[pendingCreate.ID]
	if got.Status != commonParams.InstanceRunning {
		t.Fatalf("runner status regressed to %s, want %s", got.Status, commonParams.InstanceRunning)
	}
	if !got.UpdatedAt.Equal(running.UpdatedAt) {
		t.Fatalf("runner updated at = %s, want %s", got.UpdatedAt, running.UpdatedAt)
	}
}

func TestLateFailedCreationImmediatelyOpensReplacement(t *testing.T) {
	const target = 32
	runners := make(map[string]params.Instance, target-1)
	for i := range target - 1 {
		id := fmt.Sprintf("runner-%d", i)
		runners[id] = params.Instance{
			ID:           id,
			Status:       commonParams.InstanceRunning,
			RunnerStatus: params.RunnerIdle,
		}
	}
	w := &Worker{
		scaleSet: params.ScaleSet{
			Enabled:            true,
			MaxRunners:         target,
			DesiredRunnerCount: target,
		},
		runners:           runners,
		creationsInFlight: 1,
		autoScaleWake:     make(chan struct{}, 1),
	}

	w.finishRunnerCreation(nil)

	select {
	case <-w.autoScaleWake:
	case <-time.After(time.Second):
		t.Fatal("failed creation did not immediately wake autoscaler")
	}
	if got, want := runnerCreationsToStart(w.targetRunners(), w.runnerCount(), w.creationsInFlight), 1; got != want {
		t.Fatalf("replacement creations = %d, want %d", got, want)
	}
}

// A restart marks every interrupted create `pending_delete` before the machine
// behind it is gone, and runnerCount() rightly refuses to count a runner that
// cannot serve a job. Sizing the forecast off that alone orders the whole cohort
// again on top of the one still running: measured on the bench rig at
// current_runners=0 with ten machines up, nine seconds after the restart.
func TestRestartDoesNotReorderRunnersStillBeingTornDown(t *testing.T) {
	const cohort = 10
	runners := make(map[string]params.Instance, cohort)
	for i := range cohort {
		id := fmt.Sprintf("interrupted-%d", i)
		// Exactly what Start() leaves behind for a create the restart killed:
		// forced from creating to pending_delete, never registered with GitHub,
		// and — because this package never sets it — not marked speculative.
		runners[id] = params.Instance{
			ID:           id,
			Status:       commonParams.InstancePendingDelete,
			RunnerStatus: params.RunnerPending,
		}
	}
	w := &Worker{
		scaleSet:           params.ScaleSet{Enabled: true, MaxRunners: 64},
		runners:            runners,
		speculativeRunners: cohort,
	}

	if got, want := w.runnerCount(), 0; got != want {
		t.Fatalf("runner count = %d, want %d — the premise of this test", got, want)
	}
	if got, want := w.targetRunners(), 0; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
	if got, want := runnerCreationsToStart(w.targetRunners(), w.runnerCount(), w.creationsInFlight), 0; got != want {
		t.Fatalf("creations = %d, want %d", got, want)
	}
}

// Real demand is not held back. A runner assigned to a queued job that dies
// mid-teardown still needs replacing right away, so only the speculative part of
// the target is suppressed.
func TestTornDownRunnersDoNotHoldBackAssignedWork(t *testing.T) {
	runners := map[string]params.Instance{
		"interrupted-0": {
			ID:           "interrupted-0",
			Status:       commonParams.InstancePendingDelete,
			RunnerStatus: params.RunnerPending,
		},
	}
	w := &Worker{
		scaleSet:           params.ScaleSet{Enabled: true, MaxRunners: 64, DesiredRunnerCount: 4},
		runners:            runners,
		speculativeRunners: 1,
	}

	if got, want := w.targetRunners(), 4; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
}

// Ordinary churn is not an interrupted create. A runner that registered, went
// idle and is now being reclaimed has already done its work, and the machine
// behind it is on its way out for good — the forecast must keep its slot or
// every scale-down would eat one warm runner from the next fanout.
func TestReclaimedIdleRunnersDoNotHoldBackTheForecast(t *testing.T) {
	runners := map[string]params.Instance{
		"idle-0": {
			ID:           "idle-0",
			Status:       commonParams.InstancePendingDelete,
			RunnerStatus: params.RunnerIdle,
		},
		"terminated-0": {
			ID:           "terminated-0",
			Status:       commonParams.InstanceDeleting,
			RunnerStatus: params.RunnerTerminated,
		},
	}
	w := &Worker{
		scaleSet:           params.ScaleSet{Enabled: true, MaxRunners: 64},
		runners:            runners,
		speculativeRunners: 3,
	}

	if got, want := w.targetRunners(), 3; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
}

func TestScaleSetUpdateWakesAutoscaler(t *testing.T) {
	w := &Worker{
		ctx:           context.Background(),
		scaleSet:      params.ScaleSet{ID: 1, Enabled: true, MaxRunners: 8},
		autoScaleWake: make(chan struct{}, 1),
	}
	updated := w.scaleSet
	updated.DesiredRunnerCount = 8

	w.handleScaleSetEvent(dbCommon.ChangePayload{
		EntityType: dbCommon.ScaleSetEntityType,
		Operation:  dbCommon.UpdateOperation,
		Payload:    updated,
	})

	select {
	case <-w.autoScaleWake:
	default:
		t.Fatal("scale set update did not wake autoscaler")
	}
}

func TestScaleSetUpdateDoesNotWakeAutoscalerWhenTargetDecreases(t *testing.T) {
	w := &Worker{
		ctx: context.Background(),
		scaleSet: params.ScaleSet{
			ID:                 1,
			Enabled:            true,
			MaxRunners:         8,
			DesiredRunnerCount: 8,
		},
		autoScaleWake: make(chan struct{}, 1),
	}
	updated := w.scaleSet
	updated.DesiredRunnerCount = 7

	w.handleScaleSetEvent(dbCommon.ChangePayload{
		EntityType: dbCommon.ScaleSetEntityType,
		Operation:  dbCommon.UpdateOperation,
		Payload:    updated,
	})

	select {
	case <-w.autoScaleWake:
		t.Fatal("target decrease unexpectedly woke autoscaler")
	default:
	}
}

func TestScaleSetWorkerKeepsLatestDesiredCount(t *testing.T) {
	consumer := &testConsumer{
		events: make(chan dbCommon.ChangePayload, 2),
	}
	initialUpdatedAt := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	w := &Worker{
		ctx: context.Background(),
		scaleSet: params.ScaleSet{
			ID:                 1,
			Enabled:            true,
			MaxRunners:         32,
			DesiredRunnerCount: 4,
			UpdatedAt:          initialUpdatedAt,
		},
		consumer:      consumer,
		autoScaleWake: make(chan struct{}, 1),
		quit:          make(chan struct{}),
	}

	increase := w.scaleSet
	increase.DesiredRunnerCount = 32
	increase.UpdatedAt = initialUpdatedAt.Add(time.Second)
	decrease := increase
	decrease.DesiredRunnerCount = 0
	decrease.UpdatedAt = increase.UpdatedAt.Add(time.Second)

	workerStopped := make(chan struct{})
	go func() {
		w.loop()
		close(workerStopped)
	}()

	consumer.events <- dbCommon.ChangePayload{
		EntityType: dbCommon.ScaleSetEntityType,
		Operation:  dbCommon.UpdateOperation,
		Payload:    increase,
	}
	consumer.events <- dbCommon.ChangePayload{
		EntityType: dbCommon.ScaleSetEntityType,
		Operation:  dbCommon.UpdateOperation,
		Payload:    decrease,
	}
	close(consumer.events)

	select {
	case <-workerStopped:
	case <-time.After(time.Second):
		t.Fatal("scale set worker did not process updates")
	}

	w.handleScaleSetEvent(dbCommon.ChangePayload{
		EntityType: dbCommon.ScaleSetEntityType,
		Operation:  dbCommon.UpdateOperation,
		Payload:    increase,
	})

	w.mux.Lock()
	defer w.mux.Unlock()
	if got, want := w.scaleSet.DesiredRunnerCount, decrease.DesiredRunnerCount; got != want {
		t.Fatalf("desired runner count = %d, want latest value %d", got, want)
	}
	if got, want := w.scaleSet.UpdatedAt, decrease.UpdatedAt; !got.Equal(want) {
		t.Fatalf("scale set updated at = %s, want %s", got, want)
	}
}
