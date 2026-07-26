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
	"testing"
	"time"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
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
