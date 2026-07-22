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

	commonParams "github.com/cloudbase/garm-provider-common/params"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
)

func TestRunnerCountExcludesInstancesBeingDeleted(t *testing.T) {
	w := &Worker{
		runners: map[string]params.Instance{
			"running":              {Status: commonParams.InstanceRunning},
			"pending-delete":       {Status: commonParams.InstancePendingDelete},
			"pending-force-delete": {Status: commonParams.InstancePendingForceDelete},
			"deleting":             {Status: commonParams.InstanceDeleting},
			"deleted":              {Status: commonParams.InstanceDeleted},
		},
	}

	if got, want := w.runnerCount(), 1; got != want {
		t.Fatalf("runnerCount() = %d, want %d", got, want)
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
