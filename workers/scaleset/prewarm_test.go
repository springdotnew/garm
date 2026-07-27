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
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	dbMocks "github.com/cloudbase/garm/database/common/mocks"
	"github.com/cloudbase/garm/params"
)

const (
	prewarmTestEntityID  = "6bd2a0f4-2a0e-4bd0-8a06-2f0a68c9ff3d"
	prewarmTestScaleSet  = "bench-scaleset"
	prewarmTestMaxRunner = 50
)

// newPrewarmWorker builds a worker whose only interesting state is its scale
// set shape and the store it reads the forecast from.
func newPrewarmWorker(t *testing.T, scaleSet params.ScaleSet) (*Worker, *dbMocks.Store) {
	t.Helper()

	store := dbMocks.NewStore(t)
	return &Worker{
		ctx:        context.Background(),
		consumerID: "scaleset-worker-test",
		store:      store,
		scaleSet:   scaleSet,
		entity:     params.ForgeEntity{ID: prewarmTestEntityID},
	}, store
}

func benchScaleSet() params.ScaleSet {
	return params.ScaleSet{
		Name:       prewarmTestScaleSet,
		Enabled:    true,
		MaxRunners: prewarmTestMaxRunner,
	}
}

func expectForecast(store *dbMocks.Store, forecast uint) {
	store.EXPECT().ControllerInfo().Return(params.ControllerInfo{}, nil).Once()
	store.EXPECT().
		SumRemainingPrewarmForecast(mock.Anything, prewarmTestEntityID, prewarmTestScaleSet).
		Return(forecast, nil).Once()
}

func TestForecastRaisesTheTarget(t *testing.T) {
	w, store := newPrewarmWorker(t, benchScaleSet())
	expectForecast(store, 6)

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 6; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
}

// Assigned jobs and predicted ones are different work: a forecast must not
// displace demand GitHub has already reported.
func TestForecastAddsToAssignedDemand(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.MinIdleRunners = 2
	scaleSet.DesiredRunnerCount = 3
	w, store := newPrewarmWorker(t, scaleSet)
	expectForecast(store, 4)

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 9; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
}

func TestForecastNeverPushesPastMaxRunners(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.MaxRunners = 10
	scaleSet.DesiredRunnerCount = 4
	w, store := newPrewarmWorker(t, scaleSet)
	expectForecast(store, 100)

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 10; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
}

// The kill switch has to work without a restart, so the pause flag is read
// through to the store rather than taken from the value cached at construction.
func TestPausedControllerHoldsNoSpeculativeCapacity(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.DesiredRunnerCount = 3
	w, store := newPrewarmWorker(t, scaleSet)
	w.speculativeRunners = 6

	store.EXPECT().ControllerInfo().Return(params.ControllerInfo{PrewarmPaused: true}, nil).Once()

	w.refreshPrewarmForecastLocked()

	if got, want := w.speculativeRunners, 0; got != want {
		t.Fatalf("speculative runners = %d, want %d", got, want)
	}
	if got, want := w.targetRunners(), 3; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
	store.AssertNotCalled(t, "SumRemainingPrewarmForecast",
		mock.Anything, mock.Anything, mock.Anything)
}

// Not prewarming costs a cold boot; prewarming on an unreadable forecast costs
// money for runners nobody asked for. Failures fall back to the plain target.
func TestForecastReadFailureLeavesTheOrdinaryTarget(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.DesiredRunnerCount = 3
	w, store := newPrewarmWorker(t, scaleSet)
	w.speculativeRunners = 6

	store.EXPECT().ControllerInfo().Return(params.ControllerInfo{}, nil).Once()
	store.EXPECT().
		SumRemainingPrewarmForecast(mock.Anything, mock.Anything, mock.Anything).
		Return(0, errors.New("boom")).Once()

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 3; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
}

func TestControllerInfoFailureLeavesTheOrdinaryTarget(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.DesiredRunnerCount = 3
	w, store := newPrewarmWorker(t, scaleSet)
	w.speculativeRunners = 6

	store.EXPECT().ControllerInfo().Return(params.ControllerInfo{}, errors.New("boom")).Once()

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 3; got != want {
		t.Fatalf("target runners = %d, want %d", got, want)
	}
}

// A scale set needs no reaper: when the window closes the forecast goes to
// zero, the target falls back, and the ordinary scale-down path takes it from
// there.
func TestExpiredForecastReleasesTheTarget(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.DesiredRunnerCount = 1
	w, store := newPrewarmWorker(t, scaleSet)

	expectForecast(store, 5)
	w.refreshPrewarmForecastLocked()
	if got, want := w.targetRunners(), 6; got != want {
		t.Fatalf("target runners while forecast is live = %d, want %d", got, want)
	}

	expectForecast(store, 0)
	w.refreshPrewarmForecastLocked()
	if got, want := w.targetRunners(), 1; got != want {
		t.Fatalf("target runners after the window closed = %d, want %d", got, want)
	}
}

// A scale set is addressed by name in runs-on, so the name is its label set.
func TestPrewarmLabelKeyIsTheNormalisedScaleSetName(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.Name = "  Bench-ScaleSet  "
	w, _ := newPrewarmWorker(t, scaleSet)

	if got, want := w.prewarmLabelKey(), prewarmTestScaleSet; got != want {
		t.Fatalf("label key = %q, want %q", got, want)
	}
}
