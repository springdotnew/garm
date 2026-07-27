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
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/cloudbase/garm/config"
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
		prewarm:    config.Prewarm{Enable: true, Mode: config.PrewarmModeActive},
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

// An operator who never turned prewarming on must not pay for it. The mocked
// store is strict, so any query at all fails this test rather than merely
// costing a round trip in production.
func TestDisabledPrewarmQueriesNothing(t *testing.T) {
	w, _ := newPrewarmWorker(t, benchScaleSet())
	w.prewarm = config.Prewarm{}

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 0; got != want {
		t.Fatalf("target runners with prewarm disabled = %d, want %d", got, want)
	}
}

// Shadow mode's whole output is the number an operator is supposed to read
// before switching a rule on. It must be the forecast, not zero.
func TestShadowForecastIsPublishedWithoutRaisingTheTarget(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.DesiredRunnerCount = 1
	w, store := newPrewarmWorker(t, scaleSet)
	w.prewarm.Mode = config.PrewarmModeShadow

	store.EXPECT().ControllerInfo().Return(params.ControllerInfo{}, nil).Once()
	store.EXPECT().
		ListActivePrewarmRequests(mock.Anything, prewarmTestEntityID).
		Return([]params.PrewarmRequest{
			shadowRequest(prewarmTestScaleSet, 7, 2),
			// A live request for the same scale set proves the two are not
			// summed together: acting on a shadow forecast is the one thing
			// shadow mode must never do.
			activeRequest(prewarmTestScaleSet, 4),
			// Another scale set's forecast is none of this worker's business.
			shadowRequest("some-other-scaleset", 30, 0),
		}, nil).Once()

	w.refreshPrewarmForecastLocked()

	if got, want := w.speculativeRunners, 0; got != want {
		t.Fatalf("speculative runners in shadow = %d, want %d", got, want)
	}
	if got, want := w.targetRunners(), 1; got != want {
		t.Fatalf("target runners in shadow = %d, want %d", got, want)
	}
}

// An expired shadow request describes a window that has closed. Reporting it
// would tell an operator to size for a fanout that is already over.
func TestExpiredShadowForecastIsNotPublished(t *testing.T) {
	w, store := newPrewarmWorker(t, benchScaleSet())
	w.prewarm.Mode = config.PrewarmModeShadow

	expired := shadowRequest(prewarmTestScaleSet, 9, 0)
	expired.ExpiresAt = time.Now().Add(-time.Minute)

	store.EXPECT().ControllerInfo().Return(params.ControllerInfo{}, nil).Once()
	store.EXPECT().
		ListActivePrewarmRequests(mock.Anything, prewarmTestEntityID).
		Return([]params.PrewarmRequest{expired}, nil).Once()

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 0; got != want {
		t.Fatalf("target runners for an expired shadow forecast = %d, want %d", got, want)
	}
}

// The forecast is read, not acted on, so a failure to read it costs a number on
// a dashboard — never the scale set's ordinary target.
func TestShadowForecastReadFailureLeavesTheOrdinaryTarget(t *testing.T) {
	scaleSet := benchScaleSet()
	scaleSet.DesiredRunnerCount = 2
	w, store := newPrewarmWorker(t, scaleSet)
	w.prewarm.Mode = config.PrewarmModeShadow

	store.EXPECT().ControllerInfo().Return(params.ControllerInfo{}, nil).Once()
	store.EXPECT().
		ListActivePrewarmRequests(mock.Anything, prewarmTestEntityID).
		Return(nil, errors.New("database is unreachable")).Once()

	w.refreshPrewarmForecastLocked()

	if got, want := w.targetRunners(), 2; got != want {
		t.Fatalf("target runners after a failed shadow read = %d, want %d", got, want)
	}
}

func shadowRequest(labelKey string, target, observed uint) params.PrewarmRequest {
	request := activeRequest(labelKey, target)
	request.State = params.PrewarmRequestShadow
	request.Targets[0].ObservedDemand = observed
	return request
}

func activeRequest(labelKey string, target uint) params.PrewarmRequest {
	return params.PrewarmRequest{
		State:     params.PrewarmRequestActive,
		ExpiresAt: time.Now().Add(time.Hour),
		Targets: []params.PrewarmRequestTarget{
			{LabelKey: labelKey, TargetCount: target},
		},
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
