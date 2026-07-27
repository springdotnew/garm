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
	"log/slog"

	"github.com/cloudbase/garm/config"
	"github.com/cloudbase/garm/metrics"
)

// prewarmLabelKey is how a forecast target addresses this scale set. A scale
// set is addressed by name in runs-on rather than by a label set, so its name
// is the label set.
func (w *Worker) prewarmLabelKey() string {
	return config.NormalizeLabelKey([]string{w.scaleSet.Name})
}

// refreshPrewarmForecastLocked updates how many runners this scale set should
// be holding for work GitHub has not queued yet.
//
// Prewarming a scale set needs neither a claim nor a reaper, because a scale
// set converges on a runner count rather than reserving a runner per job. The
// forecast simply raises the target while it is live; real demand consumes it
// as jobs are queued; and when the window closes the target falls back and the
// ordinary scale-down path reclaims whatever is still idle. A runner that
// picked up a job is never eligible for that path, so no forecast can remove
// work in progress.
//
// Any failure to read the forecast leaves the scale set at its ordinary
// target: not prewarming costs a cold boot, while prewarming on a stale
// forecast costs money for runners nobody asked for.
//
// The caller must hold w.mux.
func (w *Worker) refreshPrewarmForecastLocked() {
	w.speculativeRunners = 0

	// Prewarming off is prewarming absent: an operator who never configured it
	// should not pay two database round trips on every autoscale pass for a
	// forecast that cannot exist. Only `enable` is checked, not the mode — the
	// query below already filters to active requests, so shadow mode keeps
	// exercising exactly the code path it is there to rehearse.
	if !w.prewarm.Enable {
		return
	}

	// Read through to the store rather than the value cached at construction:
	// the pause flag is a kill switch, and a kill switch that needs a restart
	// is not one.
	controllerInfo, err := w.store.ControllerInfo()
	if err != nil {
		slog.ErrorContext(w.ctx, "error getting controller info; not prewarming", "error", err)
		return
	}
	if controllerInfo.PrewarmPaused {
		return
	}

	forecast, err := w.store.SumRemainingPrewarmForecast(w.ctx, w.entity.ID, w.prewarmLabelKey())
	if err != nil {
		slog.ErrorContext(w.ctx, "error reading prewarm forecast; not prewarming", "error", err)
		return
	}
	if forecast > w.scaleSet.MaxRunners {
		forecast = w.scaleSet.MaxRunners
	}

	w.speculativeRunners = int(forecast)
	metrics.PrewarmTargetRunners.WithLabelValues(w.prewarmLabelKey(), w.consumerID).Set(float64(forecast))
}
