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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	PrewarmRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "requests_total",
		Help:      "Number of prewarm requests recorded, by rule, mode and outcome",
	}, []string{"rule", "mode", "outcome"})

	PrewarmTargetRunners = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "target_runners",
		Help:      "Remaining forecast runners for a prewarm target",
	}, []string{"target", "pool"})

	PrewarmInstancesCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "instances_created_total",
		Help:      "Number of speculative runners created",
	}, []string{"target", "pool"})

	PrewarmInstancesClaimed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "instances_claimed_total",
		Help:      "Number of speculative runners claimed by a real queued job",
	}, []string{"target", "pool"})

	PrewarmInstancesReaped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "instances_reaped_total",
		Help:      "Number of speculative runners removed without ever being claimed",
	}, []string{"target", "pool", "reason"})

	PrewarmIdleSeconds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "idle_seconds_total",
		Help:      "Seconds that reaped speculative runners were alive without ever serving a job. This is the cost of a forecast that did not pay off; runners that were claimed are not counted here, because their lifetime was work.",
	}, []string{"target", "pool"})

	PrewarmPreemptionsReported = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "preemptions_reported_total",
		Help:      "Number of preemption notices reported by runners. Counted whether or not preemption replacement is enabled, so the rate is visible before it is acted on.",
	})

	PrewarmPreemptionReplacements = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "preemption_replacements_total",
		Help:      "Number of replacement runners forecast for a preempted runner's retry, by the label set the retry will request",
	}, []string{"target"})

	PrewarmReconcileDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsPrewarmSubsystem,
		Name:      "reconcile_duration_seconds",
		Help:      "Time taken by a prewarm reconcile pass",
		Buckets:   prometheus.DefBuckets,
	})
)
