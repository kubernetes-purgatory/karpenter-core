/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

func init() {
	crmetrics.Registry.MustRegister(
		EvaluationDurationHistogram,
		DecisionsPerformedCounter,
		PodsDisruptedCounter,
		EligibleNodesGauge,
		ConsolidationTimeoutTotalCounter,
		NodePoolAllowedDisruptionsGauge,
	)
}

const (
	disruptionSubsystem    = "disruption"
	actionLabel            = "action"
	methodLabel            = "method"
	consolidationTypeLabel = "consolidation_type"
)

var (
	EvaluationDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: disruptionSubsystem,
			Name:      "evaluation_duration_seconds",
			Help:      "Duration of the disruption evaluation process in seconds. Labeled by method and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{methodLabel, consolidationTypeLabel},
	)
	DecisionsPerformedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: disruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption action, method, and consolidation type.",
		},
		[]string{actionLabel, methodLabel, consolidationTypeLabel},
	)
	PodsDisruptedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: disruptionSubsystem,
			Name:      "pods_disrupted_total",
			Help:      "Total number of reschedulable pods disrupted on nodes. Labeled by NodePool, disruption action, method, and consolidation type.",
		},
		[]string{metrics.NodePoolLabel, actionLabel, methodLabel, consolidationTypeLabel},
	)
	EligibleNodesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: disruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption method and consolidation type.",
		},
		[]string{methodLabel, consolidationTypeLabel},
	)
	ConsolidationTimeoutTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: disruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]string{consolidationTypeLabel},
	)
	NodePoolAllowedDisruptionsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel},
	)
)
