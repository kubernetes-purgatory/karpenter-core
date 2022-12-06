/*
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

package pod

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/metrics"
	"github.com/aws/karpenter-core/pkg/operator/controller"
)

const (
	podName             = "name"
	podNameSpace        = "namespace"
	ownerSelfLink       = "owner"
	podHostName         = "node"
	podProvisioner      = "provisioner"
	podHostZone         = "zone"
	podHostArchitecture = "arch"
	podHostCapacityType = "capacity_type"
	podHostInstanceType = "instance_type"
	podPhase            = "phase"

	phasePending = "Pending"
)

var (
	podGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "pods",
			Name:      "state",
			Help:      "Pod state is the current state of pods. This metric can be used several ways as it is labeled by the pod name, namespace, owner, node, provisioner name, zone, architecture, capacity type, instance type and pod phase.",
		},
		labelNames(),
	)

	podStartupTimeSummary = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace:  "karpenter",
			Subsystem:  "pods",
			Name:       "startup_time_seconds",
			Help:       "The time from pod creation until the pod is running.",
			Objectives: metrics.SummaryObjectives(),
		},
	)
)

var _ controller.TypedController[*v1.Pod] = (*Controller)(nil)

// Controller for the resource
type Controller struct {
	kubeClient  client.Client
	labelsMap   sync.Map
	pendingPods sets.String
}

func init() {
	crmetrics.Registry.MustRegister(podGaugeVec)
	crmetrics.Registry.MustRegister(podStartupTimeSummary)
}

func labelNames() []string {
	return []string{
		podName,
		podNameSpace,
		ownerSelfLink,
		podHostName,
		podProvisioner,
		podHostZone,
		podHostArchitecture,
		podHostCapacityType,
		podHostInstanceType,
		podPhase,
	}
}

// NewController constructs a podController instance
func NewController(kubeClient client.Client) controller.Controller {
	return controller.Typed[*v1.Pod](kubeClient, &Controller{
		kubeClient:  kubeClient,
		pendingPods: sets.NewString(),
	})
}

func (c *Controller) Name() string {
	return "podmetrics"
}

// Reconcile executes a termination control loop for the resource
func (c *Controller) Reconcile(ctx context.Context, pod *v1.Pod) (reconcile.Result, error) {
	// Remove the previous gauge after pod labels are updated
	if labels, ok := c.labelsMap.Load(client.ObjectKeyFromObject(pod)); ok {
		podGaugeVec.Delete(labels.(prometheus.Labels))
	}
	c.record(ctx, pod)
	return reconcile.Result{}, nil
}

func (c *Controller) record(ctx context.Context, pod *v1.Pod) {
	// Record pods state metric
	labels := c.labels(ctx, pod)
	podGaugeVec.With(labels).Set(float64(1))
	c.labelsMap.Store(client.ObjectKeyFromObject(pod), labels)

	// Record pods startup time metric
	var condition *v1.PodCondition
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == v1.PodReady {
			condition = &pod.Status.Conditions[i]
		}
	}

	podKey := client.ObjectKeyFromObject(pod).String()
	if pod.Status.Phase == phasePending {
		c.pendingPods.Insert(podKey)
	} else if c.pendingPods.Has(podKey) && condition != nil {
		podStartupTimeSummary.Observe(condition.LastTransitionTime.Sub(pod.CreationTimestamp.Time).Seconds())
		c.pendingPods.Delete(podKey)
	}
}

func (c *Controller) Builder(_ context.Context, m manager.Manager) controller.Builder {
	return controller.Adapt(
		controllerruntime.
			NewControllerManagedBy(m).
			For(&v1.Pod{}),
	)
}

// labels creates the labels using the current state of the pod
func (c *Controller) labels(ctx context.Context, pod *v1.Pod) prometheus.Labels {
	metricLabels := prometheus.Labels{}
	metricLabels[podName] = pod.GetName()
	metricLabels[podNameSpace] = pod.GetNamespace()
	// Selflink has been deprecated after v.1.20
	// Manually generate the selflink for the first owner reference
	// Currently we do not support multiple owner references
	selflink := ""
	if len(pod.GetOwnerReferences()) > 0 {
		ownerreference := pod.GetOwnerReferences()[0]
		selflink = fmt.Sprintf("/apis/%s/namespaces/%s/%ss/%s", ownerreference.APIVersion, pod.Namespace, strings.ToLower(ownerreference.Kind), ownerreference.Name)
	}
	metricLabels[ownerSelfLink] = selflink
	metricLabels[podHostName] = pod.Spec.NodeName
	metricLabels[podPhase] = string(pod.Status.Phase)
	node := &v1.Node{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
		metricLabels[podHostZone] = "N/A"
		metricLabels[podHostArchitecture] = "N/A"
		metricLabels[podHostCapacityType] = "N/A"
		metricLabels[podHostInstanceType] = "N/A"
		if provisionerName, ok := pod.Spec.NodeSelector[v1alpha5.ProvisionerNameLabelKey]; ok {
			metricLabels[podProvisioner] = provisionerName
		} else {
			metricLabels[podProvisioner] = "N/A"
		}
	} else {
		metricLabels[podHostZone] = node.Labels[v1.LabelTopologyZone]
		metricLabels[podHostArchitecture] = node.Labels[v1.LabelArchStable]
		if capacityType, ok := node.Labels[v1alpha5.LabelCapacityType]; !ok {
			metricLabels[podHostCapacityType] = "N/A"
		} else {
			metricLabels[podHostCapacityType] = capacityType
		}
		metricLabels[podHostInstanceType] = node.Labels[v1.LabelInstanceTypeStable]
		if provisionerName, ok := node.Labels[v1alpha5.ProvisionerNameLabelKey]; !ok {
			metricLabels[podProvisioner] = "N/A"
		} else {
			metricLabels[podProvisioner] = provisionerName
		}
	}
	return metricLabels
}
