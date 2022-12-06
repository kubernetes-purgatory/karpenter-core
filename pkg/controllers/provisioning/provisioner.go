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

package provisioning

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/util/workqueue"
	"knative.dev/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/operator/controller"
	"github.com/aws/karpenter-core/pkg/operator/injection"

	"github.com/aws/karpenter-core/pkg/cloudprovider"
	scheduler "github.com/aws/karpenter-core/pkg/controllers/provisioning/scheduling"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/events"
	"github.com/aws/karpenter-core/pkg/metrics"
	"github.com/aws/karpenter-core/pkg/scheduling"
	"github.com/aws/karpenter-core/pkg/utils/node"
	"github.com/aws/karpenter-core/pkg/utils/pod"
	"github.com/aws/karpenter-core/pkg/utils/resources"
)

// Provisioner waits for enqueued pods, batches them, creates capacity and binds the pods to the capacity.
type Provisioner struct {
	cloudProvider  cloudprovider.CloudProvider
	kubeClient     client.Client
	coreV1Client   corev1.CoreV1Interface
	batcher        *Batcher
	volumeTopology *VolumeTopology
	cluster        *state.Cluster
	recorder       events.Recorder
}

func NewProvisioner(ctx context.Context, kubeClient client.Client, coreV1Client corev1.CoreV1Interface,
	recorder events.Recorder, cloudProvider cloudprovider.CloudProvider, cluster *state.Cluster) *Provisioner {
	p := &Provisioner{
		batcher:        NewBatcher(),
		cloudProvider:  cloudProvider,
		kubeClient:     kubeClient,
		coreV1Client:   coreV1Client,
		volumeTopology: NewVolumeTopology(kubeClient),
		cluster:        cluster,
		recorder:       recorder,
	}
	return p
}

func (p *Provisioner) Name() string {
	return "provisioner"
}

func (p *Provisioner) Trigger() {
	p.batcher.Trigger()
}

func (p *Provisioner) Builder(_ context.Context, mgr manager.Manager) controller.Builder {
	return controller.NewSingletonManagedBy(mgr)
}

func (p *Provisioner) Reconcile(ctx context.Context, _ reconcile.Request) (result reconcile.Result, err error) {
	// Batch pods
	if triggered := p.batcher.Wait(ctx); !triggered {
		return reconcile.Result{}, nil
	}
	// If the provisioning loop fails for any reason, retrigger it,
	// since pod watch events have already been processed
	defer func() {
		if err != nil {
			p.Trigger()
		}
	}()

	// We collect the nodes with their used capacities before we get the list of pending pods. This ensures that
	// the node capacities we schedule against are always >= what the actual capacity is at any given instance. This
	// prevents over-provisioning at the cost of potentially under-provisioning which will self-heal during the next
	// scheduling loop when we launch a new node.  When this order is reversed, our node capacity may be reduced by pods
	// that have bound which we then provision new un-needed capacity for.
	var stateNodes []*state.Node
	var markedForDeletionNodes []*state.Node
	p.cluster.ForEachNode(func(node *state.Node) bool {
		// We don't consider the nodes that are MarkedForDeletion since this capacity shouldn't be considered
		// as persistent capacity for the cluster (since it will soon be removed). Additionally, we are scheduling for
		// the pods that are on these nodes so the MarkedForDeletion node capacity can't be considered.
		if !node.MarkedForDeletion {
			stateNodes = append(stateNodes, node.DeepCopy())
		} else {
			markedForDeletionNodes = append(markedForDeletionNodes, node.DeepCopy())
		}
		return true
	})

	// Get pods, exit if nothing to do
	pendingPods, err := p.GetPendingPods(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}
	// Get pods from nodes that are preparing for deletion
	// We do this after getting the pending pods so that we undershoot if pods are
	// actively migrating from a node that is being deleted
	// NOTE: The assumption is that these nodes are cordoned and no additional pods will schedule to them
	deletingNodePods, err := node.GetNodePods(ctx, p.kubeClient, lo.Map(markedForDeletionNodes, func(n *state.Node, _ int) *v1.Node { return n.Node })...)
	if err != nil {
		return reconcile.Result{}, err
	}
	pods := append(pendingPods, deletingNodePods...)
	if len(pods) == 0 {
		return reconcile.Result{}, nil
	}

	// Schedule pods to potential nodes, exit if nothing to do
	nodes, err := p.schedule(ctx, pods, stateNodes)
	if err != nil {
		return reconcile.Result{}, err
	}
	if len(nodes) == 0 {
		return reconcile.Result{}, nil
	}

	nodeNames, err := p.LaunchNodes(ctx, LaunchOptions{RecordPodNomination: true}, nodes...)

	// Any successfully created node is going to have the nodeName value filled in the slice
	successfullyCreatedNodeCount := lo.CountBy(nodeNames, func(name string) bool { return name != "" })
	metrics.NodesCreatedCounter.WithLabelValues(metrics.ProvisioningReason).Add(float64(successfullyCreatedNodeCount))

	return reconcile.Result{}, err
}

type LaunchOptions struct {
	// RecordPodNomination causes nominate pod events to be recorded against the node.
	RecordPodNomination bool
}

// LaunchNodes launches nodes passed into the function in parallel. It returns a slice of the successfully created node
// names as well as a multierr of any errors that occurred while launching nodes
func (p *Provisioner) LaunchNodes(ctx context.Context, opts LaunchOptions, nodes ...*scheduler.Node) ([]string, error) {
	// Launch capacity and bind pods
	errs := make([]error, len(nodes))
	nodeNames := make([]string, len(nodes))
	workqueue.ParallelizeUntil(ctx, len(nodes), len(nodes), func(i int) {
		// create a new context to avoid a data race on the ctx variable
		ctx := logging.WithLogger(ctx, logging.FromContext(ctx).With("provisioner", nodes[i].Labels[v1alpha5.ProvisionerNameLabelKey]))
		// register the provisioner on the context so we can pull it off for tagging purposes
		// TODO: rethink this, maybe just pass the provisioner down instead of hiding it in the context?
		ctx = injection.WithNamespacedName(ctx, types.NamespacedName{Name: nodes[i].Labels[v1alpha5.ProvisionerNameLabelKey]})
		if nodeName, err := p.launch(ctx, opts, nodes[i]); err != nil {
			errs[i] = fmt.Errorf("launching node, %w", err)
		} else {
			nodeNames[i] = nodeName
		}
	})
	if err := multierr.Combine(errs...); err != nil {
		return nodeNames, err
	}
	return nodeNames, nil
}

func (p *Provisioner) GetPendingPods(ctx context.Context) ([]*v1.Pod, error) {
	var podList v1.PodList
	if err := p.kubeClient.List(ctx, &podList, client.MatchingFields{"spec.nodeName": ""}); err != nil {
		return nil, fmt.Errorf("listing pods, %w", err)
	}
	var pods []*v1.Pod
	for i := range podList.Items {
		po := podList.Items[i]
		// filter for provisionable pods first so we don't check for validity/PVCs on pods we won't provision anyway
		// (e.g. those owned by daemonsets)
		if !pod.IsProvisionable(&po) {
			continue
		}
		if err := p.Validate(ctx, &po); err != nil {
			logging.FromContext(ctx).With("pod", client.ObjectKeyFromObject(&po)).Debugf("ignoring pod, %s", err)
			continue
		}
		pods = append(pods, &po)
	}
	return pods, nil
}

// nolint: gocyclo
func (p *Provisioner) NewScheduler(ctx context.Context, pods []*v1.Pod, stateNodes []*state.Node, opts scheduler.SchedulerOptions) (*scheduler.Scheduler, error) {
	// Build node templates
	var nodeTemplates []*scheduling.NodeTemplate
	var provisionerList v1alpha5.ProvisionerList
	instanceTypes := map[string][]*cloudprovider.InstanceType{}
	domains := map[string]sets.String{}
	if err := p.kubeClient.List(ctx, &provisionerList); err != nil {
		return nil, fmt.Errorf("listing provisioners, %w", err)
	}

	// nodeTemplates generated from provisioners are ordered by weight
	// since they are stored within a slice and scheduling
	// will always attempt to schedule on the first nodeTemplate
	provisionerList.OrderByWeight()

	for i := range provisionerList.Items {
		provisioner := &provisionerList.Items[i]
		if !provisioner.DeletionTimestamp.IsZero() {
			continue
		}
		// Create node template
		nodeTemplates = append(nodeTemplates, scheduling.NewNodeTemplate(provisioner))
		// Get instance type options
		instanceTypeOptions, err := p.cloudProvider.GetInstanceTypes(ctx, provisioner)
		if err != nil {
			return nil, fmt.Errorf("getting instance types, %w", err)
		}
		instanceTypes[provisioner.Name] = append(instanceTypes[provisioner.Name], instanceTypeOptions...)

		// Construct Topology Domains
		for _, instanceType := range instanceTypeOptions {
			for key, requirement := range instanceType.Requirements {
				domains[key] = domains[key].Union(sets.NewString(requirement.Values()...))
			}
		}
		for key, requirement := range scheduling.NewNodeSelectorRequirements(provisioner.Spec.Requirements...) {
			if requirement.Operator() == v1.NodeSelectorOpIn {
				domains[key] = domains[key].Union(sets.NewString(requirement.Values()...))
			}
		}
	}
	if len(nodeTemplates) == 0 {
		return nil, fmt.Errorf("no provisioners found")
	}

	// inject topology constraints
	pods = p.injectTopology(ctx, pods)

	// Calculate cluster topology
	topology, err := scheduler.NewTopology(ctx, p.kubeClient, p.cluster, domains, pods)
	if err != nil {
		return nil, fmt.Errorf("tracking topology counts, %w", err)
	}

	// Calculate daemon overhead
	daemonOverhead, err := p.getDaemonOverhead(ctx, nodeTemplates)
	if err != nil {
		return nil, fmt.Errorf("getting daemon overhead, %w", err)
	}
	return scheduler.NewScheduler(ctx, p.kubeClient, nodeTemplates, provisionerList.Items, p.cluster, stateNodes, topology, instanceTypes, daemonOverhead, p.recorder, opts), nil
}

func (p *Provisioner) schedule(ctx context.Context, pods []*v1.Pod, stateNodes []*state.Node) ([]*scheduler.Node, error) {
	defer metrics.Measure(schedulingDuration.WithLabelValues(injection.GetNamespacedName(ctx).Name))()

	scheduler, err := p.NewScheduler(ctx, pods, stateNodes, scheduler.SchedulerOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating scheduler, %w", err)
	}

	// don't care about inflight scheduling results in this context
	nodes, _, err := scheduler.Solve(ctx, pods)
	return nodes, err
}

func (p *Provisioner) launch(ctx context.Context, opts LaunchOptions, node *scheduler.Node) (string, error) {
	// Check limits
	latest := &v1alpha5.Provisioner{}
	name := node.Requirements.Get(v1alpha5.ProvisionerNameLabelKey).Values()[0]
	if err := p.kubeClient.Get(ctx, types.NamespacedName{Name: name}, latest); err != nil {
		return "", fmt.Errorf("getting current resource usage, %w", err)
	}
	if err := latest.Spec.Limits.ExceededBy(latest.Status.Resources); err != nil {
		return "", err
	}

	// Order instance types so that we get the cheapest instance types of the available offerings
	sort.Slice(node.InstanceTypeOptions, func(i, j int) bool {
		iOfferings := node.InstanceTypeOptions[i].Offerings.Available()
		jOfferings := node.InstanceTypeOptions[j].Offerings.Available()
		return cheapestOfferingPrice(iOfferings, node.Requirements) < cheapestOfferingPrice(jOfferings, node.Requirements)
	})

	logging.FromContext(ctx).Infof("launching %s", node)
	k8sNode, err := p.cloudProvider.Create(
		logging.WithLogger(ctx, logging.FromContext(ctx).Named("cloudprovider")),
		&cloudprovider.NodeRequest{InstanceTypeOptions: node.InstanceTypeOptions, Template: &node.NodeTemplate},
	)
	if err != nil {
		return "", fmt.Errorf("creating cloud provider instance, %w", err)
	}
	ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("node", k8sNode.Name))

	if err := mergo.Merge(k8sNode, node.ToNode()); err != nil {
		return "", fmt.Errorf("merging cloud provider node, %w", err)
	}
	// ensure we clear out the status
	k8sNode.Status = v1.NodeStatus{}

	// Idempotently create a node. In rare cases, nodes can come online and
	// self register before the controller is able to register a node object
	// with the API server. In the common case, we create the node object
	// ourselves to enforce the binding decision and enable images to be pulled
	// before the node is fully Ready.
	if _, err := p.coreV1Client.Nodes().Create(ctx, k8sNode, metav1.CreateOptions{}); err != nil {
		if errors.IsAlreadyExists(err) {
			logging.FromContext(ctx).Debugf("node already registered")
		} else {
			return "", fmt.Errorf("creating node %s, %w", k8sNode.Name, err)
		}
	}
	p.cluster.NominateNodeForPod(k8sNode.Name)
	if err := p.cluster.UpdateNode(ctx, k8sNode); err != nil {
		return "", fmt.Errorf("updating cluster state, %w", err)
	}
	if opts.RecordPodNomination {
		for _, pod := range node.Pods {
			p.recorder.Publish(events.NominatePod(pod, k8sNode))
		}
	}
	return k8sNode.Name, nil
}

func (p *Provisioner) getDaemonOverhead(ctx context.Context, nodeTemplates []*scheduling.NodeTemplate) (map[*scheduling.NodeTemplate]v1.ResourceList, error) {
	overhead := map[*scheduling.NodeTemplate]v1.ResourceList{}

	daemonSetList := &appsv1.DaemonSetList{}
	if err := p.kubeClient.List(ctx, daemonSetList); err != nil {
		return nil, fmt.Errorf("listing daemonsets, %w", err)
	}

	for _, nodeTemplate := range nodeTemplates {
		var daemons []*v1.Pod
		for _, daemonSet := range daemonSetList.Items {
			p := &v1.Pod{Spec: daemonSet.Spec.Template.Spec}
			if err := nodeTemplate.Taints.Tolerates(p); err != nil {
				continue
			}
			if err := nodeTemplate.Requirements.Compatible(scheduling.NewPodRequirements(p)); err != nil {
				continue
			}
			daemons = append(daemons, p)
		}
		overhead[nodeTemplate] = resources.RequestsForPods(daemons...)
	}

	return overhead, nil
}

func (p *Provisioner) Validate(ctx context.Context, pod *v1.Pod) error {
	return multierr.Combine(
		validateAffinity(pod),
		p.volumeTopology.validatePersistentVolumeClaims(ctx, pod),
	)
}

func (p *Provisioner) injectTopology(ctx context.Context, pods []*v1.Pod) []*v1.Pod {
	var schedulablePods []*v1.Pod
	for _, pod := range pods {
		if err := p.volumeTopology.Inject(ctx, pod); err != nil {
			logging.FromContext(ctx).With("pod", client.ObjectKeyFromObject(pod)).Errorf("getting volume topology requirements, %w", err)
		} else {
			schedulablePods = append(schedulablePods, pod)
		}
	}
	return schedulablePods
}

// cheapestOfferingPrice gets the cheapest price of an offering on an instance type given
// the node requirements
func cheapestOfferingPrice(ofs []cloudprovider.Offering, requirements scheduling.Requirements) float64 {
	minPrice := math.MaxFloat64
	for _, of := range ofs {
		if requirements.Get(v1alpha5.LabelCapacityType).Has(of.CapacityType) && requirements.Get(v1.LabelTopologyZone).Has(of.Zone) {
			minPrice = math.Min(minPrice, of.Price)
		}
	}
	return minPrice
}

func validateAffinity(p *v1.Pod) (errs error) {
	if p.Spec.Affinity == nil {
		return nil
	}
	if p.Spec.Affinity.NodeAffinity != nil {
		for _, term := range p.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			errs = multierr.Append(errs, validateNodeSelectorTerm(term.Preference))
		}
		if p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			for _, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				errs = multierr.Append(errs, validateNodeSelectorTerm(term))
			}
		}
	}
	return errs
}

func validateNodeSelectorTerm(term v1.NodeSelectorTerm) (errs error) {
	if term.MatchFields != nil {
		errs = multierr.Append(errs, fmt.Errorf("node selector term with matchFields is not supported"))
	}
	if term.MatchExpressions != nil {
		for _, requirement := range term.MatchExpressions {
			errs = multierr.Append(errs, v1alpha5.ValidateRequirement(requirement))
		}
	}
	return errs
}

var schedulingDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: "allocation_controller",
		Name:      "scheduling_duration_seconds",
		Help:      "Duration of scheduling process in seconds. Broken down by provisioner and error.",
		Buckets:   metrics.DurationBuckets(),
	},
	[]string{metrics.ProvisionerLabel},
)

func init() {
	crmetrics.Registry.MustRegister(schedulingDuration)
}
