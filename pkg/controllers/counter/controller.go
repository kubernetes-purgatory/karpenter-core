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

package counter

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	corecontroller "github.com/aws/karpenter-core/pkg/operator/controller"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/aws/karpenter-core/pkg/utils/resources"
)

var _ corecontroller.TypedController[*v1alpha5.Provisioner] = (*Controller)(nil)

// Controller for the resource
type Controller struct {
	kubeClient client.Client
	cluster    *state.Cluster
}

// NewController is a constructor
func NewController(kubeClient client.Client, cluster *state.Cluster) corecontroller.Controller {
	return corecontroller.Typed[*v1alpha5.Provisioner](kubeClient, &Controller{
		kubeClient: kubeClient,
		cluster:    cluster,
	})
}

func (c *Controller) Name() string {
	return "counter"
}

// Reconcile a control loop for the resource
func (c *Controller) Reconcile(ctx context.Context, provisioner *v1alpha5.Provisioner) (reconcile.Result, error) {
	stored := provisioner.DeepCopy()
	nodes := v1.NodeList{}
	if err := c.kubeClient.List(ctx, &nodes, client.MatchingLabels{v1alpha5.ProvisionerNameLabelKey: provisioner.Name}); err != nil {
		return reconcile.Result{}, err
	}
	// Nodes aren't synced yet, so return an error which will cause retry with backoff.
	if !c.nodesSynced(nodes.Items, provisioner.Name) {
		return reconcile.Result{RequeueAfter: 250 * time.Millisecond}, nil
	}
	// Determine resource usage and update provisioner.status.resources
	provisioner.Status.Resources = c.resourceCountsFor(provisioner.Name)
	if !equality.Semantic.DeepEqual(stored, provisioner) {
		if err := c.kubeClient.Status().Patch(ctx, provisioner, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) resourceCountsFor(provisionerName string) v1.ResourceList {
	var provisioned []v1.ResourceList
	// Record all resources provisioned by the provisioners, we look at the cluster state nodes as their capacity
	// is accurately reported even for nodes that haven't fully started yet. This allows us to update our provisioner
	// status immediately upon node creation instead of waiting for the node to become ready.
	c.cluster.ForEachNode(func(n *state.Node) bool {
		if n.Node.Labels[v1alpha5.ProvisionerNameLabelKey] == provisionerName {
			provisioned = append(provisioned, n.Capacity())
		}
		return true
	})

	result := v1.ResourceList{}
	// only report the non-zero resources
	for key, value := range resources.Merge(provisioned...) {
		if value.IsZero() {
			continue
		}
		result[key] = value
	}
	return result
}

func (c *Controller) Builder(_ context.Context, m manager.Manager) corecontroller.Builder {
	return corecontroller.Adapt(controllerruntime.
		NewControllerManagedBy(m).
		For(&v1alpha5.Provisioner{}).
		Watches(
			&source.Kind{Type: &v1.Node{}},
			handler.EnqueueRequestsFromMapFunc(func(o client.Object) []reconcile.Request {
				if name, ok := o.GetLabels()[v1alpha5.ProvisionerNameLabelKey]; ok {
					return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
				}
				return nil
			}),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}))
}

// nodesSynced returns true if the cluster state is synced with the current list cache state with respect to the nodes
// created by the specified provisioner. Since updates may occur for the counting controller at a different time than
// the cluster state controller, we don't update the counter state until the states are synced.  An alternative solution
// would be to add event support to cluster state and listen for those node events instead.
func (c *Controller) nodesSynced(nodes []v1.Node, provisionerName string) bool {
	extraNodes := sets.String{}
	for _, n := range nodes {
		extraNodes.Insert(n.Name)
	}
	missingNode := false
	c.cluster.ForEachNode(func(n *state.Node) bool {
		// skip any nodes not created by this provisioner
		if n.Node.Labels[v1alpha5.ProvisionerNameLabelKey] != provisionerName {
			return true
		}
		if !extraNodes.Has(n.Node.Name) {
			missingNode = true
			return false
		}
		extraNodes.Delete(n.Node.Name)
		return true
	})

	if !missingNode && len(extraNodes) == 0 {
		return true
	}
	return false
}
