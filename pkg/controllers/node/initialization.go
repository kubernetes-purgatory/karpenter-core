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

package node

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"

	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/utils/node"
	"github.com/aws/karpenter-core/pkg/utils/resources"
)

type Initialization struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
}

// Reconcile reconciles the node
func (r *Initialization) Reconcile(ctx context.Context, provisioner *v1alpha5.Provisioner, n *v1.Node) (reconcile.Result, error) {
	// node has been previously determined to be ready, so there's nothing to do
	if n.Labels[v1alpha5.LabelNodeInitialized] == "true" {
		return reconcile.Result{}, nil
	}

	// node is not ready per the label, we need to check if kubelet indicates that the node is ready as well as if
	// startup taints are removed and extended resources have been initialized
	instanceType, err := r.getInstanceType(ctx, provisioner, n.Labels[v1.LabelInstanceTypeStable])
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("determining instance type, %w", err)
	}
	if !r.isInitialized(n, provisioner, instanceType) {
		return reconcile.Result{}, nil
	}

	n.Labels[v1alpha5.LabelNodeInitialized] = "true"
	return reconcile.Result{}, nil
}

func (r *Initialization) getInstanceType(ctx context.Context, provisioner *v1alpha5.Provisioner, instanceTypeName string) (*cloudprovider.InstanceType, error) {
	instanceTypes, err := r.cloudProvider.GetInstanceTypes(ctx, provisioner)
	if err != nil {
		return nil, err
	}
	// The instance type may not be found which can occur if the instance type label was removed/edited.  This shouldn't occur,
	// but if it does we only lose the ability to check for extended resources.
	return lo.FindOrElse(instanceTypes, nil, func(it *cloudprovider.InstanceType) bool { return it.Name == instanceTypeName }), nil
}

// isInitialized returns true if the node has:
// a) its current status is set to Ready
// b) all the startup taints have been removed from the node
// c) all extended resources have been registered
// This method handles both nil provisioners and nodes without extended resources gracefully.
func (r *Initialization) isInitialized(n *v1.Node, provisioner *v1alpha5.Provisioner, instanceType *cloudprovider.InstanceType) bool {
	// fast checks first
	if node.GetCondition(n, v1.NodeReady).Status != v1.ConditionTrue {
		return false
	}
	if _, ok := IsStartupTaintRemoved(n, provisioner); !ok {
		return false
	}

	if _, ok := IsExtendedResourceRegistered(n, instanceType); !ok {
		return false
	}
	return true
}

// IsStartupTaintRemoved returns true if there are no startup taints registered for the provisioner, or if all startup
// taints have been removed from the node
func IsStartupTaintRemoved(node *v1.Node, provisioner *v1alpha5.Provisioner) (*v1.Taint, bool) {
	if provisioner != nil {
		for _, startupTaint := range provisioner.Spec.StartupTaints {
			for i := 0; i < len(node.Spec.Taints); i++ {
				// if the node still has a startup taint applied, it's not ready
				if startupTaint.MatchTaint(&node.Spec.Taints[i]) {
					return &node.Spec.Taints[i], false
				}
			}
		}
	}
	return nil, true
}

// IsExtendedResourceRegistered returns true if there are no extended resources on the node, or they have all been
// registered by device plugins
func IsExtendedResourceRegistered(node *v1.Node, instanceType *cloudprovider.InstanceType) (v1.ResourceName, bool) {
	if instanceType == nil {
		// no way to know, so assume they're registered
		return "", true
	}
	for resourceName, quantity := range instanceType.Capacity {
		if quantity.IsZero() {
			continue
		}
		// kubelet will zero out both the capacity and allocatable for an extended resource on startup, so if our
		// annotation says the resource should be there, but it's zero'd in both then the device plugin hasn't
		// registered it yet.
		// We wait on allocatable since this is the value that is used in scheduling
		if resources.IsZero(node.Status.Allocatable[resourceName]) {
			return resourceName, false
		}
	}
	return "", true
}
