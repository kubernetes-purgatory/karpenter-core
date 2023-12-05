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

package lifecycle

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"knative.dev/pkg/logging"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	corecontroller "sigs.k8s.io/karpenter/pkg/operator/controller"
	nodepoolutil "sigs.k8s.io/karpenter/pkg/utils/nodepool"
)

// Controller is hash controller that constructs a hash based on the fields that are considered for static drift.
// The hash is placed in the metadata for increased observability and should be found on each object.
type Controller struct {
	kubeClient client.Client
    dynamicClient dynamic.DynamicClient
}

func NewController(kubeClient client.Client,dynamicClient dynamic.DynamicClient) *Controller {
	return &Controller{
		kubeClient: kubeClient,
        dynamicClient: dynamicClient,
	}
}


func (c *Controller) Reconcile(ctx context.Context, np *v1beta1.NodePool) (reconcile.Result, error) {
    nodeClassRef := np.Spec.Template.Spec.NodeClassRef
    if nodeClassRef == nil {
        return reconcile.Result{}, fmt.Errorf("nodeClassRef is nil")
    }

    group,version,found:= strings.Cut(nodeClassRef.APIVersion, "/")

    if !found {
        return reconcile.Result{}, fmt.Errorf("failed to parse apiVersion: %v", nodeClassRef.APIVersion)
    }

    gvr := schema.GroupVersionResource{
        Group:    group,
        Version:  version,
        Resource: strings.ToLower(nodeClassRef.Kind) + "es", 
    }

    nodeClassUnstructured, err := c.dynamicClient.Resource(gvr).Namespace(np.Namespace).Get(ctx, nodeClassRef.Name, metav1.GetOptions{})
    if err != nil {
        return reconcile.Result{}, fmt.Errorf("failed to get resource: %v", err)
    }

    stored := np.DeepCopy()
    // Check if the resource is ready (or perform your readiness checks here)
    if isResourceReady(nodeClassUnstructured) {
        np.StatusConditions().MarkTrue(v1beta1.NodeClassReady)
    } else {
        np.StatusConditions().MarkFalse(v1beta1.NodeClassReady, "NodeClassNotReady", "NodeClass is not ready")
    }

    if !equality.Semantic.DeepEqual(stored, np) {
        if err := nodepoolutil.PatchStatus(ctx, c.kubeClient, stored, np); err != nil {
            logging.FromContext(ctx).With("nodepool", np.Name).Errorf("unable to update nodeclass readiness into nodepool, %s", err)
        }
    }

    return reconcile.Result{}, nil
}

func isResourceReady(nodeClassUnstructured *unstructured.Unstructured) bool {

    status, found, err := unstructured.NestedFieldCopy(nodeClassUnstructured.Object, "status")
    if err != nil || !found {
        return false
    }

    conditions, found, err := unstructured.NestedSlice(status.(map[string]interface{}), "conditions")
    if err != nil || !found {
        return false
    }

    for _, condition := range conditions {
        conditionMap, ok := condition.(map[string]interface{})
        if !ok {
            continue
        }

        conditionType, typeOk := conditionMap["type"].(string)
        if !typeOk {
            continue
        }
        conditionStatus, _ := conditionMap["status"].(string)

        if conditionStatus == "True" && conditionType == "Ready" {
            return true
        }
    }
        return false
}

type NodePoolController struct {
	*Controller
}

func NewNodePoolController(kubeClient client.Client,dynamicClient dynamic.DynamicClient) corecontroller.Controller {
	return corecontroller.Typed[*v1beta1.NodePool](kubeClient, &NodePoolController{
		Controller: NewController(kubeClient,dynamicClient),
	})
}

func (c *NodePoolController) Name() string {
	return "nodepool.lifecycle"
}

func (c *NodePoolController) Builder(_ context.Context, m manager.Manager) corecontroller.Builder {
    var apiVersion string
    var kind string

    nodePoolList := &v1beta1.NodePoolList{}
    if err := c.kubeClient.List(context.Background(), nodePoolList); err != nil {
        return nil
    }

    for _, nodePool := range nodePoolList.Items {
        apiVersion = nodePool.Spec.Template.Spec.NodeClassRef.APIVersion
        kind = nodePool.Spec.Template.Spec.NodeClassRef.Kind
        break
    }

    return corecontroller.Adapt(controllerruntime.
        NewControllerManagedBy(m).
        WithEventFilter(predicate.Funcs{}).
        For(&v1beta1.NodePool{}).
        WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
        Watches(
           &unstructured.Unstructured{
                Object: map[string]interface{}{
                    "apiVersion": apiVersion,
                    "kind":       kind,
                },
            },
            handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
                requests := []reconcile.Request{}
                for _, nodePool := range nodePoolList.Items {
                    if nodePool.Spec.Template.Spec.NodeClassRef.Name == o.GetName() {
                        requests = append(requests, reconcile.Request{
                        NamespacedName: client.ObjectKey{
                            Name:      nodePool.Name,
                            Namespace: nodePool.Namespace,
                        },
                    })
                    }
                }
                return requests
            }),
        ),
    )
}

