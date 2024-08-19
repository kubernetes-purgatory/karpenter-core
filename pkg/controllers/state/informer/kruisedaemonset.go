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

package informer

import (
	"context"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"

	"sigs.k8s.io/karpenter/pkg/operator/options"

	kruisev1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	kruisev1beta1 "github.com/openkruise/kruise/apis/apps/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/operator/injection"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func init() {
	gv := schema.GroupVersion{Group: "apps.kruise.io", Version: "v1alpha1"}
	v1.AddToGroupVersion(scheme.Scheme, gv)
	scheme.Scheme.AddKnownTypes(gv, &kruisev1alpha1.DaemonSet{})

	gv = schema.GroupVersion{Group: "apps.kruise.io", Version: "v1beta1"}
	v1.AddToGroupVersion(scheme.Scheme, gv)
	scheme.Scheme.AddKnownTypes(gv, &kruisev1beta1.StatefulSet{})
}

type KruiseDaemonSetController struct {
	kubeClient client.Client
	cluster    *state.Cluster
}

func NewKruiseDaemonSetController(kubeClient client.Client, cluster *state.Cluster) *KruiseDaemonSetController {
	return &KruiseDaemonSetController{
		kubeClient: kubeClient,
		cluster:    cluster,
	}
}

func (c *KruiseDaemonSetController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, "state.kruise-daemonset")

	daemonSet := kruisev1alpha1.DaemonSet{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, &daemonSet); err != nil {
		if errors.IsNotFound(err) {
			// notify cluster state of the daemonset deletion
			c.cluster.DeleteDaemonSet(state.ObjectKey{
				Group:     "apps.kruise.io",
				Kind:      "DaemonSet",
				Namespace: req.Namespace,
				Name:      req.Name,
			})
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if err := c.cluster.UpdateDaemonSet(ctx, &daemonSet); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: time.Minute}, nil
}

func (c *KruiseDaemonSetController) Register(ctx context.Context, m manager.Manager) error {
	if !options.FromContext(ctx).SupportKruise {
		return nil
	}

	return controllerruntime.NewControllerManagedBy(m).
		Named("state.kruise-daemonset").
		For(&kruisev1alpha1.DaemonSet{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(c)
}
