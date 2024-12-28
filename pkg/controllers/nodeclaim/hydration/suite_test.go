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

package hydration_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/hydration"
	"sigs.k8s.io/karpenter/pkg/operator"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var hydrationController *hydration.Controller
var env *test.Environment
var cloudProvider *fake.CloudProvider

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lifecycle")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...), test.WithFieldIndexers(test.NodeProviderIDFieldIndexer(ctx)))
	ctx = options.ToContext(ctx, test.Options())

	cloudProvider = fake.NewCloudProvider()
	hydrationController = hydration.NewController(env.Client, cloudProvider)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
	cloudProvider.Reset()
})

var _ = Describe("Hydration", func() {
	var nodeClaim *v1.NodeClaim
	BeforeEach(func() {
		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{v1.HydrationAnnotationKey: "not-hydrated"},
			},
		})
	})

	It("should hydrate the NodeClass label", func() {
		delete(nodeClaim.Labels, v1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind()))
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, hydrationController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Labels[v1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind())]).To(Equal(nodeClaim.Spec.NodeClassRef.Name))
	})
	It("shouldn't hydrate nodes which have already been hydrated", func() {
		nodeClaim.Annotations[v1.HydrationAnnotationKey] = operator.Version
		delete(nodeClaim.Labels, v1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind()))
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, hydrationController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(lo.Keys(nodeClaim.Labels)).ToNot(ContainElement(v1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind())))
	})
	It("shouldn't hydrate NodeClaims which aren't managed by this instance of Karpenter", func() {
		nodeClaim.Spec.NodeClassRef = &v1.NodeClassReference{
			Group: "karpenter.test.sh",
			Kind:  "UnmanagedNodeClass",
			Name:  "default",
		}
		delete(nodeClaim.Labels, v1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind()))
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, hydrationController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(lo.Keys(nodeClaim.Labels)).ToNot(ContainElement(v1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind())))
	})
})
