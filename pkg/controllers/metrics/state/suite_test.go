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

package metrics_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	clock "k8s.io/utils/clock/testing"

	io_prometheus_client "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis"
	"github.com/aws/karpenter-core/pkg/apis/config/settings"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	statemetrics "github.com/aws/karpenter-core/pkg/controllers/metrics/state/scraper"
	"github.com/aws/karpenter-core/pkg/operator/controller"
	"github.com/aws/karpenter-core/pkg/operator/scheme"

	"github.com/aws/karpenter-core/pkg/cloudprovider/fake"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "knative.dev/pkg/logging/testing"

	. "github.com/aws/karpenter-core/pkg/test/expectations"
)

var ctx context.Context
var fakeClock *clock.FakeClock
var env *test.Environment
var cluster *state.Cluster
var nodeController controller.Controller
var podController controller.Controller
var cloudProvider *fake.CloudProvider
var provisioner *v1alpha5.Provisioner
var nodeScraper *statemetrics.NodeScraper

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controllers/Metrics/State")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(scheme.Scheme, apis.CRDs...)

	ctx = settings.ToContext(ctx, test.Settings())
	cloudProvider = fake.NewCloudProvider()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	fakeClock = clock.NewFakeClock(time.Now())
	cluster = state.NewCluster(ctx, fakeClock, env.Client, cloudProvider)
	provisioner = test.Provisioner(test.ProvisionerOptions{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	nodeController = state.NewNodeController(env.Client, cluster)
	podController = state.NewPodController(env.Client, cluster)
	nodeScraper = statemetrics.NewNodeScraper(cluster)
	ExpectApplied(ctx, env.Client, provisioner)
})

var _ = AfterSuite(func() {
	ExpectCleanedUp(ctx, env.Client)
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Node Metrics", func() {
	It("should update the allocatable metric", func() {
		resources := v1.ResourceList{
			v1.ResourcePods:   resource.MustParse("100"),
			v1.ResourceCPU:    resource.MustParse("5000"),
			v1.ResourceMemory: resource.MustParse("32Gi"),
		}

		node := test.Node(test.NodeOptions{Allocatable: resources})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		// metrics should now be tracking the allocatable capacity of our single node
		nodeScraper.Scrape(ctx)
		nodeAllocation := ExpectMetric("karpenter_nodes_allocatable")

		expectedValues := map[string]float64{
			"cpu":    float64(resources.Cpu().MilliValue()) / float64(1000),
			"pods":   float64(resources.Pods().Value()),
			"memory": float64(resources.Memory().Value()),
		}

		var metrics []*io_prometheus_client.Metric
		for _, m := range nodeAllocation.Metric {
			for _, l := range m.Label {
				if l.GetName() == "node_name" && l.GetValue() == node.GetName() {
					metrics = append(metrics, m)
				}
			}
		}

		for _, metric := range metrics {
			for _, l := range metric.Label {
				if l.GetName() == "resource_type" {
					Expect(metric.GetGauge().GetValue()).To(Equal(expectedValues[l.GetValue()]),
						fmt.Sprintf("%s, %f to equal %f", l.GetValue(), metric.GetGauge().GetValue(),
							expectedValues[l.GetValue()]))
				}
			}
		}
	})
})
