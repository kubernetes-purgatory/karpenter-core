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

package eviction_test

import (
	"context"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/eviction"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var recorder *test.EventRecorder
var queue *eviction.Queue
var pdb *policyv1.PodDisruptionBudget
var pod *corev1.Pod

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Eviction")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
	ctx = options.ToContext(ctx, test.Options())
	recorder = test.NewEventRecorder()
	queue = eviction.NewTestingQueue(env.Client, recorder)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	recorder.Reset() // Reset the events that we captured during the run
	// Shut down the queue and restart it to ensure no races
	*queue = lo.FromPtr(eviction.NewTestingQueue(env.Client, recorder))
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

var testLabels = map[string]string{"test": "label"}

var _ = Describe("Eviction/Queue", func() {
	BeforeEach(func() {
		pdb = test.PodDisruptionBudget(test.PDBOptions{
			Labels:         testLabels,
			MaxUnavailable: &intstr.IntOrString{IntVal: 0},
		})
		pod = test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: testLabels,
			},
		})
		eviction.NodesEvictionRequestsTotal.Reset()
	})

	Context("Eviction API", func() {
		It("should succeed with no event when the pod is not found", func() {
			Expect(queue.Evict(ctx, eviction.NewQueueKey(pod))).To(BeTrue())
			Expect(recorder.Events()).To(HaveLen(0))
		})
		It("should succeed with no event when the pod UID conflicts", func() {
			ExpectApplied(ctx, env.Client, pod)
			Expect(queue.Evict(ctx, eviction.QueueKey{NamespacedName: client.ObjectKeyFromObject(pod), UID: uuid.NewUUID()})).To(BeTrue())
			ExpectMetricCounterValue(eviction.NodesEvictionRequestsTotal, 1, map[string]string{eviction.CodeLabel: "409"})
			Expect(recorder.Events()).To(HaveLen(0))
		})
		It("should succeed with an evicted event when there are no PDBs", func() {
			ExpectApplied(ctx, env.Client, pod)
			Expect(queue.Evict(ctx, eviction.NewQueueKey(pod))).To(BeTrue())
			ExpectMetricCounterValue(eviction.NodesEvictionRequestsTotal, 1, map[string]string{eviction.CodeLabel: "200"})
			Expect(recorder.Calls("Evicted")).To(Equal(1))
		})
		It("should succeed with no event when there are PDBs that allow an eviction", func() {
			pdb = test.PodDisruptionBudget(test.PDBOptions{
				Labels:         testLabels,
				MaxUnavailable: &intstr.IntOrString{IntVal: 1},
			})
			ExpectApplied(ctx, env.Client, pod)
			Expect(queue.Evict(ctx, eviction.NewQueueKey(pod))).To(BeTrue())
			Expect(recorder.Calls("Evicted")).To(Equal(1))
		})
		It("should emit a FailedEviction event when a PDB is blocking", func() {
			ExpectApplied(ctx, env.Client, pdb, pod)
			Expect(queue.Evict(ctx, eviction.NewQueueKey(pod))).To(BeFalse())
			Expect(recorder.Calls("FailedEviction")).To(Equal(1))
		})
		It("should fail when two PDBs refer to the same pod", func() {
			pdb2 := test.PodDisruptionBudget(test.PDBOptions{
				Labels:         testLabels,
				MaxUnavailable: &intstr.IntOrString{IntVal: 0},
			})
			ExpectApplied(ctx, env.Client, pdb, pdb2, pod)
			Expect(queue.Evict(ctx, eviction.NewQueueKey(pod))).To(BeFalse())
			ExpectMetricCounterValue(eviction.NodesEvictionRequestsTotal, 1, map[string]string{eviction.CodeLabel: "500"})
		})
		It("should ensure that calling Evict() is valid while making Add() calls", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			wg := sync.WaitGroup{}
			DeferCleanup(func() {
				cancel()
				wg.Wait() // Ensure that we wait for reconcile loop to finish so that we don't get a RACE
			})

			// Keep calling Reconcile() for the entirety of this test
			wg.Add(1)
			go func() {
				defer wg.Done()

				for {
					ExpectSingletonReconciled(ctx, queue)
					if cancelCtx.Err() != nil {
						return
					}
				}
			}()

			// Ensure that we add enough pods to the queue while we are pulling items off of the queue (enough to trigger a DATA RACE)
			for i := 0; i < 10000; i++ {
				queue.Add(test.Pod())
			}
		})
	})
})
