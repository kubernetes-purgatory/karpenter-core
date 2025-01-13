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

package v1_test

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Pallinder/go-randomdata"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	. "sigs.k8s.io/karpenter/pkg/apis/v1"
)

var _ = Describe("CEL/Validation", func() {
	var nodePool *NodePool

	BeforeEach(func() {
		if env.Version.Minor() < 25 {
			Skip("CEL Validation is for 1.25>")
		}
		nodePool = &NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: strings.ToLower(randomdata.SillyName())},
			Spec: NodePoolSpec{
				Template: NodeClaimTemplate{
					Spec: NodeClaimTemplateSpec{
						NodeClassRef: &NodeClassReference{
							Group: "karpenter.test.sh",
							Kind:  "TestNodeClass",
							Name:  "default",
						},
						Requirements: []NodeSelectorRequirementWithMinValues{
							{
								NodeSelectorRequirement: v1.NodeSelectorRequirement{
									Key:      CapacityTypeLabelKey,
									Operator: v1.NodeSelectorOpExists,
								},
							},
						},
					},
				},
			},
		}
	})
	Context("Disruption", func() {
		It("should fail on negative expireAfter", func() {
			nodePool.Spec.Template.Spec.ExpireAfter = MustParseNillableDuration("-1s")
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should succeed on a disabled expireAfter", func() {
			nodePool.Spec.Template.Spec.ExpireAfter = MustParseNillableDuration("Never")
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed on a valid expireAfter", func() {
			nodePool.Spec.Template.Spec.ExpireAfter = MustParseNillableDuration("30s")
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail on negative consolidateAfter", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = MustParseNillableDuration("-1s")
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should succeed on a disabled consolidateAfter", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = MustParseNillableDuration("Never")
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed on a valid consolidateAfter", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = MustParseNillableDuration("30s")
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenEmpty
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when setting consolidateAfter with consolidationPolicy=WhenEmpty", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = MustParseNillableDuration("30s")
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenEmpty
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when setting consolidateAfter with consolidationPolicy=WhenUnderutilized", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = MustParseNillableDuration("30s")
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenEmptyOrUnderutilized
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when setting consolidateAfter to 'Never' with consolidationPolicy=WhenUnderutilized", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = MustParseNillableDuration("Never")
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenEmptyOrUnderutilized
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when setting consolidateAfter to 'Never' with consolidationPolicy=WhenEmpty", func() {
			nodePool.Spec.Disruption.ConsolidateAfter = MustParseNillableDuration("Never")
			nodePool.Spec.Disruption.ConsolidationPolicy = ConsolidationPolicyWhenEmpty
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail when creating a budget with an invalid cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("*"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a schedule with less than 5 entries", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * "),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a negative duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("-20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a seconds duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("30s"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a negative value int", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "-10",
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a negative value percent", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "-10%",
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a value percent with more than 3 digits", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "1000%",
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a cron but no duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating a budget with a duration but no cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should succeed when creating a budget with both duration and cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when creating a budget with hours and minutes in duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("2h20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when creating a budget with neither duration nor cron", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes: "10",
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should succeed when creating a budget with special cased crons", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("@annually"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail when creating two budgets where one has an invalid crontab", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{
				{
					Nodes:    "10",
					Schedule: lo.ToPtr("@annually"),
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				},
				{
					Nodes:    "10",
					Schedule: lo.ToPtr("*"),
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail when creating multiple budgets where one doesn't have both schedule and duration", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{
				{
					Nodes:    "10",
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				},
				{
					Nodes:    "10",
					Schedule: lo.ToPtr("* * * * *"),
					Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				},
				{
					Nodes: "10",
				},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		DescribeTable("should succeed when creating a budget with valid reasons", func(reason DisruptionReason) {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				Reasons:  []DisruptionReason{reason},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		},
			Entry("should allow disruption reason Drifted", DisruptionReasonDrifted),
			Entry("should allow disruption reason Underutilized", DisruptionReasonUnderutilized),
			Entry("should allow disruption reason Empty", DisruptionReasonEmpty),
		)

		DescribeTable("should fail when creating a budget with invalid reasons", func(reason string) {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				Reasons:  []DisruptionReason{DisruptionReason(reason)},
			}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		},
			Entry("should not allow invalid reason", "invalid"),
			Entry("should not allow expired disruption reason", "expired"),
			Entry("should not allow empty reason", ""),
		)

		It("should allow setting multiple reasons", func() {
			nodePool.Spec.Disruption.Budgets = []Budget{{
				Nodes:    "10",
				Schedule: lo.ToPtr("* * * * *"),
				Duration: &metav1.Duration{Duration: lo.Must(time.ParseDuration("20m"))},
				Reasons:  []DisruptionReason{DisruptionReasonDrifted, DisruptionReasonEmpty},
			}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
	})
	Context("Taints", func() {
		It("should succeed for valid taints", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{
				{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule},
				{Key: "c", Value: "d", Effect: v1.TaintEffectNoExecute},
				{Key: "e", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "Test", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "test.com/Test", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "test.com.com/test", Value: "f", Effect: v1.TaintEffectPreferNoSchedule},
				{Key: "key-only", Effect: v1.TaintEffectNoExecute},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for invalid taint keys", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "test.com.com}", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "Test.com/test", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "test/test/test", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "test/", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "/test", Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail at runtime for taint keys that are too long", func() {
			oldNodePool := nodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: fmt.Sprintf("test.com.test.%s/test", strings.ToLower(randomdata.Alphanumeric(250))), Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool = oldNodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: fmt.Sprintf("test.com.test/test-%s", strings.ToLower(randomdata.Alphanumeric(250))), Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for missing taint key", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Effect: v1.TaintEffectNoSchedule}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid taint value", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "invalid-value", Effect: v1.TaintEffectNoSchedule, Value: "???"}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid taint effect", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{{Key: "invalid-effect", Effect: "???"}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should not fail for same key with different effects", func() {
			nodePool.Spec.Template.Spec.Taints = []v1.Taint{
				{Key: "a", Effect: v1.TaintEffectNoSchedule},
				{Key: "a", Effect: v1.TaintEffectNoExecute},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
	})
	Context("Requirements", func() {
		It("should succeed for valid requirement keys", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "Test", Operator: v1.NodeSelectorOpExists}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test.com/Test", Operator: v1.NodeSelectorOpExists}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test.com.com/test", Operator: v1.NodeSelectorOpExists}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "key-only", Operator: v1.NodeSelectorOpExists}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for invalid requirement keys", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test.com.com}", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "Test.com/test", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test/test/test", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "test/", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "/test", Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail at runtime for requirement keys that are too long", func() {
			oldNodePool := nodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: fmt.Sprintf("test.com.test.%s/test", strings.ToLower(randomdata.Alphanumeric(250))), Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool = oldNodePool.DeepCopy()
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: fmt.Sprintf("test.com.test/test-%s", strings.ToLower(randomdata.Alphanumeric(250))), Operator: v1.NodeSelectorOpExists}}}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for the karpenter.sh/nodepool label", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: NodePoolLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{randomdata.SillyName()}}}}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should allow supported ops", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"1"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"1"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpExists}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for unsupported ops", func() {
			for _, op := range []v1.NodeSelectorOperator{"unknown"} {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: op, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should fail for restricted domains", func() {
			for label := range RestrictedLabelDomains {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: label + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should allow kubernetes domains", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range K8sLabelDomains {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: label + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow kubernetes subdomains exceptions", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range K8sLabelDomains {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: "subdomain." + label + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow well known label exceptions", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range WellKnownLabels.Difference(sets.New(NodePoolLabelKey)) {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
					{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: label, Operator: v1.NodeSelectorOpIn, Values: []string{"test"}}},
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow non-empty set after removing overlapped value", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test", "foo"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"test", "bar"}}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should allow empty requirements", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail with invalid GT or LT values", func() {
			for _, requirement := range []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"1", "2"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"a"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpGt, Values: []string{"-1"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"1", "2"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"a"}}},
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpLt, Values: []string{"-1"}}},
			} {
				nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{requirement}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should error when minValues is negative", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"insance-type-1"}}, MinValues: lo.ToPtr(-1)},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should error when minValues is zero", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"insance-type-1"}}, MinValues: lo.ToPtr(0)},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should error when minValues is more than 50", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpExists}, MinValues: lo.ToPtr(51)},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should allow more than 50 values if minValues is not specified.", func() {
			var instanceTypes []string
			for i := 0; i < 90; i++ {
				instanceTypes = append(instanceTypes, "instance"+strconv.Itoa(i))
			}
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: instanceTypes}},
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should error when minValues is greater than the number of unique values specified within In operator", func() {
			nodePool.Spec.Template.Spec.Requirements = []NodeSelectorRequirementWithMinValues{
				{NodeSelectorRequirement: v1.NodeSelectorRequirement{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"insance-type-1"}}, MinValues: lo.ToPtr(2)},
			}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
	})
	Context("Labels", func() {
		It("should allow unrecognized labels", func() {
			nodePool.Spec.Template.Labels = map[string]string{"foo": randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should fail for the karpenter.sh/nodepool label", func() {
			nodePool.Spec.Template.Labels = map[string]string{NodePoolLabelKey: randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid label keys", func() {
			nodePool.Spec.Template.Labels = map[string]string{"spaces are not allowed": randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail at runtime for label keys that are too long", func() {
			oldNodePool := nodePool.DeepCopy()
			nodePool.Spec.Template.Labels = map[string]string{fmt.Sprintf("test.com.test.%s/test", strings.ToLower(randomdata.Alphanumeric(250))): randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			nodePool = oldNodePool.DeepCopy()
			nodePool.Spec.Template.Labels = map[string]string{fmt.Sprintf("test.com.test/test-%s", strings.ToLower(randomdata.Alphanumeric(250))): randomdata.SillyName()}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for invalid label values", func() {
			nodePool.Spec.Template.Labels = map[string]string{randomdata.SillyName(): "/ is not allowed"}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
			Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
		})
		It("should fail for restricted label domains", func() {
			for label := range RestrictedLabelDomains {
				fmt.Println(label)
				nodePool.Spec.Template.Labels = map[string]string{label + "/unknown": randomdata.SillyName()}
				Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
				Expect(nodePool.RuntimeValidate()).ToNot(Succeed())
			}
		})
		It("should allow labels kOps require", func() {
			nodePool.Spec.Template.Labels = map[string]string{
				"kops.k8s.io/instancegroup": "karpenter-nodes",
				"kops.k8s.io/gpu":           "1",
			}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			Expect(nodePool.RuntimeValidate()).To(Succeed())
		})
		It("should allow labels in kubernetes domains", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range K8sLabelDomains {
				fmt.Println(label)
				nodePool.Spec.Template.Labels = map[string]string{
					label: "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow labels prefixed with the kubernetes domain", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range K8sLabelDomains {
				nodePool.Spec.Template.Labels = map[string]string{
					fmt.Sprintf("%s/key", label): "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow subdomain labels in restricted domains exceptions list", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range K8sLabelDomains {
				nodePool.Spec.Template.Labels = map[string]string{
					fmt.Sprintf("subdomain.%s", label): "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
		It("should allow subdomain labels prefixed with the kubernetes domain", func() {
			oldNodePool := nodePool.DeepCopy()
			for label := range K8sLabelDomains {
				nodePool.Spec.Template.Labels = map[string]string{
					fmt.Sprintf("subdomain.%s/key", label): "test-value",
				}
				Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
				Expect(env.Client.Delete(ctx, nodePool)).To(Succeed())
				Expect(nodePool.RuntimeValidate()).To(Succeed())
				nodePool = oldNodePool.DeepCopy()
			}
		})
	})
	Context("TerminationGracePeriod", func() {
		It("should succeed on a positive terminationGracePeriod duration", func() {
			nodePool.Spec.Template.Spec.TerminationGracePeriod = &metav1.Duration{Duration: time.Second * 300}
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
		})
		It("should fail on a negative terminationGracePeriod duration", func() {
			nodePool.Spec.Template.Spec.TerminationGracePeriod = &metav1.Duration{Duration: time.Second * -30}
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
	})
	Context("NodeClassRef", func() {
		It("should fail to mutate group", func() {
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			nodePool.Spec.Template.Spec.NodeClassRef.Group = "karpenter.test.mutated.sh"
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail to mutate kind", func() {
			Expect(env.Client.Create(ctx, nodePool)).To(Succeed())
			nodePool.Spec.Template.Spec.NodeClassRef.Group = "TestNodeClass2"
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail if group is unset", func() {
			nodePool.Spec.Template.Spec.NodeClassRef.Group = ""
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail if kind is unset", func() {
			nodePool.Spec.Template.Spec.NodeClassRef.Kind = ""
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
		It("should fail if name is unset", func() {
			nodePool.Spec.Template.Spec.NodeClassRef.Name = ""
			Expect(env.Client.Create(ctx, nodePool)).ToNot(Succeed())
		})
	})
})
