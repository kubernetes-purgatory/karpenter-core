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

package operator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/zapr"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"
	"knative.dev/pkg/configmap/informer"
	knativeinjection "knative.dev/pkg/injection"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"
	"knative.dev/pkg/system"
	"knative.dev/pkg/webhook"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/aws/karpenter-core/pkg/apis"
	"github.com/aws/karpenter-core/pkg/events"
	corecontroller "github.com/aws/karpenter-core/pkg/operator/controller"
	"github.com/aws/karpenter-core/pkg/operator/injection"
	"github.com/aws/karpenter-core/pkg/operator/options"
	"github.com/aws/karpenter-core/pkg/operator/scheme"
	"github.com/aws/karpenter-core/pkg/operator/settingsstore"
)

const (
	appName   = "karpenter"
	component = "controller"
)

type Operator struct {
	manager.Manager

	RESTConfig          *rest.Config
	KubernetesInterface kubernetes.Interface
	SettingsStore       settingsstore.Store
	EventRecorder       events.Recorder
	Clock               clock.Clock

	webhooks []knativeinjection.ControllerConstructor
}

// NewOperator instantiates a controller manager or panics
func NewOperator() (context.Context, *Operator) {
	// Root Context
	ctx := signals.NewContext()
	ctx = knativeinjection.WithNamespaceScope(ctx, system.Namespace())
	// TODO: This can be removed if we eventually decide that we need leader election. Having leader election has resulted in the webhook
	// having issues described in https://github.com/aws/karpenter/issues/2562 so these issues need to be resolved if this line is removed
	ctx = sharedmain.WithHADisabled(ctx) // Disable leader election for webhook

	// Options
	opts := options.New().MustParse()
	ctx = injection.WithOptions(ctx, *opts)

	// Webhook
	ctx = webhook.WithOptions(ctx, webhook.Options{
		Port:        opts.WebhookPort,
		ServiceName: opts.ServiceName,
		SecretName:  fmt.Sprintf("%s-cert", opts.ServiceName),
		GracePeriod: 5 * time.Second,
	})

	// Client Config
	config := controllerruntime.GetConfigOrDie()
	config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(float32(opts.KubeClientQPS), opts.KubeClientBurst)
	config.UserAgent = appName

	// Client
	kubernetesInterface := kubernetes.NewForConfigOrDie(config)
	configMapWatcher := informer.NewInformedWatcher(kubernetesInterface, system.Namespace())

	// Logging
	logger := NewLogger(ctx, component, config, configMapWatcher)
	ctx = logging.WithLogger(ctx, logger)

	// Create the settingsStore for settings injection
	settingsStore := settingsstore.NewWatcherOrDie(ctx, kubernetesInterface, configMapWatcher, apis.Settings.List()...)

	// Inject settings after starting the ConfigMapWatcher
	lo.Must0(configMapWatcher.Start(ctx.Done()))
	ctx = settingsStore.InjectSettings(ctx)

	// Manager
	manager, err := controllerruntime.NewManager(config, controllerruntime.Options{
		Logger:                     ignoreDebugEvents(zapr.NewLogger(logger.Desugar())),
		LeaderElection:             opts.EnableLeaderElection,
		LeaderElectionID:           "karpenter-leader-election",
		LeaderElectionResourceLock: resourcelock.LeasesResourceLock,
		Scheme:                     scheme.Scheme,
		MetricsBindAddress:         fmt.Sprintf(":%d", opts.MetricsPort),
		HealthProbeBindAddress:     fmt.Sprintf(":%d", opts.HealthProbePort),
		BaseContext: func() context.Context {
			ctx := context.Background()
			ctx = logging.WithLogger(ctx, logger)
			ctx = injection.WithConfig(ctx, config)
			ctx = injection.WithOptions(ctx, *opts)
			return ctx
		},
	})
	manager = lo.Must(manager, err, "failed to setup manager")
	if opts.EnableProfiling {
		registerPprof(manager)
	}
	lo.Must0(manager.GetFieldIndexer().IndexField(ctx, &v1.Pod{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*v1.Pod).Spec.NodeName}
	}), "failed to setup pod indexer")

	return ctx, &Operator{
		Manager:             manager,
		RESTConfig:          config,
		KubernetesInterface: kubernetesInterface,
		SettingsStore:       settingsStore,
		EventRecorder:       events.NewRecorder(manager.GetEventRecorderFor(appName)),
		Clock:               clock.RealClock{},
	}
}

func (o *Operator) WithControllers(ctx context.Context, controllers ...corecontroller.Controller) *Operator {
	for _, c := range controllers {
		// Wrap the controllers with any decorators
		c = corecontroller.InjectSettings(c, o.SettingsStore)

		lo.Must0(c.Builder(ctx, o.Manager).Complete(c), "failed to register controller")
	}
	lo.Must0(o.AddHealthzCheck("healthz", healthz.Ping), "failed to setup liveness probe")
	lo.Must0(o.AddReadyzCheck("readyz", healthz.Ping), "failed to setup readiness probe")
	return o
}

func (o *Operator) WithWebhooks(webhooks ...knativeinjection.ControllerConstructor) *Operator {
	o.webhooks = append(o.webhooks, webhooks...)
	return o
}

func (o *Operator) Start(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		lo.Must0(o.Manager.Start(ctx))
	}()
	if !injection.GetOptions(ctx).DisableWebhook {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sharedmain.MainWithConfig(ctx, "webhook", o.GetConfig(), o.webhooks...)
		}()
	}
	wg.Wait()
}
