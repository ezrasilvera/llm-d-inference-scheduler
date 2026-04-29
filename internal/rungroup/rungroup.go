/*
Copyright 2025 The Kubernetes Authors.

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

// Package rungroup provides a RunnableGroup abstraction for starting a set of
// named goroutines together and propagating the first failure to all of them.
// Two implementations are provided: ErrGroupRunner for standalone (no-kube) mode
// and ManagerRunner for Kubernetes mode where a ctrl.Manager owns the lifecycle.
package rungroup

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/llm-d/llm-d-inference-scheduler/internal/runnable"
)

// RunnableGroup collects named runnables and starts them together.
// If any runnable returns a non-nil error, the shared context is cancelled
// and Run returns that error (wrapped with the runnable name).
type RunnableGroup interface {
	Add(name string, fn func(ctx context.Context) error)
	Run(ctx context.Context) error
}

// NewErrGroupRunner returns a RunnableGroup backed by errgroup.
// Use this in no-kube mode where no ctrl.Manager is available.
func NewErrGroupRunner() RunnableGroup {
	return &errGroupRunner{}
}

type namedFn struct {
	name string
	fn   func(ctx context.Context) error
}

type errGroupRunner struct {
	fns []namedFn
}

func (e *errGroupRunner) Add(name string, fn func(ctx context.Context) error) {
	e.fns = append(e.fns, namedFn{name: name, fn: fn})
}

func (e *errGroupRunner) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, nf := range e.fns {
		nf := nf
		g.Go(func() error {
			if err := nf.fn(gctx); err != nil {
				return fmt.Errorf("%s: %w", nf.name, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// NewManagerRunner returns a RunnableGroup backed by a ctrl.Manager.
// Runnables added via Add are registered with the manager using NoLeaderElection.
// Run calls mgr.Start(ctx), which starts all registered runnables.
// Use this in Kubernetes mode.
func NewManagerRunner(mgr ctrl.Manager) RunnableGroup {
	return &managerRunner{mgr: mgr}
}

type managerRunner struct {
	mgr ctrl.Manager
	err error
}

func (m *managerRunner) Add(name string, fn func(ctx context.Context) error) {
	if m.err != nil {
		return
	}
	wrapped := manager.RunnableFunc(func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	})
	m.err = m.mgr.Add(runnable.NoLeaderElection(wrapped))
}

func (m *managerRunner) Run(ctx context.Context) error {
	if m.err != nil {
		return m.err
	}
	return m.mgr.Start(ctx)
}
