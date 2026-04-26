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

// Package runner is a thin shim that delegates to the shared EPP runner in nokube mode.
// All nokube logic lives in cmd/epp/runner; this package exists only to keep the
// cmd/epp-nokube entrypoint isolated from cmd/epp/main.go.
package runner

import (
	"context"

	epprunner "github.com/llm-d/llm-d-inference-scheduler/cmd/epp/runner"
)

// Run starts the EPP in nokube mode using the shared runner.
func Run(ctx context.Context) error {
	return epprunner.NewRunner().
		WithNokubeMode().
		WithExecutableName("epp-nokube").
		Run(ctx)
}
