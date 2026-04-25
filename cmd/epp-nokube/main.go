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

// Package main contains the nokube (non-Kubernetes) Endpoint Picker (EPP).
// It discovers backends via a configured BackendDiscovery plugin instead of
// watching Kubernetes pods.
package main

import (
	"os"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/llm-d/llm-d-inference-scheduler/cmd/epp-nokube/runner"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx := ctrl.SetupSignalHandler()
	if err := runner.Run(ctx); err != nil {
		ctrl.Log.Error(err, "nokube EPP exited with error")
		return 1
	}
	return 0
}
