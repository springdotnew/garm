// Copyright 2026 Cloudbase Solutions SRL
//
//	Licensed under the Apache License, Version 2.0 (the "License");
//	you may not use this file except in compliance with the License.
//	You may obtain a copy of the License at
//
//		http://www.apache.org/licenses/LICENSE-2.0
//
//	Unless required by applicable law or agreed to in writing, software
//	distributed under the License is distributed on an "AS IS" BASIS,
//	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//	See the License for the specific language governing permissions and
//	limitations under the License.
package runner

import "testing"

func TestKnownBootstrapStage(t *testing.T) {
	tests := map[string]string{
		"using cached runner found in /home/runner/actions-runner": "cached-runner",
		"configuring runner":                                       "configuring",
		"downloading JIT credentials":                              "jit-credentials",
		"generating systemd unit file":                             "systemd-unit",
		"enabling runner service":                                  "systemd-enable",
		"runner service started":                                   "service-started",
		"runner listener connect retry":                            "listener-connect-retry",
		"runner listener ready":                                    "listener-ready",
		"runner listener readiness timeout":                        "listener-ready-timeout",
		"runner successfully installed":                            "installed",
		"failed to configure runner: secret-bearing provider text": "",
	}

	for message, expected := range tests {
		t.Run(message, func(t *testing.T) {
			if actual := knownBootstrapStage(message); actual != expected {
				t.Fatalf("knownBootstrapStage() = %q, want %q", actual, expected)
			}
		})
	}
}
