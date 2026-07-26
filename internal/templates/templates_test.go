// Copyright 2026 Cloudbase Solutions SRL
//
//    Licensed under the Apache License, Version 2.0 (the "License"); you may
//    not use this file except in compliance with the License. You may obtain
//    a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//    License for the specific language governing permissions and limitations
//    under the License.

package templates

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/params"
)

// TestInstallTemplatesRenderForceInsecure renders every default install
// template and checks that the agent config only carries force_insecure when
// the controller allows insecure agent connections. Agents older than v0.1.1
// do not know the setting, so it must be absent unless explicitly enabled.
// TestWrapperRendersProxySettings renders the install wrappers with and
// without a proxy config and checks that proxy environment variables only
// show up when a proxy is set.
func TestWrapperRendersProxySettings(t *testing.T) {
	ctx := context.Background()
	proxyCfg := commonParams.ProxyConfig{
		HTTPProxy:  "http://user:pass@proxy.example.com:3128",
		HTTPSProxy: "http://user:pass@proxy.example.com:3128",
		NoProxy:    "localhost,.internal.example.com,10.0.0.0/8",
	}

	for _, osType := range []commonParams.OSType{commonParams.Linux, commonParams.Windows} {
		t.Run(string(osType), func(t *testing.T) {
			rendered, err := RenderRunnerInstallWrapper(ctx, osType, "https://garm.example.com/api/v1/metadata", "https://garm.example.com/api/v1/callbacks", "token", commonParams.ProxyConfig{})
			if err != nil {
				t.Fatalf("failed to render wrapper: %v", err)
			}
			for _, needle := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "DefaultWebProxy"} {
				if strings.Contains(string(rendered), needle) {
					t.Errorf("expected %q to be absent when no proxy is set", needle)
				}
			}
			if osType == commonParams.Linux && strings.Contains(string(rendered), "set -x") {
				t.Error("linux wrapper must not trace the callback bearer token")
			}

			rendered, err = RenderRunnerInstallWrapper(ctx, osType, "https://garm.example.com/api/v1/metadata", "https://garm.example.com/api/v1/callbacks", "token", proxyCfg)
			if err != nil {
				t.Fatalf("failed to render wrapper: %v", err)
			}
			for _, needle := range []string{proxyCfg.HTTPProxy, proxyCfg.NoProxy} {
				if !strings.Contains(string(rendered), needle) {
					t.Errorf("expected %q in the rendered wrapper", needle)
				}
			}
			if osType == commonParams.Windows {
				for _, needle := range []string{"DefaultWebProxy", `[Environment]::SetEnvironmentVariable("HTTPS_PROXY", $env:HTTPS_PROXY, "Machine")`} {
					if !strings.Contains(string(rendered), needle) {
						t.Errorf("expected %q in the rendered windows wrapper", needle)
					}
				}
			} else if !strings.Contains(string(rendered), `export https_proxy='`+proxyCfg.HTTPSProxy+`'`) {
				t.Error("expected lowercase https_proxy export in the linux wrapper")
			}
		})
	}
}

// TestWindowsInstallTemplatesHaveProxyPreamble checks that the windows
// install scripts configure the .NET default web proxy from the proxy
// environment variables inherited from the wrapper. PowerShell 5.1 does not
// honor proxy environment variables on its own.
func TestWindowsInstallTemplatesHaveProxyPreamble(t *testing.T) {
	for _, forge := range []params.EndpointType{params.GithubEndpointType, params.GiteaEndpointType} {
		content, err := GetTemplateContent(commonParams.Windows, forge)
		if err != nil {
			t.Fatalf("failed to get template content: %v", err)
		}
		if !strings.Contains(string(content), "[System.Net.WebRequest]::DefaultWebProxy = $defaultProxy") {
			t.Errorf("expected windows %s install template to set the default web proxy", forge)
		}
	}
}

// TestInstallTemplatesRenderAgentProxy renders every default install
// template and checks that the garm-agent proxy section is only emitted when
// a proxy is set. Agents older than v0.1.1 do not know the proxy section, so
// it must be absent otherwise.
func TestInstallTemplatesRenderAgentProxy(t *testing.T) {
	for _, forge := range []params.EndpointType{params.GithubEndpointType, params.GiteaEndpointType} {
		for _, osType := range []commonParams.OSType{commonParams.Linux, commonParams.Windows} {
			t.Run(fmt.Sprintf("%s_%s", forge, osType), func(t *testing.T) {
				content, err := GetTemplateContent(osType, forge)
				if err != nil {
					t.Fatalf("failed to get template content: %v", err)
				}

				tplCtx := InstallContext{
					AgentMode:  true,
					AgentURL:   "wss://garm.example.com/agent",
					AgentToken: "secret",
					AgentShell: "false",
				}

				rendered, err := RenderRunnerInstallScript(string(content), tplCtx)
				if err != nil {
					t.Fatalf("failed to render template: %v", err)
				}
				if strings.Contains(string(rendered), "[proxy]") {
					t.Error("expected proxy section to be absent when no proxy is set")
				}

				tplCtx.HTTPSProxy = "http://user:pass@proxy.example.com:3128"
				tplCtx.NoProxy = "localhost,.internal.example.com"
				rendered, err = RenderRunnerInstallScript(string(content), tplCtx)
				if err != nil {
					t.Fatalf("failed to render template: %v", err)
				}
				needles := []string{
					"[proxy]",
					`https_proxy = "http://user:pass@proxy.example.com:3128"`,
					`no_proxy = "localhost,.internal.example.com"`,
				}
				if forge == params.GithubEndpointType {
					// The github runner reads proxy settings from the
					// .env file in the runner install dir.
					needles = append(needles, "https_proxy=http://user:pass@proxy.example.com:3128")
				}
				for _, needle := range needles {
					if !strings.Contains(string(rendered), needle) {
						t.Errorf("expected %q in the rendered template", needle)
					}
				}
			})
		}
	}
}

func TestInstallTemplatesRenderForceInsecure(t *testing.T) {
	for _, forge := range []params.EndpointType{params.GithubEndpointType, params.GiteaEndpointType} {
		for _, osType := range []commonParams.OSType{commonParams.Linux, commonParams.Windows} {
			t.Run(fmt.Sprintf("%s_%s", forge, osType), func(t *testing.T) {
				content, err := GetTemplateContent(osType, forge)
				if err != nil {
					t.Fatalf("failed to get template content: %v", err)
				}

				tplCtx := InstallContext{
					AgentMode:  true,
					AgentURL:   "wss://garm.example.com/agent",
					AgentToken: "secret",
					AgentShell: "false",
				}

				rendered, err := RenderRunnerInstallScript(string(content), tplCtx)
				if err != nil {
					t.Fatalf("failed to render template: %v", err)
				}
				if strings.Contains(string(rendered), "force_insecure") {
					t.Error("expected force_insecure to be absent when not allowed")
				}

				tplCtx.ForceInsecureGARMAgent = true
				rendered, err = RenderRunnerInstallScript(string(content), tplCtx)
				if err != nil {
					t.Fatalf("failed to render template: %v", err)
				}
				if !strings.Contains(string(rendered), "force_insecure = true") {
					t.Error("expected force_insecure = true in the agent config")
				}
				// The neighboring config keys must survive the conditional.
				if !strings.Contains(string(rendered), "enable_shell = ") {
					t.Error("expected enable_shell to remain in the agent config")
				}
			})
		}
	}
}

func TestLinuxInstallWrapperSerializesBootstrapWithoutTracingToken(t *testing.T) {
	t.Parallel()

	wrapper, err := RenderRunnerInstallWrapper(
		context.Background(),
		commonParams.Linux,
		"https://controller.example/metadata",
		"https://controller.example/callbacks",
		"sensitive-token",
		commonParams.ProxyConfig{},
	)
	if err != nil {
		t.Fatalf("failed to render wrapper: %v", err)
	}

	rendered := string(wrapper)
	for _, needle := range []string{
		`mkdir "$LOCK_DIR"`,
		`if [[ -e "$DONE_FILE" ]]`,
		`touch "$DONE_FILE"`,
	} {
		if !strings.Contains(rendered, needle) {
			t.Errorf("expected %q in rendered wrapper", needle)
		}
	}
	for _, needle := range []string{"set -x", "set -ex"} {
		if strings.Contains(rendered, needle) {
			t.Errorf("did not expect %q in rendered wrapper", needle)
		}
	}
	if strings.Contains(rendered, "flock") {
		t.Error("did not expect the generic wrapper to require flock")
	}
	if strings.Index(rendered, `mkdir "$LOCK_DIR"`) >= strings.Index(rendered, `curl -H`) {
		t.Error("expected bootstrap lock before metadata download")
	}
	if strings.Index(rendered, `/tmp/real-install.sh`) >= strings.Index(rendered, `touch "$DONE_FILE"`) {
		t.Error("expected completion marker after the install script")
	}
}

func TestLinuxInstallWrapperRunsConcurrentBootstrapOnce(t *testing.T) {
	t.Parallel()

	wrapper, err := RenderRunnerInstallWrapper(
		context.Background(),
		commonParams.Linux,
		"https://controller.example/metadata",
		"https://controller.example/callbacks",
		"sensitive-token",
		commonParams.ProxyConfig{},
	)
	if err != nil {
		t.Fatalf("failed to render wrapper: %v", err)
	}

	tempDir := t.TempDir()
	binDir := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatalf("failed to create bin directory: %v", err)
	}
	countPath := filepath.Join(tempDir, "install-count")
	fakeCurl := filepath.Join(binDir, "curl")
	fakeCurlScript := `#!/bin/bash
set -e
output=
while (( $# > 0 )); do
	if [[ "$1" == "-o" ]]; then
		output="$2"
		shift 2
		continue
	fi
	shift
done
cat > "$output" <<'EOF'
#!/bin/bash
printf 'run\n' >> "$RUN_COUNT_FILE"
sleep 0.2
EOF
printf 200
`
	if err := os.WriteFile(fakeCurl, []byte(fakeCurlScript), 0o755); err != nil {
		t.Fatalf("failed to write fake curl: %v", err)
	}

	rendered := strings.NewReplacer(
		"/tmp/garm-runner-bootstrap.lock", filepath.Join(tempDir, "bootstrap.lock"),
		"/tmp/garm-runner-bootstrap.done", filepath.Join(tempDir, "bootstrap.done"),
		"/tmp/real-install.sh", filepath.Join(tempDir, "install.sh"),
	).Replace(string(wrapper))
	wrapperPath := filepath.Join(tempDir, "wrapper.sh")
	if err := os.WriteFile(wrapperPath, []byte(rendered), 0o755); err != nil {
		t.Fatalf("failed to write wrapper: %v", err)
	}

	run := func() error {
		cmd := exec.CommandContext(context.Background(), "bash", wrapperPath)
		cmd.Env = append(os.Environ(),
			"PATH="+binDir+":"+os.Getenv("PATH"),
			"RUN_COUNT_FILE="+countPath,
		)
		return cmd.Run()
	}
	results := make(chan error, 2)
	go func() { results <- run() }()
	go func() { results <- run() }()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("wrapper failed: %v", err)
		}
	}

	count, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("failed to read install count: %v", err)
	}
	if got := strings.Count(string(count), "run\n"); got != 1 {
		t.Fatalf("expected one install execution, got %d", got)
	}
}
