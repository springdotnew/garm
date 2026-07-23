// SPDX-License-Identifier: Apache-2.0

package templates

import (
	"context"
	"strings"
	"testing"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/stretchr/testify/require"
)

func TestLinuxInstallWrapperSerializesBootstrapWithoutTracingToken(t *testing.T) {
	t.Parallel()

	wrapper, err := RenderRunnerInstallWrapper(
		context.Background(),
		commonParams.Linux,
		"https://controller.example/metadata",
		"sensitive-token",
	)
	require.NoError(t, err)

	rendered := string(wrapper)
	require.Contains(t, rendered, `flock 9`)
	require.Contains(t, rendered, `if [[ -e "$DONE_FILE" ]]`)
	require.Contains(t, rendered, `touch "$DONE_FILE"`)
	require.NotContains(t, rendered, "set -x")
	require.NotContains(t, rendered, "set -ex")
	require.True(t, strings.Index(rendered, `flock 9`) < strings.Index(rendered, `curl -H`))
	require.True(t, strings.Index(rendered, `/tmp/real-install.sh`) < strings.Index(rendered, `touch "$DONE_FILE"`))
}
