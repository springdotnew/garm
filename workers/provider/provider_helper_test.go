// Copyright 2026 Cloudbase Solutions SRL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package provider

import (
	"context"
	"testing"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	dbMocks "github.com/cloudbase/garm/database/common/mocks"
	"github.com/cloudbase/garm/params"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUpdateArgsFromProviderInstancePreservesDeletionIntent(t *testing.T) {
	store := dbMocks.NewStore(t)
	worker := &Provider{
		ctx:   context.Background(),
		store: store,
	}

	providerInstance := commonParams.ProviderInstance{
		ProviderID: "zone/runner-name",
		OSName:     "Ubuntu",
		OSVersion:  "24.04",
		Status:     commonParams.InstanceRunning,
	}

	store.EXPECT().UpdateInstance(mock.Anything, "runner-name", mock.MatchedBy(func(update params.UpdateInstanceParams) bool {
		return update.ProviderID == providerInstance.ProviderID && update.Status == commonParams.InstanceRunning
	})).Return(params.Instance{}, runnerErrors.NewInstanceTransitionError(
		commonParams.InstancePendingDelete,
		commonParams.InstanceRunning,
	)).Once()

	want := params.Instance{
		Name:       "runner-name",
		ProviderID: providerInstance.ProviderID,
		Status:     commonParams.InstancePendingDelete,
	}
	store.EXPECT().UpdateInstance(mock.Anything, "runner-name", mock.MatchedBy(func(update params.UpdateInstanceParams) bool {
		return update.ProviderID == providerInstance.ProviderID && update.Status == ""
	})).Return(want, nil).Once()

	got, err := worker.updateArgsFromProviderInstance("runner-name", providerInstance)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
