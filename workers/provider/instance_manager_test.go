// Copyright 2026 Cloudbase Solutions SRL
//
//	Licensed under the Apache License, Version 2.0 (the "License"); you may
//	not use this file except in compliance with the License. You may obtain
//	a copy of the License at
//
//	     http://www.apache.org/licenses/LICENSE-2.0
//
//	Unless required by applicable law or agreed to in writing, software
//	distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//	WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//	License for the specific language governing permissions and limitations
//	under the License.
package provider

import (
	"context"
	"testing"
	"time"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/auth"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
	runnerCommon "github.com/cloudbase/garm/runner/common"
	runnerCommonMocks "github.com/cloudbase/garm/runner/common/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type testProviderHelper struct{}

func (testProviderHelper) SetInstanceStatus(string, commonParams.InstanceStatus, []byte, bool) error {
	return nil
}

func (testProviderHelper) InstanceTokenGetter() auth.InstanceTokenGetter {
	return nil
}

func (testProviderHelper) updateArgsFromProviderInstance(string, commonParams.ProviderInstance) (params.Instance, error) {
	return params.Instance{}, nil
}

func (testProviderHelper) GetControllerInfo() (params.ControllerInfo, error) {
	return params.ControllerInfo{}, nil
}

func (testProviderHelper) GetGithubEntity(entity params.ForgeEntity) (params.ForgeEntity, error) {
	return entity, nil
}

func TestInstanceUpdatesDoNotWaitForProviderCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	deleteStarted := make(chan struct{})
	releaseDelete := make(chan struct{})
	providerMock := runnerCommonMocks.NewProvider(t)
	providerMock.EXPECT().DeleteInstance(mock.Anything, "provider-id", mock.Anything).
		Run(func(context.Context, string, runnerCommon.DeleteInstanceParams) {
			close(deleteStarted)
			<-releaseDelete
		}).
		Return(nil).
		Once()

	manager := &instanceManager{
		ctx: ctx,
		instance: params.Instance{
			Name:       "runner-name",
			ProviderID: "provider-id",
			Status:     commonParams.InstancePendingDelete,
		},
		provider: providerMock,
		helper:   testProviderHelper{},
	}
	require.NoError(t, manager.Start())
	t.Cleanup(func() {
		require.NoError(t, manager.Stop())
	})

	consolidated := make(chan error, 1)
	go func() {
		consolidated <- manager.consolidateState()
	}()
	<-deleteStarted

	update := dbCommon.ChangePayload{
		EntityType: dbCommon.InstanceEntityType,
		Operation:  dbCommon.UpdateOperation,
		Payload: params.Instance{
			Name:   "runner-name",
			Status: commonParams.InstancePendingDelete,
		},
	}
	require.NoError(t, manager.Update(update))

	secondUpdate := make(chan error, 1)
	go func() {
		secondUpdate <- manager.Update(update)
	}()

	select {
	case err := <-secondUpdate:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(releaseDelete)
		require.FailNow(t, "instance update blocked behind provider call")
	}

	close(releaseDelete)
	require.ErrorIs(t, <-consolidated, ErrInstanceDeleted)
}
