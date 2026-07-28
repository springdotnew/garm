// Copyright 2025 Cloudbase Solutions SRL
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
package scaleset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm-provider-common/util"
	"github.com/cloudbase/garm/cache"
	"github.com/cloudbase/garm/config"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/database/watcher"
	garmErrors "github.com/cloudbase/garm/internal/errors"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/params"
	"github.com/cloudbase/garm/runner/common"
	garmUtil "github.com/cloudbase/garm/util"
	"github.com/cloudbase/garm/util/github/scalesets"
)

const (
	maxConcurrentRunnerCreations = 32
	scaleDownIdleGrace           = 30 * time.Second
)

func NewWorker(ctx context.Context, store dbCommon.Store, scaleSet params.ScaleSet, provider common.Provider, prewarm config.Prewarm) (*Worker, error) {
	consumerID := fmt.Sprintf("scaleset-worker-%s-%d", scaleSet.Name, scaleSet.ID)
	controllerInfo, err := store.ControllerInfo()
	if err != nil {
		return nil, fmt.Errorf("getting controller info: %w", err)
	}
	ctx = garmUtil.WithSlogContext(
		ctx,
		slog.Any("worker", consumerID),
	)
	scalesetEntity, err := scaleSet.GetEntity()
	if err != nil {
		return nil, fmt.Errorf("failed to get scaleset entity: %w", err)
	}
	entity, err := store.GetForgeEntity(ctx, scalesetEntity.EntityType, scalesetEntity.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get entity from the db: %w", err)
	}
	return &Worker{
		ctx:             ctx,
		controllerInfo:  controllerInfo,
		consumerID:      consumerID,
		store:           store,
		provider:        provider,
		scaleSet:        scaleSet,
		entity:          entity,
		prewarm:         prewarm,
		runners:         make(map[string]params.Instance),
		runnerSnapshots: make(map[string]time.Time),
		autoScaleWake:   make(chan struct{}, 1),
	}, nil
}

type Worker struct {
	ctx            context.Context
	consumerID     string
	controllerInfo params.ControllerInfo

	provider common.Provider
	store    dbCommon.Store
	scaleSet params.ScaleSet
	entity   params.ForgeEntity
	prewarm  config.Prewarm
	runners  map[string]params.Instance
	// runnerSnapshots records the newest database timestamp observed for each
	// runner, including runners removed from the cache. Instance watcher
	// handlers run concurrently, so the cache needs its own ordering guard.
	runnerSnapshots map[string]time.Time

	consumer dbCommon.Consumer

	listener          *scaleSetListener
	autoScaleWake     chan struct{}
	creationsInFlight int
	// speculativeRunners is the prewarm forecast this scale set is currently
	// holding capacity for. It is refreshed once per autoscale pass so that a
	// target stays stable for the whole of a critical section.
	speculativeRunners int
	scaleSetClient     *scalesets.ScaleSetClient
	scaleSetClientGen  uint64

	mux               sync.Mutex
	scaleSetClientMux sync.Mutex
	running           bool
	quit              chan struct{}
}

func (w *Worker) ensureScaleSetInGitHub() error {
	entity, err := w.scaleSet.GetEntity()
	if err != nil {
		return fmt.Errorf("failed to get entity: %w", err)
	}
	cli, err := w.GetScaleSetClient()
	if err != nil {
		return fmt.Errorf("failed to get scaleset client: %w", err)
	}

	ghCli, err := cli.GetGithubClient()
	if err != nil {
		return fmt.Errorf("failed to get github client: %w", err)
	}

	rgID, err := ghCli.GetEntityRunnerGroupIDByName(w.ctx, w.scaleSet.GitHubRunnerGroup)
	if err != nil {
		return fmt.Errorf("failed to get github runner group for entity %s: %w", entity.ID, err)
	}
	scaleSet, err := cli.GetRunnerScaleSetByNameAndRunnerGroup(w.ctx, int(rgID), w.scaleSet.Name)
	if err == nil {
		// The scale set exists
		if scaleSet.ID != w.scaleSet.ScaleSetID {
			// The scale set exists in github, but the ID differs from what we know to be true.
			// It is possible that the scale set is being managed by some other auto scaler.
			// We error here, as there is no way to listen on a scale set that already has a listener
			// or is being managed by something else.
			return fmt.Errorf("scale set already exists in github and it differs from the ID we know (github: %d vs local: %d)", scaleSet.ID, w.scaleSet.ScaleSetID)
		}
		return nil
	}
	if !errors.Is(err, runnerErrors.ErrNotFound) {
		return fmt.Errorf("failed to get scale set: %w", err)
	}

	labels := w.scaleSet.GitHubLabels()

	createScaleSetParams := &params.RunnerScaleSet{
		Name:          w.scaleSet.Name,
		RunnerGroupID: rgID,
		Labels:        labels,
		RunnerSetting: params.RunnerSetting{
			Ephemeral:     true,
			DisableUpdate: w.scaleSet.DisableUpdate,
		},
		Enabled: &w.scaleSet.Enabled,
	}
	runnerScaleSet, err := cli.CreateRunnerScaleSet(w.ctx, createScaleSetParams)
	if err != nil {
		return fmt.Errorf("error creating runner scale set: %w", err)
	}

	// update the DB scale set
	updateParams := params.UpdateScaleSetParams{
		ScaleSetID: runnerScaleSet.ID,
	}
	_, err = w.store.UpdateEntityScaleSet(w.ctx, entity, w.scaleSet.ID, updateParams, nil)
	if err != nil {
		return fmt.Errorf("failed to update scale set: %w", err)
	}

	// The scale set was recreated. We need to reset the last message ID we recorded previously.
	if err := w.SetLastMessageID(0); err != nil {
		return fmt.Errorf("failed to reset last message id: %w", err)
	}
	w.scaleSet.ScaleSetID = runnerScaleSet.ID

	return nil
}

func (w *Worker) Stop() error {
	slog.DebugContext(w.ctx, "stopping scale set worker", "scale_set", w.consumerID)
	w.mux.Lock()
	defer w.mux.Unlock()

	if !w.running {
		return nil
	}

	w.consumer.Close()
	w.running = false
	if w.quit != nil {
		close(w.quit)
	}
	w.listener.Stop()
	return nil
}

func (w *Worker) IsRunning() bool {
	w.mux.Lock()
	defer w.mux.Unlock()

	return w.running
}

func (w *Worker) Start() (err error) {
	slog.DebugContext(w.ctx, "starting scale set worker", "scale_set", w.consumerID)
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.running {
		return nil
	}

	instances, err := w.store.ListScaleSetInstances(w.ctx, w.scaleSet.ID, false)
	if err != nil {
		return fmt.Errorf("listing scale set instances: %w", err)
	}

	for _, instance := range instances {
		switch instance.Status {
		case commonParams.InstanceCreating:
			// We're just starting up. We found an instance stuck in creating.
			// When a provider creates an instance, it sets the db instance to
			// creating and then issues an API call to the IaaS to create the
			// instance using some userdata it needs to come up. But the instance
			// will still need to call back home to fetch aditional metadata and
			// complete its setup. We should remove the instance as it is not
			// possible to reliably determine the state of the instance (if it's in
			// mid boot before it reached the phase where it runs the metadtata, or
			// if it already failed).
			instanceState := commonParams.InstancePendingDelete
			locking.Lock(instance.Name, w.consumerID)
			if instance.AgentID != 0 {
				scaleSetCli, err := w.GetScaleSetClient()
				if err != nil {
					slog.ErrorContext(w.ctx, "error getting scale set client", "error", err)
					return fmt.Errorf("getting scale set client: %w", err)
				}
				if err := scaleSetCli.RemoveRunner(w.ctx, instance.AgentID); err != nil {
					// scale sets use JIT runners. This means that we create the runner in github
					// before we create the actual instance that will use the credentials. We need
					// to remove the runner from github if it exists.
					if !errors.Is(err, runnerErrors.ErrNotFound) {
						if errors.Is(err, runnerErrors.ErrUnauthorized) {
							// we don't have access to remove the runner. This implies that our
							// credentials may have expired or ar incorrect.
							//
							// nolint:golangci-lint,godox
							// TODO(gabriel-samfira): we need to set the scale set as inactive and stop the listener (if any).
							slog.ErrorContext(w.ctx, "error removing runner", "runner_name", instance.Name, "error", err)
							w.runners[instance.ID] = instance
							locking.Unlock(instance.Name, false)
							continue
						}
						// The runner may have come up, registered and is currently running a
						// job, in which case, github will not allow us to remove it.
						runnerInstance, err := scaleSetCli.GetRunner(w.ctx, instance.AgentID)
						if err != nil {
							if !errors.Is(err, runnerErrors.ErrNotFound) {
								// We could not get info about the runner and it wasn't not found
								slog.ErrorContext(w.ctx, "error getting runner details", "error", err)
								w.runners[instance.ID] = instance
								locking.Unlock(instance.Name, false)
								continue
							}
						}
						if runnerInstance.Status == string(params.RunnerIdle) ||
							runnerInstance.Status == string(params.RunnerActive) {
							// This is a highly unlikely scenario, but let's account for it anyway.
							//
							// The runner is running a job or is idle. Mark it as running, as
							// it appears that it finished booting and is now running.
							//
							// NOTE: if the instance was in creating and it managed to boot, there
							// is a high chance that we do not have a provider ID for the runner
							// inside our database. When removing the runner, the provider will attempt
							// to use the instance name instead of the provider ID, the same as when
							// creation of the instance fails and we try to clean up any lingering resources
							// in the provider.
							slog.DebugContext(w.ctx, "runner is running a job or is idle; not removing", "runner_name", instance.Name)
							instanceState = commonParams.InstanceRunning
						}
					}
				}
			}
			runnerUpdateParams := params.UpdateInstanceParams{
				Status: instanceState,
			}
			instance, err = w.store.ForceUpdateInstance(w.ctx, instance.Name, runnerUpdateParams)
			if err != nil {
				if !errors.Is(err, runnerErrors.ErrNotFound) {
					locking.Unlock(instance.Name, false)
					return fmt.Errorf("updating runner %s: %w", instance.Name, err)
				}
			}
		case commonParams.InstanceDeleting:
			// Set the instance in deleting. It is assumed that the runner was already
			// removed from github either by github or by garm. Deleting status indicates
			// that it was already being handled by the provider. There should be no entry on
			// github for the runner if that was the case.
			// Setting it in pending_delete will cause the provider to try again, an operation
			// which is idempotent (if it's already deleted, the provider reports success).
			runnerUpdateParams := params.UpdateInstanceParams{
				Status: commonParams.InstancePendingDelete,
			}
			instance, err = w.store.ForceUpdateInstance(w.ctx, instance.Name, runnerUpdateParams)
			if err != nil {
				if !errors.Is(err, runnerErrors.ErrNotFound) {
					locking.Unlock(instance.Name, false)
					return fmt.Errorf("updating runner %s: %w", instance.Name, err)
				}
			}
		case commonParams.InstanceDeleted:
			if err := w.handleInstanceCleanup(instance); err != nil {
				locking.Unlock(instance.Name, false)
				return fmt.Errorf("failed to remove database entry for %s: %w", instance.Name, err)
			}
			continue
		}
		w.runners[instance.ID] = instance
		locking.Unlock(instance.Name, false)
	}

	if err := w.ensureScaleSetInGitHub(); err != nil {
		return fmt.Errorf("failed to ensure scale set: %w", err)
	}

	consumer, err := watcher.RegisterConsumer(
		w.ctx, w.consumerID,
		watcher.WithAny(
			watcher.WithAll(
				watcher.WithScaleSetFilter(w.scaleSet),
				watcher.WithOperationTypeFilter(dbCommon.UpdateOperation),
			),
			watcher.WithScaleSetInstanceFilter(w.scaleSet),
		),
	)
	if err != nil {
		return fmt.Errorf("error registering consumer: %w", err)
	}
	defer func() {
		if err != nil {
			consumer.Close()
		}
	}()

	slog.DebugContext(w.ctx, "creating scale set listener")
	listener := newListener(w.ctx, w)

	w.listener = listener
	w.consumer = consumer
	w.running = true
	w.quit = make(chan struct{})

	slog.DebugContext(w.ctx, "starting scale set worker loops", "scale_set", w.consumerID)
	go w.loop()
	go w.keepListenerAlive()
	go w.handleAutoScale()
	return nil
}

func (w *Worker) runnerByName() map[string]params.Instance {
	runners := make(map[string]params.Instance)
	for _, runner := range w.runners {
		runners[runner.Name] = runner
	}
	return runners
}

func (w *Worker) setRunnerDBStatus(runner string, status commonParams.InstanceStatus) (params.Instance, error) {
	updateParams := params.UpdateInstanceParams{
		Status: status,
	}
	newDbInstance, err := w.store.UpdateInstance(w.ctx, runner, updateParams)
	if err != nil {
		return params.Instance{}, fmt.Errorf("updating runner %s: %w", runner, err)
	}
	return newDbInstance, nil
}

func (w *Worker) removeRunnerFromGithubAndSetPendingDelete(runnerName string, agentID int64) error {
	scaleSetCli, err := w.GetScaleSetClient()
	if err != nil {
		return fmt.Errorf("getting scale set client: %w", err)
	}
	if err := scaleSetCli.RemoveRunner(w.ctx, agentID); err != nil {
		if !errors.Is(err, runnerErrors.ErrNotFound) {
			return fmt.Errorf("removing runner %s: %w", runnerName, err)
		}
	}
	instance, err := w.setRunnerDBStatus(runnerName, commonParams.InstancePendingDelete)
	if err != nil {
		if errors.Is(err, runnerErrors.ErrNotFound) {
			// The runner record is already gone from the database; there is
			// nothing left to mark. The watcher delete event will evict it
			// from the local cache.
			return nil
		}
		return fmt.Errorf("updating runner %s: %w", runnerName, err)
	}
	w.runners[instance.ID] = instance
	return nil
}

func (w *Worker) reapTimedOutRunners(runners map[string]params.RunnerReference) (func(), error) {
	lockNames := []string{}

	unlockFn := func() {
		for _, name := range lockNames {
			slog.DebugContext(w.ctx, "unlockFn unlocking runner", "runner_name", name)
			locking.Unlock(name, false)
		}
	}

	for _, runner := range w.runners {
		if time.Since(runner.CreatedAt).Minutes() < float64(w.scaleSet.RunnerTimeout()) {
			continue
		}
		switch runner.Status {
		case commonParams.InstancePendingDelete, commonParams.InstancePendingForceDelete,
			commonParams.InstanceDeleting, commonParams.InstanceDeleted:
			continue
		case commonParams.InstanceCreating, commonParams.InstancePendingCreate:
			// Instance is still being created in the provider, or is about to be
			// created. The provider worker owns create timeouts/failures: the
			// create call carries a deadline derived from the bootstrap timeout
			// (and optionally the provider exec timeout), so an instance cannot
			// linger here past it; on failure or expiry the provider worker moves
			// it through Error -> PendingDelete on its own. Nothing for the
			// scale-set-level reaper to do; jumping straight to pending_delete
			// from here is also not a valid status transition (creating only
			// allows -> error or -> running).
			continue
		}

		if runner.RunnerStatus != params.RunnerPending && runner.RunnerStatus != params.RunnerInstalling && runner.RunnerStatus != params.RunnerFailed {
			slog.DebugContext(w.ctx, "runner is not pending, installing or failed; skipping", "runner_name", runner.Name)
			continue
		}
		if ghRunner, ok := runners[runner.Name]; !ok || ghRunner.GetStatus() == params.RunnerOffline {
			if ok := locking.TryLock(runner.Name, w.consumerID); !ok {
				slog.DebugContext(w.ctx, "runner is locked; skipping", "runner_name", runner.Name)
				continue
			}

			slog.InfoContext(
				w.ctx, "reaping timed-out/failed runner",
				"runner_name", runner.Name)

			if err := w.removeRunnerFromGithubAndSetPendingDelete(runner.Name, runner.AgentID); err != nil {
				// Don't let a single poisoned runner (e.g. one that raced into a
				// status that can no longer transition to pending_delete through
				// some other codepath) abort reaping for the rest of the batch.
				// Log it, release just this runner's lock, and keep going.
				slog.ErrorContext(w.ctx, "error removing runner", "runner_name", runner.Name, "error", err)
				locking.Unlock(runner.Name, false)
				continue
			}
			lockNames = append(lockNames, runner.Name)
		}
	}
	return unlockFn, nil
}

func (w *Worker) consolidateRunnerState(runners []params.RunnerReference) error {
	w.mux.Lock()
	defer w.mux.Unlock()

	ghRunnersByName := make(map[string]params.RunnerReference)
	for _, runner := range runners {
		ghRunnersByName[runner.Name] = runner
	}

	scaleSetCli, err := w.GetScaleSetClient()
	if err != nil {
		return fmt.Errorf("getting scale set client: %w", err)
	}
	dbRunnersByName := w.runnerByName()
	// Cross check what exists in github with what we have in the database.
	for name, runner := range ghRunnersByName {
		status := runner.GetStatus()
		if _, ok := dbRunnersByName[name]; !ok {
			// runner appears to be active. Is it not managed by GARM?
			if status != params.RunnerIdle && status != params.RunnerActive {
				slog.InfoContext(w.ctx, "runner does not exist in GARM; removing from github", "runner_name", name)
				if err := scaleSetCli.RemoveRunner(w.ctx, runner.ID); err != nil {
					if errors.Is(err, runnerErrors.ErrNotFound) {
						continue
					}
					slog.ErrorContext(w.ctx, "error removing runner", "runner_name", runner.Name, "error", err)
				}
			}
			continue
		}
	}

	unlockFn, err := w.reapTimedOutRunners(ghRunnersByName)
	if err != nil {
		return fmt.Errorf("reaping timed out runners: %w", err)
	}
	defer unlockFn()

	// refresh the map. It may have been mutated above.
	dbRunnersByName = w.runnerByName()
	// Cross check what exists in the database with what we have in github.
	for name, runner := range dbRunnersByName {
		// in the case of scale sets, JIT configs are used. There is no situation
		// in which we create a runner in the DB and one does not exist in github.
		// We can safely assume that if the runner is not in github anymore, it can
		// be removed from the provider and the DB.
		switch runner.Status {
		case commonParams.InstancePendingDelete, commonParams.InstancePendingForceDelete,
			commonParams.InstanceDeleting, commonParams.InstanceDeleted:
			continue
		case commonParams.InstanceCreating:
			// The provider worker owns provisioning; an instance in creating
			// cannot transition to pending_delete while the create call is in
			// flight. If its runner is missing from github, we will pick it up
			// on a later pass, once the create returns and the instance moves
			// to running or error.
			continue
		}

		if _, ok := ghRunnersByName[name]; !ok {
			if ok := locking.TryLock(name, w.consumerID); !ok {
				slog.DebugContext(w.ctx, "runner is locked; skipping", "runner_name", name)
				continue
			}
			// unlock the runner only after this function returns. These locks are
			// worker-local: they serialize this worker's own goroutines (listener
			// handlers, reaper, scale down) so none of them process this runner
			// while we reconcile it, including the provider cross-check further
			// down. The provider worker does NOT participate in instance locking;
			// it reacts to the pending_delete status via watcher events as soon
			// as the transition commits. Cross-worker coordination is done by the
			// status state machine, not by these locks.
			defer locking.Unlock(name, false)

			slog.InfoContext(w.ctx, "runner does not exist in github; removing from provider", "runner_name", name)
			instance, err := w.setRunnerDBStatus(runner.Name, commonParams.InstancePendingDelete)
			if err != nil {
				var te *runnerErrors.InstanceTransitionError
				switch {
				case errors.Is(err, runnerErrors.ErrNotFound):
					// The record is already gone from the database. Drop it from
					// the local cache rather than writing back the zero-value
					// instance below.
					delete(w.runners, runner.ID)
					continue
				case errors.As(err, &te) && garmErrors.InstanceIsBeingDeleted(te.From):
					// Already being deleted by another code path; nothing to do.
					continue
				default:
					// Don't let one runner's error abort consolidation for the
					// entire scale set; log it and keep processing the rest.
					slog.ErrorContext(w.ctx, "error updating runner", "runner_name", runner.Name, "error", err)
					continue
				}
			}
			// We will get an update event anyway from the watcher, but updating the runner
			// here, will prevent race conditions if some other event is already in the queue
			// which involves this runner. For the duration of the lifetime of this function, we
			// hold the lock, so no race condition can occur.
			w.runners[runner.ID] = instance
		}
	}

	return w.consolidateProviderState()
}

// consolidateProviderState cross checks what exists in the provider with the
// DB. Provider instances with no DB record are removed from the provider, and
// DB runners with no provider instance are marked as pending_delete. Must be
// called with w.mux held.
func (w *Worker) consolidateProviderState() error {
	pseudoPoolID, err := w.pseudoPoolID()
	if err != nil {
		return fmt.Errorf("getting pseudo pool ID: %w", err)
	}
	listParams := common.ListInstancesParams{
		ListInstancesV011: common.ListInstancesV011Params{
			ProviderBaseParams: common.ProviderBaseParams{
				ControllerInfo: w.controllerInfo,
			},
		},
	}

	providerRunners, err := w.provider.ListInstances(w.ctx, pseudoPoolID, listParams)
	if err != nil {
		return fmt.Errorf("listing instances: %w", err)
	}

	providerRunnersByName := make(map[string]commonParams.ProviderInstance)
	for _, runner := range providerRunners {
		providerRunnersByName[runner.Name] = runner
	}

	deleteInstanceParams := common.DeleteInstanceParams{
		DeleteInstanceV011: common.DeleteInstanceV011Params{
			ProviderBaseParams: common.ProviderBaseParams{
				ControllerInfo: w.controllerInfo,
			},
		},
	}

	// The runner cache may have been mutated by the github cross check.
	dbRunnersByName := w.runnerByName()
	for _, runner := range providerRunners {
		if _, ok := dbRunnersByName[runner.Name]; !ok {
			slog.InfoContext(w.ctx, "runner does not exist in database; removing from provider", "runner_name", runner.Name)
			// There is no situation in which the runner will disappear from the provider
			// after it was removed from the database. The provider worker will remove the
			// instance from the provider and mark the instance as deleted in the database.
			// It is the responsibility of the scaleset worker to then clean up the runners
			// in the deleted state.
			// That means that if we have a runner in the provider but not the DB, it is most
			// likely an inconsistency.
			if err := w.provider.DeleteInstance(w.ctx, runner.Name, deleteInstanceParams); err != nil {
				slog.ErrorContext(w.ctx, "error removing instance", "instance_name", runner.Name, "error", err)
			}
			continue
		}
	}

	dbRunnersByName = w.runnerByName()
	for _, runner := range dbRunnersByName {
		switch runner.Status {
		case commonParams.InstancePendingDelete, commonParams.InstancePendingForceDelete,
			commonParams.InstanceDeleting, commonParams.InstanceDeleted:
			// This instance is already being deleted.
			continue
		case commonParams.InstanceCreating, commonParams.InstancePendingCreate:
			// Instance is still being created in the provider, or is about to be created.
			// Allow it to finish.
			continue
		}

		locked := locking.TryLock(runner.Name, w.consumerID)
		if !locked {
			slog.DebugContext(w.ctx, "runner is locked; skipping", "runner_name", runner.Name)
			continue
		}

		if _, ok := providerRunnersByName[runner.Name]; !ok {
			// The runner is not in the provider anymore. Remove it from the DB.
			slog.InfoContext(w.ctx, "runner does not exist in provider; removing from database", "runner_name", runner.Name)
			if err := w.removeRunnerFromGithubAndSetPendingDelete(runner.Name, runner.AgentID); err != nil {
				// Same reasoning as reapTimedOutRunners and the github cross
				// check above: one poisoned runner must not abort the sweep
				// for the rest of the scale set. Log it and keep going.
				slog.ErrorContext(w.ctx, "error removing runner", "runner_name", runner.Name, "error", err)
				locking.Unlock(runner.Name, false)
				continue
			}
		}
		locking.Unlock(runner.Name, false)
	}

	return nil
}

func (w *Worker) pseudoPoolID() (string, error) {
	// This is temporary. We need to extend providers to know about scale sets.
	entity, err := w.scaleSet.GetEntity()
	if err != nil {
		return "", fmt.Errorf("getting entity: %w", err)
	}
	return fmt.Sprintf("%s-%s", w.scaleSet.Name, entity.ID), nil
}

func (w *Worker) handleScaleSetEvent(event dbCommon.ChangePayload) {
	scaleSet, ok := event.Payload.(params.ScaleSet)
	if !ok {
		slog.ErrorContext(w.ctx, "invalid payload for scale set type", "scale_set_type", event.EntityType, "payload", event.Payload)
		return
	}
	switch event.Operation {
	case dbCommon.UpdateOperation:
		slog.DebugContext(w.ctx, "got update operation")
		w.mux.Lock()
		if scaleSet.UpdatedAt.Before(w.scaleSet.UpdatedAt) {
			currentUpdatedAt := w.scaleSet.UpdatedAt
			w.mux.Unlock()
			slog.DebugContext(
				w.ctx,
				"ignoring stale scale set update",
				"current_updated_at", currentUpdatedAt,
				"event_updated_at", scaleSet.UpdatedAt,
			)
			return
		}
		previousTarget := w.targetRunners()

		if scaleSet.MaxRunners < w.scaleSet.MaxRunners || !scaleSet.Enabled {
			// we stop the listener if the scale set is disabled or if the max runners
			// is decreased. In the case where max runners changes but the scale set
			// is still enabled, we rely on the keepListenerAlive to restart the listener
			// which will listen for new messages with the changed max runners. This way
			// we don't have to potentially wait for 50 second for the max runner value
			// to be updated, in which time we might get more runners spawned than the
			// new max runner value.
			if err := w.listener.Stop(); err != nil {
				slog.ErrorContext(w.ctx, "error stopping listener", "error", err)
			}
		}
		w.scaleSet = scaleSet
		targetIncreased := w.targetRunners() > previousTarget
		w.mux.Unlock()
		if targetIncreased {
			w.signalAutoScale()
		}
	default:
		slog.DebugContext(w.ctx, "invalid operation type; ignoring", "operation_type", event.Operation)
	}
}

func (w *Worker) handleInstanceCleanup(instance params.Instance) error {
	if instance.Status == commonParams.InstanceDeleted {
		if err := w.store.DeleteInstanceByName(w.ctx, instance.Name); err != nil {
			if !errors.Is(err, runnerErrors.ErrNotFound) {
				return fmt.Errorf("deleting instance %s: %w", instance.ID, err)
			}
		}
		delete(w.runners, instance.ID)
	}
	return nil
}

// runnerSnapshotIsNewer must be called with w.mux held.
func (w *Worker) runnerSnapshotIsNewer(instance params.Instance) bool {
	latest, ok := w.runnerSnapshots[instance.ID]
	if current, exists := w.runners[instance.ID]; exists && (!ok || current.UpdatedAt.After(latest)) {
		latest = current.UpdatedAt
		ok = true
	}
	return !ok || instance.UpdatedAt.After(latest)
}

// recordRunnerSnapshot must be called with w.mux held.
func (w *Worker) recordRunnerSnapshot(instance params.Instance) {
	if w.runnerSnapshots == nil {
		w.runnerSnapshots = make(map[string]time.Time)
	}
	w.runnerSnapshots[instance.ID] = instance.UpdatedAt
}

// applyRunnerSnapshot must be called with w.mux held. The instance watcher
// fans out handlers, and a successful creation can finish after a later
// provider update, so only the newest database snapshot may update the cache.
func (w *Worker) applyRunnerSnapshot(instance params.Instance) bool {
	if !w.runnerSnapshotIsNewer(instance) {
		return false
	}
	w.recordRunnerSnapshot(instance)
	if instance.Status == commonParams.InstanceDeleted {
		delete(w.runners, instance.ID)
		return true
	}
	w.runners[instance.ID] = instance
	return true
}

func (w *Worker) handleInstanceEntityEvent(event dbCommon.ChangePayload) {
	instance, ok := event.Payload.(params.Instance)
	if !ok {
		slog.ErrorContext(w.ctx, "invalid payload for instance type", "instance_type", event.EntityType, "payload", event.Payload)
		return
	}
	switch event.Operation {
	case dbCommon.CreateOperation:
		slog.DebugContext(w.ctx, "got create operation")
		w.mux.Lock()
		w.applyRunnerSnapshot(instance)
		w.mux.Unlock()
	case dbCommon.UpdateOperation:
		slog.DebugContext(w.ctx, "got update operation")
		w.mux.Lock()
		if !w.runnerSnapshotIsNewer(instance) {
			w.mux.Unlock()
			return
		}
		if instance.Status == commonParams.InstanceDeleted {
			if err := w.handleInstanceCleanup(instance); err != nil {
				slog.ErrorContext(w.ctx, "error cleaning up instance", "instance_id", instance.ID, "error", err)
			} else {
				w.recordRunnerSnapshot(instance)
			}
			w.mux.Unlock()
			return
		}
		w.applyRunnerSnapshot(instance)
		w.mux.Unlock()
	case dbCommon.DeleteOperation:
		slog.DebugContext(w.ctx, "got delete operation")
		w.mux.Lock()
		if w.runnerSnapshotIsNewer(instance) {
			w.recordRunnerSnapshot(instance)
			delete(w.runners, instance.ID)
		}
		w.mux.Unlock()
	default:
		slog.DebugContext(w.ctx, "invalid operation type; ignoring", "operation_type", event.Operation)
	}
}

func (w *Worker) handleEvent(event dbCommon.ChangePayload) {
	switch event.EntityType {
	case dbCommon.ScaleSetEntityType:
		slog.DebugContext(w.ctx, "got scaleset event")
		w.handleScaleSetEvent(event)
	case dbCommon.InstanceEntityType:
		slog.DebugContext(w.ctx, "got instance event")
		go w.handleInstanceEntityEvent(event)
	default:
		slog.DebugContext(w.ctx, "invalid entity type; ignoring", "entity_type", event.EntityType)
	}
}

func (w *Worker) loop() {
	defer w.Stop()

	for {
		select {
		case <-w.quit:
			return
		case event, ok := <-w.consumer.Watch():
			if !ok {
				slog.InfoContext(w.ctx, "consumer channel closed")
				return
			}
			w.handleEvent(event)
		case <-w.ctx.Done():
			slog.DebugContext(w.ctx, "context done")
			return
		}
	}
}

func (w *Worker) sleepWithCancel(sleepTime time.Duration) (canceled bool) {
	if sleepTime == 0 {
		return false
	}
	ticker := time.NewTicker(sleepTime)
	defer ticker.Stop()

	select {
	case <-ticker.C:
		return false
	case <-w.quit:
	case <-w.ctx.Done():
	}
	return true
}

func (w *Worker) sessionLoopMayRun() bool {
	w.mux.Lock()
	defer w.mux.Unlock()
	return w.scaleSet.Enabled
}

func (w *Worker) keepListenerAlive() {
	var backoff time.Duration
Loop:
	for {
		if !w.sessionLoopMayRun() {
			if canceled := w.sleepWithCancel(2 * time.Second); canceled {
				slog.InfoContext(w.ctx, "worker is stopped; exiting keepListenerAlive")
				return
			}
			continue
		}
		// noop if already started.
		if err := w.listener.Start(); err != nil {
			slog.ErrorContext(w.ctx, "error starting listener", "error", err, "consumer_id", w.consumerID)
			if canceled := w.sleepWithCancel(2 * time.Second); canceled {
				slog.InfoContext(w.ctx, "worker is stopped; exiting keepListenerAlive")
				return
			}
			// we failed to start the listener. Try again.
			continue
		}

		select {
		case <-w.quit:
			return
		case <-w.ctx.Done():
			return
		case <-w.listener.Wait():
			slog.DebugContext(w.ctx, "listener is stopped; attempting to restart")
			w.mux.Lock()
			if !w.scaleSet.Enabled {
				if err := w.listener.Stop(); err != nil {
					slog.ErrorContext(w.ctx, "failed to stop listener", "error", err)
				}
				w.mux.Unlock()
				continue Loop
			}
			w.mux.Unlock()
			for {
				w.mux.Lock()
				// In case the scaleset was disabled while we were in the
				// backoff sleep.
				if !w.scaleSet.Enabled {
					w.mux.Unlock()
					continue Loop
				}
				slog.DebugContext(w.ctx, "attempting to restart")
				if err := w.listener.Start(); err != nil {
					w.mux.Unlock()
					switch backoff {
					case 0:
						backoff = 5 * time.Second
						slog.InfoContext(w.ctx, "backing off restart attempt", "backoff", backoff)
					default:
						backoff = min(time.Duration(float64(backoff)*1.5), 60*time.Second)
					}
					slog.ErrorContext(w.ctx, "error restarting listener", "error", err, "backoff", backoff)
					if canceled := w.sleepWithCancel(backoff); canceled {
						slog.DebugContext(w.ctx, "listener restart canceled")
						return
					}
					continue
				}
				backoff = 0
				w.mux.Unlock()
				continue Loop
			}
		}
	}
}

func (w *Worker) handleScaleUp() {
	if !w.scaleSet.Enabled {
		slog.DebugContext(w.ctx, "scale set is disabled; not scaling up")
		return
	}

	currentRunners := w.runnerCount()
	creationsToStart := runnerCreationsToStart(w.targetRunners(), currentRunners, w.creationsInFlight)
	if creationsToStart == 0 {
		slog.DebugContext(w.ctx, "target is less than or equal to current; not scaling up")
		return
	}

	controllerConfig, err := w.store.ControllerInfo()
	if err != nil {
		slog.ErrorContext(w.ctx, "error getting controller config", "error", err)
		return
	}

	scaleSetCli, err := w.GetScaleSetClient()
	if err != nil {
		slog.ErrorContext(w.ctx, "error getting scale set client", "error", err)
		return
	}

	scaleSet := w.scaleSet
	workerQuit := w.quit
	w.creationsInFlight += creationsToStart
	slog.InfoContext(
		w.ctx,
		"scheduling runner creations",
		"count", creationsToStart,
		"current_runners", currentRunners,
		"in_flight", w.creationsInFlight,
		"target_runners", w.targetRunners(),
	)
	startRunnerCreations(creationsToStart, func() {
		w.createScaleSetRunner(controllerConfig, scaleSet, scaleSetCli, workerQuit)
	})
}

func runnerCreationsToStart(target, current, inFlight int) int {
	needed := target - current - inFlight
	availableSlots := maxConcurrentRunnerCreations - inFlight
	if needed <= 0 || availableSlots <= 0 {
		return 0
	}
	return min(needed, availableSlots)
}

func startRunnerCreations(count int, create func()) {
	for range count {
		go create()
	}
}

func (w *Worker) createScaleSetRunner(
	controllerConfig params.ControllerInfo,
	scaleSet params.ScaleSet,
	scaleSetCli *scalesets.ScaleSetClient,
	workerQuit <-chan struct{},
) {
	var dbInstance params.Instance
	created := false
	createStarted := time.Now()
	defer func() {
		if created {
			w.finishRunnerCreation(&dbInstance)
			return
		}
		w.finishRunnerCreation(nil)
	}()

	newRunnerName := strings.ToLower(fmt.Sprintf("%s-%s", scaleSet.GetRunnerPrefix(), util.NewID()))
	jitStarted := time.Now()
	jitConfig, err := scaleSetCli.GenerateJitRunnerConfig(w.ctx, newRunnerName, scaleSet.ScaleSetID)
	if err != nil {
		slog.ErrorContext(w.ctx, "error generating jit config", "error", err)
		return
	}
	jitDuration := time.Since(jitStarted)
	slog.InfoContext(
		w.ctx,
		"generated runner jit config",
		"runner_name", newRunnerName,
		"duration", jitDuration,
	)
	slog.DebugContext(w.ctx, "creating new runner", "runner_name", newRunnerName)
	decodedJit, err := jitConfig.DecodedJITConfig()
	if err != nil {
		slog.ErrorContext(w.ctx, "error decoding jit config", "error", err)
		if err := scaleSetCli.RemoveRunner(w.ctx, jitConfig.Runner.ID); err != nil {
			slog.ErrorContext(w.ctx, "error deleting runner", "error", err)
		}
		return
	}

	runnerParams := params.CreateInstanceParams{
		Name:              newRunnerName,
		Status:            commonParams.InstancePendingCreate,
		RunnerStatus:      params.RunnerPending,
		OSArch:            scaleSet.OSArch,
		OSType:            scaleSet.OSType,
		CallbackURL:       controllerConfig.CallbackURL,
		MetadataURL:       controllerConfig.MetadataURL,
		CreateAttempt:     1,
		GitHubRunnerGroup: scaleSet.GitHubRunnerGroup,
		JitConfiguration:  decodedJit,
		AgentID:           jitConfig.Runner.ID,
	}

	// JIT generation can block long enough for more job messages, a max-runner
	// reduction, or a disable to arrive. Keep that control path unblocked, then
	// commit this reservation only if it is still part of the current target.
	w.mux.Lock()
	if !w.runnerCreationStillNeeded(scaleSet.ID, workerQuit) {
		w.mux.Unlock()
		if err := scaleSetCli.RemoveRunner(w.ctx, jitConfig.Runner.ID); err != nil {
			slog.ErrorContext(w.ctx, "error deleting runner", "error", err)
		}
		return
	}
	dbStarted := time.Now()
	dbInstance, err = w.store.CreateScaleSetInstance(w.ctx, scaleSet.ID, runnerParams)
	w.mux.Unlock()
	if err != nil {
		slog.ErrorContext(w.ctx, "error creating instance", "error", err)
		if err := scaleSetCli.RemoveRunner(w.ctx, jitConfig.Runner.ID); err != nil {
			slog.ErrorContext(w.ctx, "error deleting runner", "error", err)
		}
		return
	}
	created = true
	slog.InfoContext(
		w.ctx,
		"committed runner creation",
		"runner_name", newRunnerName,
		"jit_duration", jitDuration,
		"db_duration", time.Since(dbStarted),
		"total_duration", time.Since(createStarted),
	)
}

func (w *Worker) finishRunnerCreation(instance *params.Instance) {
	w.mux.Lock()
	w.creationsInFlight--
	if instance != nil {
		w.applyRunnerSnapshot(*instance)
	}
	w.mux.Unlock()

	// Re-evaluate after every released reservation. If this creation failed
	// after the rest of its wave succeeded, the freed slot needs a replacement
	// immediately instead of waiting for the periodic autoscale tick.
	w.signalAutoScale()
}

// runnerCreationStillNeeded must be called with w.mux held. creationsInFlight
// includes the caller's reservation, so target equality means it is needed.
func (w *Worker) runnerCreationStillNeeded(scaleSetID uint, workerQuit <-chan struct{}) bool {
	select {
	case <-workerQuit:
		return false
	default:
	}
	return w.scaleSet.Enabled &&
		w.scaleSet.ID == scaleSetID &&
		w.runnerCount()+w.creationsInFlight <= w.targetRunners()
}

func (w *Worker) waitForToolsOrCancel() (hasTools, stopped bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		entity, err := w.scaleSet.GetEntity()
		if err != nil {
			slog.ErrorContext(w.ctx, "error getting entity", "error", err)
		}
		if _, err := cache.GetGithubToolsCache(entity.ID); err != nil {
			slog.DebugContext(w.ctx, "tools not found in cache; waiting for tools")
			return false, false
		}
		return true, false
	case <-w.quit:
		return false, true
	case <-w.ctx.Done():
		return false, true
	}
}

func (w *Worker) handleScaleDown() {
	delta := w.runnerCount() - w.targetRunners()
	if delta <= 0 {
		return
	}
	scaleSetCli, err := w.GetScaleSetClient()
	if err != nil {
		slog.ErrorContext(w.ctx, "error getting scale set client", "error", err)
		return
	}
	removed := 0
	for _, runner := range w.runners {
		slog.InfoContext(w.ctx, "considering runners for removal", "delta", delta, "removed", removed)
		if removed >= delta {
			break
		}
		switch runner.Status {
		case commonParams.InstanceRunning:
			if !runnerCanScaleDown(runner, time.Now()) {
				slog.DebugContext(w.ctx, "runner is not ready for scale down; skipping", "runner_name", runner.Name, "runner_status", runner.RunnerStatus)
				continue
			}
			locked := locking.TryLock(runner.Name, w.consumerID)
			if !locked {
				slog.DebugContext(w.ctx, "runner is locked; skipping", "runner_name", runner.Name)
				continue
			}
			slog.DebugContext(w.ctx, "removing runner", "runner_name", runner.Name)
			if err := scaleSetCli.RemoveRunner(w.ctx, runner.AgentID); err != nil {
				if !errors.Is(err, runnerErrors.ErrNotFound) {
					slog.ErrorContext(w.ctx, "error removing runner", "runner_name", runner.Name, "error", err)
					locking.Unlock(runner.Name, false)
					continue
				}
			}
			runnerUpdateParams := params.UpdateInstanceParams{
				Status: commonParams.InstancePendingDelete,
			}
			updatedRunner, err := w.store.UpdateInstance(w.ctx, runner.Name, runnerUpdateParams)
			if err != nil {
				if errors.Is(err, runnerErrors.ErrNotFound) {
					// The error seems to be that the instance was removed from the database. We still had it in our
					// state, so either the update never came from the watcher or something else happened.
					// Remove it from the local cache.
					delete(w.runners, runner.ID)
					removed++
					locking.Unlock(runner.Name, true)
					continue
				}
				var te *runnerErrors.InstanceTransitionError
				if errors.As(err, &te) && garmErrors.InstanceIsBeingDeleted(te.From) {
					// Another code path already moved the instance into the
					// deletion lane; it is being removed, which is what we wanted.
					// Keep the lock entry (remove=false): the runner record still
					// exists and other goroutines of this worker may be blocked
					// on this lock; removing the entry from under a blocked
					// waiter would let two goroutines hold the same runner's lock.
					removed++
					locking.Unlock(runner.Name, false)
					continue
				}
				// nolint:golangci-lint,godox
				// TODO: This should not happen, unless there is some issue with the database.
				// The UpdateInstance() function should add tenacity, but even in that case, if it
				// still errors out, we need to handle it somehow.
				slog.ErrorContext(w.ctx, "error updating runner", "runner_name", runner.Name, "error", err)
				locking.Unlock(runner.Name, false)
				continue
			}
			w.runners[runner.ID] = updatedRunner
			locking.Unlock(runner.Name, false)
			removed++
		case commonParams.InstancePendingDelete, commonParams.InstancePendingForceDelete,
			commonParams.InstanceDeleting, commonParams.InstanceDeleted:
			continue
		default:
			slog.WarnContext(w.ctx, "runner is not in a valid state; skipping", "runner_name", runner.Name, "runner_status", runner.Status)
			continue
		}
	}
}

func runnerCanScaleDown(runner params.Instance, now time.Time) bool {
	if runner.Status != commonParams.InstanceRunning || runner.RunnerStatus != params.RunnerIdle {
		return false
	}
	return runner.UpdatedAt.IsZero() || !now.Before(runner.UpdatedAt.Add(scaleDownIdleGrace))
}

func (w *Worker) targetRunners() int {
	var desiredRunners uint
	if w.scaleSet.DesiredRunnerCount > 0 {
		desiredRunners = uint(w.scaleSet.DesiredRunnerCount)
	}
	// Speculative runners are the forecast for jobs GitHub has not queued yet.
	// They add to the target rather than replacing it: assigned jobs and
	// predicted ones are different work, and a job that gets queued consumes
	// its unit of the forecast, so the two do not double count for long.
	var speculativeRunners uint
	if w.speculativeRunners > 0 {
		speculativeRunners = uint(w.speculativeRunners)
	}
	// Speculative runners whose machine has not been reclaimed yet are already
	// paid for, so the forecast must not ask for them twice. Only the
	// speculative part of the target is held back: assigned jobs are real work
	// and a runner that dies under one still needs replacing immediately.
	speculativeRunners -= min(speculativeRunners, uint(w.speculativeRunnersBeingTornDown()))
	targetRunners := min(w.scaleSet.MinIdleRunners+desiredRunners+speculativeRunners, w.scaleSet.MaxRunners)

	return int(targetRunners)
}

// speculativeRunnersBeingTornDown counts this scale set's unclaimed speculative
// runners whose row has left the states runnerCount() accepts but whose machine
// may well still be running. A restart puts every interrupted create in exactly
// that position at once, so without this the worker reads a full cohort as zero
// and orders the whole cohort again while the first one is still up.
func (w *Worker) speculativeRunnersBeingTornDown() int {
	count := 0
	for _, runner := range w.runners {
		if !runner.Speculative || runner.ReservedForWorkflowJobID != nil {
			continue
		}
		if garmErrors.InstanceIsBeingDeleted(runner.Status) {
			count++
		}
	}
	return count
}

func (w *Worker) runnerCount() int {
	// Terminated and deleting runners cannot serve the desired jobs. Active
	// runners still count because GitHub's TotalAssignedJobs includes running
	// jobs; excluding them causes a replacement to be created for every start.
	count := 0
	for _, runner := range w.runners {
		if garmErrors.InstanceIsBeingDeleted(runner.Status) {
			continue
		}
		if runner.RunnerStatus == params.RunnerTerminated {
			continue
		}
		count++
	}
	return count
}

func (w *Worker) signalAutoScale() {
	select {
	case w.autoScaleWake <- struct{}{}:
	default:
	}
}

func (w *Worker) handleAutoScale() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastMsg := ""
	lastMsgDebugLog := func(msg string, targetRunners, currentRunners int) {
		if lastMsg != msg {
			slog.DebugContext(w.ctx, msg, "current_runners", currentRunners, "target_runners", targetRunners)
			lastMsg = msg
		}
	}

	for {
		hasTools, stopped := w.waitForToolsOrCancel()
		if stopped {
			slog.DebugContext(w.ctx, "worker is stopped; exiting handleAutoScale")
			return
		}

		if !hasTools {
			w.sleepWithCancel(1 * time.Second)
			continue
		}

		select {
		case <-w.quit:
			return
		case <-w.ctx.Done():
			return
		case <-ticker.C:
		case <-w.autoScaleWake:
		}

		w.mux.Lock()
		w.refreshPrewarmForecastLocked()
		for _, instance := range w.runners {
			if err := w.handleInstanceCleanup(instance); err != nil {
				slog.ErrorContext(w.ctx, "error cleaning up instance", "instance_id", instance.ID, "error", err)
			}
		}

		if w.runnerCount() == w.targetRunners() {
			lastMsgDebugLog("desired runner count reached", w.targetRunners(), w.runnerCount())
			w.mux.Unlock()
			continue
		}

		if w.runnerCount() < w.targetRunners() {
			lastMsgDebugLog("scaling up", w.targetRunners(), w.runnerCount())
			w.handleScaleUp()
		} else {
			lastMsgDebugLog("attempting to scale down", w.targetRunners(), w.runnerCount())
			w.handleScaleDown()
		}
		w.mux.Unlock()
	}
}
