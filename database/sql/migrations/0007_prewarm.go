// Copyright 2025 Cloudbase Solutions SRL
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

package migrations

import (
	"time"

	"github.com/go-gormigrate/gormigrate/v2"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Minimal model copies for the migration. These are intentionally decoupled
// from the main models so that future model changes don't break this migration.

type prewarmRequest0007 struct {
	ID        uuid.UUID `gorm:"type:uuid;primary_key;"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	EntityID   uuid.UUID `gorm:"index:idx_prewarm_request_dedup,unique,priority:1;index:idx_prewarm_request_entity_state,priority:1"`
	EntityType string

	Repository   string `gorm:"index:idx_prewarm_request_dedup,unique,priority:2"`
	WorkflowName string `gorm:"index:idx_prewarm_request_dedup,unique,priority:3"`
	RunID        int64  `gorm:"index:idx_prewarm_request_dedup,unique,priority:4"`
	RunAttempt   int64  `gorm:"index:idx_prewarm_request_dedup,unique,priority:5"`
	RuleID       string `gorm:"index:idx_prewarm_request_dedup,unique,priority:6"`

	TriggerJobID int64

	Mode      string
	State     string    `gorm:"index:idx_prewarm_request_entity_state,priority:2"`
	ExpiresAt time.Time `gorm:"index"`
}

func (prewarmRequest0007) TableName() string { return "prewarm_requests" }

type prewarmRequestTarget0007 struct {
	ID        uuid.UUID `gorm:"type:uuid;primary_key;"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	PrewarmRequestID uuid.UUID `gorm:"index:idx_prewarm_target_label,unique,priority:1"`
	LabelKey         string    `gorm:"index:idx_prewarm_target_label,unique,priority:2"`
	Labels           datatypes.JSON

	TargetCount    uint
	ObservedDemand uint
	CreatedCount   uint
	ClaimedCount   uint
	ReapedCount    uint
}

func (prewarmRequestTarget0007) TableName() string { return "prewarm_request_targets" }

// instance0007 and workflowJob0007 are minimal stubs that only declare the new
// columns. AutoMigrate will add them without touching existing ones.

type instance0007 struct {
	Speculative              bool       `gorm:"index:idx_instances_speculative,priority:1"`
	SpeculativeRequestID     *uuid.UUID `gorm:"index"`
	SpeculativeExpiresAt     *time.Time `gorm:"index:idx_instances_speculative,priority:2"`
	ReservedForWorkflowJobID *int64     `gorm:"index"`
}

func (instance0007) TableName() string { return "instances" }

type workflowJob0007 struct {
	RunAttempt   int64
	WorkflowName string `gorm:"index:idx_workflow_jobs_workflow_name"`
}

func (workflowJob0007) TableName() string { return "workflow_jobs" }

// controllerInfosTable is the single row that carries controller-wide runtime
// switches. Prewarming reads its pause flag on every reconcile, so the column
// has to exist before the feature can be turned on.
const controllerInfosTable = "controller_infos"

type controllerInfo0007 struct {
	PrewarmPaused bool
}

func (controllerInfo0007) TableName() string { return controllerInfosTable }

func init() {
	Register(&gormigrate.Migration{
		ID: "0007_prewarm",
		Migrate: func(tx *gorm.DB) error {
			return tx.AutoMigrate(
				&prewarmRequest0007{},
				&prewarmRequestTarget0007{},
				&instance0007{},
				&workflowJob0007{},
				&controllerInfo0007{},
			)
		},
	})
}
