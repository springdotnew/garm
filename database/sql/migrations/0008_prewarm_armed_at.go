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
	"gorm.io/gorm"
)

// prewarmRequest0008 adds the arming timestamp.
//
// A forecast is recorded the moment the gate job's webhook arrives, but it must
// not be *served* until the gate job itself has been given a runner. Until this
// column that ordering lived in one pool manager's memory, which is invisible to
// the scale-set worker in another goroutine and was not consulted by the pool
// manager's own consolidation ticker either — so both could speculate early, and
// on live hardware the scale set did, 1.462s before the gate was reserved.
// Putting the arming in the row makes an unserved forecast invisible to every
// speculative reader at once, without any of them having to agree on a wake.
type prewarmRequest0008 struct {
	ArmedAt *time.Time `gorm:"index"`
}

func (prewarmRequest0008) TableName() string { return "prewarm_requests" }

func init() {
	Register(&gormigrate.Migration{
		ID: "0008_prewarm_armed_at",
		Migrate: func(tx *gorm.DB) error {
			if err := tx.AutoMigrate(&prewarmRequest0008{}); err != nil {
				return err
			}
			// Rows that predate the column were written by a controller that
			// served them as soon as they existed. Leaving them NULL would hide
			// an in-flight cohort from the upgraded controller and strand it, so
			// they are armed at their creation time — exactly the behaviour they
			// already had.
			return tx.Exec(
				"UPDATE prewarm_requests SET armed_at = created_at WHERE armed_at IS NULL",
			).Error
		},
	})
}
