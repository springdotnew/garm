package migrations

import (
	"testing"
	"time"

	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Verify 0008 applies to a database that already holds prewarm requests, and
// that it arms them.
//
// The backfill is the whole risk here. Every speculative read filters on
// `armed_at IS NOT NULL`, so a cohort that is mid-flight when the controller is
// upgraded would become invisible the moment the new binary starts — the
// runners already exist, nothing would reconcile against them, and they would
// sit until the reaper took them. Arming pre-existing rows at their creation
// time preserves exactly the behaviour they had before the column existed.
func TestPrewarmArmedAtMigrationOnExistingDB(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(t.TempDir()+"/test.db"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	var target []*gormigrate.Migration
	for _, m := range All() {
		if m.ID == "0007_prewarm" || m.ID == "0008_prewarm_armed_at" {
			target = append(target, m)
		}
	}
	if len(target) != 2 {
		t.Fatalf("expected 0007 and 0008, got %d", len(target))
	}

	for _, stmt := range []string{
		"CREATE TABLE instances (id text PRIMARY KEY, name text)",
		"CREATE TABLE workflow_jobs (id integer PRIMARY KEY, name text)",
		"CREATE TABLE controller_infos (id text PRIMARY KEY, controller_id text)",
	} {
		if err := db.Exec(stmt).Error; err != nil {
			t.Fatalf("failed to seed schema with %q: %v", stmt, err)
		}
	}

	// 0007 alone, so the row below is written by a schema with no armed_at.
	if err := gormigrate.New(db, gormigrate.DefaultOptions, target[:1]).Migrate(); err != nil {
		t.Fatalf("0007 failed: %v", err)
	}

	createdAt := time.Date(2026, 7, 28, 4, 8, 41, 0, time.UTC)
	if err := db.Exec(
		"INSERT INTO prewarm_requests (id, created_at, updated_at, entity_id, entity_type, "+
			"repository, workflow_name, run_id, run_attempt, rule_id, trigger_job_id, mode, state, expires_at) "+
			"VALUES (?, ?, ?, ?, 'organization', 'springdotnew/spring', 'PR Tests', 1, 1, 'r', 42, 'active', 'active', ?)",
		"11111111-1111-1111-1111-111111111111", createdAt, createdAt,
		"22222222-2222-2222-2222-222222222222", createdAt.Add(8*time.Minute),
	).Error; err != nil {
		t.Fatalf("failed to seed a pre-upgrade request: %v", err)
	}

	if err := gormigrate.New(db, gormigrate.DefaultOptions, target).Migrate(); err != nil {
		t.Fatalf("0008 failed: %v", err)
	}

	if !db.Migrator().HasColumn(&prewarmRequest0008{}, "armed_at") {
		t.Fatal("prewarm_requests is missing column armed_at")
	}

	var armed []time.Time
	if err := db.Raw("SELECT armed_at FROM prewarm_requests").Scan(&armed).Error; err != nil {
		t.Fatalf("failed to read armed_at: %v", err)
	}
	if len(armed) != 1 {
		t.Fatalf("expected 1 request, got %d", len(armed))
	}
	if !armed[0].UTC().Equal(createdAt) {
		t.Fatalf("pre-existing request should be armed at its creation time %s, got %s",
			createdAt, armed[0].UTC())
	}
}
