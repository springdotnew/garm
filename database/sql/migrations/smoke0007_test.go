package migrations

import (
	"testing"

	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Verify 0007 applies cleanly on an existing database whose instances,
// workflow_jobs and controller_infos tables predate prewarming. Fresh
// databases take the initSchema path, so this is the only coverage for the
// gormigrate upgrade path — including the controller_infos column that carries
// the runtime kill switch.
func TestPrewarmMigrationOnExistingDB(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(t.TempDir()+"/test.db"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
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

	var target []*gormigrate.Migration
	for _, m := range All() {
		if m.ID == "0007_prewarm" {
			target = append(target, m)
		}
	}
	if len(target) != 1 {
		t.Fatalf("expected to find 0007_prewarm migration, got %d", len(target))
	}

	m := gormigrate.New(db, gormigrate.DefaultOptions, target)
	if err := m.Migrate(); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	for _, table := range []string{"prewarm_requests", "prewarm_request_targets"} {
		if !db.Migrator().HasTable(table) {
			t.Fatalf("%s table was not created", table)
		}
	}

	for _, col := range []string{
		"entity_id", "entity_type", "repository", "workflow_name",
		"run_id", "run_attempt", "rule_id", "trigger_job_id",
		"mode", "state", "expires_at",
	} {
		if !db.Migrator().HasColumn(&prewarmRequest0007{}, col) {
			t.Fatalf("prewarm_requests is missing column %q", col)
		}
	}

	for _, col := range []string{
		"prewarm_request_id", "label_key", "labels",
		"target_count", "observed_demand", "created_count", "claimed_count", "reaped_count",
	} {
		if !db.Migrator().HasColumn(&prewarmRequestTarget0007{}, col) {
			t.Fatalf("prewarm_request_targets is missing column %q", col)
		}
	}

	for _, col := range []string{
		"speculative", "speculative_request_id",
		"speculative_expires_at", "reserved_for_workflow_job_id",
	} {
		if !db.Migrator().HasColumn(&instance0007{}, col) {
			t.Fatalf("instances is missing column %q", col)
		}
	}

	for _, col := range []string{"run_attempt", "workflow_name"} {
		if !db.Migrator().HasColumn(&workflowJob0007{}, col) {
			t.Fatalf("workflow_jobs is missing column %q", col)
		}
	}

	// The kill switch lives on controller_infos. Without this column an
	// upgraded controller cannot be paused without a config change and a
	// restart, which is the whole point of the flag.
	if !db.Migrator().HasColumn(&controllerInfo0007{}, "prewarm_paused") {
		t.Fatal("controller_infos is missing prewarm_paused")
	}

	// Duplicate webhook deliveries must collapse into a single request. The
	// dedup index is what makes that true regardless of who inserts first.
	insert := `INSERT INTO prewarm_requests
		(id, entity_id, entity_type, repository, workflow_name, run_id, run_attempt, rule_id, trigger_job_id, mode, state, expires_at)
		VALUES (?, 'e1', 'repository', 'springdotnew/spring', 'PR Tests', 42, 1, 'rule-1', 7, 'active', 'active', CURRENT_TIMESTAMP)`
	if err := db.Exec(insert, "req-1").Error; err != nil {
		t.Fatalf("failed to insert prewarm request: %v", err)
	}
	if err := db.Exec(insert, "req-2").Error; err == nil {
		t.Fatal("duplicate prewarm request was not rejected")
	}

	targetInsert := `INSERT INTO prewarm_request_targets
		(id, prewarm_request_id, label_key, target_count)
		VALUES (?, 'req-1', 'gcp-4vcpu-spot', 5)`
	if err := db.Exec(targetInsert, "tgt-1").Error; err != nil {
		t.Fatalf("failed to insert prewarm target: %v", err)
	}
	if err := db.Exec(targetInsert, "tgt-2").Error; err == nil {
		t.Fatal("duplicate prewarm target label was not rejected")
	}
}
