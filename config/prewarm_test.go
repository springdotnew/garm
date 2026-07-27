// Copyright 2025 Cloudbase Solutions SRL
//
//	Licensed under the Apache License, Version 2.0 (the "License"); you may
//	not use this file except in compliance with the License. You may obtain
//	a copy of the License at
//
//	     https://www.apache.org/licenses/LICENSE-2.0
//
//	Unless required by applicable law or agreed to in writing, software
//	distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//	WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//	License for the specific language governing permissions and limitations
//	under the License.

package config

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func validPrewarmRule() PrewarmRule {
	return PrewarmRule{
		ID:            "spring-pr-tests-attempt-1",
		Repository:    "springdotnew/spring",
		Workflow:      "PR Tests",
		TriggerJob:    "changes",
		TriggerAction: "queued",
		RunAttempt:    1,
		Targets: []PrewarmTarget{
			{Labels: []string{"gcp-4vcpu-spot"}, Count: 79},
			{Labels: []string{"gcp-8vcpu"}, Count: 10},
		},
	}
}

func validPrewarmConfig() Prewarm {
	return Prewarm{
		Enable:                true,
		Mode:                  PrewarmModeShadow,
		MaxSpeculativeRunners: 120,
		DefaultTTL:            "8m",
		Rules:                 []PrewarmRule{validPrewarmRule()},
	}
}

func TestPrewarmValidateValidConfig(t *testing.T) {
	cfg := validPrewarmConfig()
	require.NoError(t, cfg.Validate())
	require.Equal(t, 8*time.Minute, cfg.TTL())
	require.False(t, cfg.IsActive(), "shadow mode must not report as active")

	cfg.Mode = PrewarmModeActive
	require.True(t, cfg.IsActive())
}

func TestPrewarmZeroValueIsDisabledAndValid(t *testing.T) {
	var cfg Prewarm
	require.NoError(t, cfg.Validate())
	require.False(t, cfg.IsActive())
	require.Equal(t, DefaultPrewarmTTL, cfg.TTL())
}

func TestPrewarmValidateEnabledConfig(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*Prewarm)
		errString string
	}{
		{
			name:      "invalid mode",
			mutate:    func(c *Prewarm) { c.Mode = "sometimes" },
			errString: `invalid prewarm mode "sometimes"`,
		},
		{
			name:      "missing mode",
			mutate:    func(c *Prewarm) { c.Mode = "" },
			errString: "prewarm mode must be set",
		},
		{
			name:      "zero global cap",
			mutate:    func(c *Prewarm) { c.MaxSpeculativeRunners = 0 },
			errString: "max_speculative_runners must be greater than 0",
		},
		{
			name:      "no rules",
			mutate:    func(c *Prewarm) { c.Rules = nil },
			errString: "no rule is defined",
		},
		{
			name:      "invalid default ttl",
			mutate:    func(c *Prewarm) { c.DefaultTTL = "eight minutes" },
			errString: "invalid default_ttl",
		},
		{
			name:      "negative default ttl",
			mutate:    func(c *Prewarm) { c.DefaultTTL = "-8m" },
			errString: "invalid default_ttl",
		},
		{
			name: "forecast exceeds global cap",
			mutate: func(c *Prewarm) {
				c.MaxSpeculativeRunners = 10
			},
			errString: "exceeds max_speculative_runners",
		},
		{
			name: "duplicate rule id",
			mutate: func(c *Prewarm) {
				c.Rules = append(c.Rules, validPrewarmRule())
			},
			errString: "duplicate prewarm rule id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validPrewarmConfig()
			tc.mutate(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errString)
		})
	}
}

func TestPrewarmRuleValidate(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*PrewarmRule)
		errString string
	}{
		{"missing id", func(r *PrewarmRule) { r.ID = "" }, "id is mandatory"},
		{"missing repository", func(r *PrewarmRule) { r.Repository = "" }, "repository is mandatory"},
		{"repository without owner", func(r *PrewarmRule) { r.Repository = "spring" }, "owner/name form"},
		{"repository with empty owner", func(r *PrewarmRule) { r.Repository = "/spring" }, "owner/name form"},
		{"repository with extra segment", func(r *PrewarmRule) { r.Repository = "a/b/c" }, "owner/name form"},
		{"missing workflow", func(r *PrewarmRule) { r.Workflow = "" }, "workflow is mandatory"},
		{"missing trigger job", func(r *PrewarmRule) { r.TriggerJob = "" }, "trigger_job is mandatory"},
		{"unsupported action", func(r *PrewarmRule) { r.TriggerAction = "completed" }, "invalid trigger_action"},
		{"negative attempt", func(r *PrewarmRule) { r.RunAttempt = -1 }, "must not be negative"},
		{"invalid ttl", func(r *PrewarmRule) { r.TTL = "soon" }, "invalid ttl"},
		{"no targets", func(r *PrewarmRule) { r.Targets = nil }, "at least one target is mandatory"},
		{
			"zero count target",
			func(r *PrewarmRule) { r.Targets = []PrewarmTarget{{Labels: []string{"a"}, Count: 0}} },
			"count must be greater than 0",
		},
		{
			"empty labels",
			func(r *PrewarmRule) { r.Targets = []PrewarmTarget{{Labels: nil, Count: 1}} },
			"at least one label is mandatory",
		},
		{
			"blank label",
			func(r *PrewarmRule) { r.Targets = []PrewarmTarget{{Labels: []string{"  "}, Count: 1}} },
			"labels must not be empty",
		},
		{
			"duplicate label set differing only by order and case",
			func(r *PrewarmRule) {
				r.Targets = []PrewarmTarget{
					{Labels: []string{"gcp-4vcpu", "spot"}, Count: 1},
					{Labels: []string{"SPOT", "gcp-4vcpu"}, Count: 2},
				}
			},
			"duplicate target label set",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := validPrewarmRule()
			tc.mutate(&rule)
			err := rule.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errString)
		})
	}
}

// A malformed rule must be rejected even while prewarming is disabled, so the
// operator finds out when they write it rather than when they enable it.
func TestPrewarmRulesValidatedWhileDisabled(t *testing.T) {
	cfg := validPrewarmConfig()
	cfg.Enable = false
	cfg.Rules[0].Workflow = ""

	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow is mandatory")
}

func TestPrewarmRuleDefaultAction(t *testing.T) {
	rule := validPrewarmRule()
	rule.TriggerAction = ""
	require.NoError(t, rule.Validate())
	require.Equal(t, "queued", rule.Action())
}

func TestPrewarmRuleMatches(t *testing.T) {
	rule := validPrewarmRule()

	require.True(t, rule.Matches("springdotnew/spring", "PR Tests", "changes", "queued", 1))
	// GitHub repository slugs are case insensitive.
	require.True(t, rule.Matches("SpringDotNew/Spring", "PR Tests", "changes", "queued", 1))

	require.False(t, rule.Matches("springdotnew/other", "PR Tests", "changes", "queued", 1))
	require.False(t, rule.Matches("springdotnew/spring", "Other Tests", "changes", "queued", 1))
	require.False(t, rule.Matches("springdotnew/spring", "PR Tests", "typecheck", "queued", 1))
	require.False(t, rule.Matches("springdotnew/spring", "PR Tests", "changes", "in_progress", 1))
	require.False(t, rule.Matches("springdotnew/spring", "PR Tests", "changes", "queued", 2),
		"a rerun must not reuse an attempt-1 profile")

	// Workflow and job names are case sensitive: GitHub reports them verbatim
	// and a profile that matched loosely could prewarm the wrong fanout.
	require.False(t, rule.Matches("springdotnew/spring", "pr tests", "changes", "queued", 1))
}

func TestPrewarmRuleMatchesAnyAttempt(t *testing.T) {
	rule := validPrewarmRule()
	rule.RunAttempt = 0

	require.True(t, rule.Matches("springdotnew/spring", "PR Tests", "changes", "queued", 1))
	require.True(t, rule.Matches("springdotnew/spring", "PR Tests", "changes", "queued", 7))
}

func TestPrewarmTargetLabelKeyIsOrderAndCaseIndependent(t *testing.T) {
	first := PrewarmTarget{Labels: []string{"gcp-4vcpu", "self-hosted", "linux"}}
	second := PrewarmTarget{Labels: []string{"LINUX", "self-hosted", "GCP-4VCPU"}}

	require.Equal(t, first.LabelKey(), second.LabelKey())
	require.Equal(t, "gcp-4vcpu,linux,self-hosted", first.LabelKey())
}

func TestNormalizeLabelKeyDropsBlanksAndDuplicates(t *testing.T) {
	require.Equal(t, "a,b", NormalizeLabelKey([]string{"b", "  ", "a", "B", ""}))
	require.Equal(t, "", NormalizeLabelKey(nil))
}

func TestPrewarmRuleTTLOverride(t *testing.T) {
	cfg := validPrewarmConfig()
	cfg.Rules[0].TTL = "3m"
	require.NoError(t, cfg.Validate())

	require.Equal(t, 3*time.Minute, cfg.Rules[0].TTL.DurationOr(cfg.TTL()))
	// An unset rule TTL falls back to the global default.
	cfg.Rules[0].TTL = ""
	require.Equal(t, 8*time.Minute, cfg.Rules[0].TTL.DurationOr(cfg.TTL()))
}

func TestPrewarmTotalTargetCount(t *testing.T) {
	rule := validPrewarmRule()
	require.Equal(t, uint(89), rule.TotalTargetCount())
}

// The profile is authored as TOML in the controller config, so decoding the
// documented shape is part of the contract.
func TestPrewarmDecodesFromTOML(t *testing.T) {
	const document = `
[prewarm]
enable = true
mode = "shadow"
max_speculative_runners = 120
default_ttl = "8m"

[[prewarm.rule]]
id = "spring-pr-tests-attempt-1"
repository = "springdotnew/spring"
workflow = "PR Tests"
trigger_job = "changes"
trigger_action = "queued"
run_attempt = 1

[[prewarm.rule.target]]
labels = ["gcp-2vcpu-arm-spot"]
count = 2

[[prewarm.rule.target]]
labels = ["gcp-4vcpu-spot"]
count = 79
`

	var cfg struct {
		Prewarm Prewarm `toml:"prewarm"`
	}
	_, err := toml.Decode(document, &cfg)
	require.NoError(t, err)
	require.NoError(t, cfg.Prewarm.Validate())

	require.True(t, cfg.Prewarm.Enable)
	require.Equal(t, PrewarmModeShadow, cfg.Prewarm.Mode)
	require.Equal(t, uint(120), cfg.Prewarm.MaxSpeculativeRunners)
	require.Equal(t, 8*time.Minute, cfg.Prewarm.TTL())
	require.Len(t, cfg.Prewarm.Rules, 1)
	require.Len(t, cfg.Prewarm.Rules[0].Targets, 2)
	require.Equal(t, uint(81), cfg.Prewarm.Rules[0].TotalTargetCount())
}

func TestPrewarmRejectsInvalidTTLFromTOML(t *testing.T) {
	const document = `
[prewarm]
enable = true
mode = "active"
max_speculative_runners = 10
default_ttl = "eight minutes"
`

	var cfg struct {
		Prewarm Prewarm `toml:"prewarm"`
	}
	_, err := toml.Decode(document, &cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid duration")
}
