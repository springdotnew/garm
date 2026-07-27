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
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

const (
	// PrewarmModeShadow evaluates rules and records the forecast without ever
	// asking a provider for an instance. Use it to measure forecast accuracy
	// against real traffic before spending money on speculative capacity.
	PrewarmModeShadow PrewarmMode = "shadow"
	// PrewarmModeActive creates speculative runners for the forecast.
	PrewarmModeActive PrewarmMode = "active"

	// DefaultPrewarmTTL is used when a rule and the global config both leave
	// the TTL unset. It is deliberately short: an unclaimed speculative runner
	// is pure waste, and the gate job it was forecast from resolves in about a
	// minute.
	DefaultPrewarmTTL = 8 * time.Minute

	// DefaultPrewarmPreemptionTTL is how long a replacement forecast is kept
	// when the preemption section leaves it unset. It is much longer than
	// DefaultPrewarmTTL because what separates a preemption from the retry it
	// causes is the rest of the workflow run, not a gate job that resolves in
	// about a minute.
	DefaultPrewarmPreemptionTTL = 30 * time.Minute

	// prewarmTriggerActionQueued is the only webhook action a rule may trigger
	// on. A forecast is only useful while the fanout has not been queued yet.
	prewarmTriggerActionQueued = "queued"
)

// PrewarmMode selects whether matched rules create runners or only record what
// they would have created.
type PrewarmMode string

// Prewarm holds the speculative runner prewarming configuration. Prewarming
// watches for the first (gate) job of a known workflow and creates ordinary
// ephemeral JIT runners for the fanout that job is expected to unblock, so the
// downstream jobs find warm runners instead of waiting on a cold boot.
//
// The zero value is disabled, which is a no-op on every code path.
type Prewarm struct {
	// Enable turns speculative prewarming on. When false, no rule is
	// evaluated and no speculative instance is ever created.
	Enable bool `toml:"enable" json:"enable"`
	// Mode selects between "shadow" (record the forecast only) and "active"
	// (create the forecast runners).
	Mode PrewarmMode `toml:"mode" json:"mode"`
	// MaxSpeculativeRunners caps the total number of speculative runners that
	// may exist across every rule and entity at any one time.
	MaxSpeculativeRunners uint `toml:"max_speculative_runners" json:"max-speculative-runners"`
	// DefaultTTL is how long an unclaimed speculative runner is kept before it
	// is reaped. Rules may override it.
	DefaultTTL PrewarmTTL `toml:"default_ttl" json:"default-ttl"`
	// Rules are the trigger definitions. Rule IDs must be unique.
	Rules []PrewarmRule `toml:"rule,omitempty" json:"rule,omitempty"`
	// Preemption pre-acquires replacement capacity when a spot runner is about
	// to be reclaimed.
	Preemption PrewarmPreemption `toml:"preemption,omitempty" json:"preemption,omitempty"`
}

// IsActive reports whether matched rules should create real instances.
func (p *Prewarm) IsActive() bool {
	return p.Enable && p.Mode == PrewarmModeActive
}

// TTL returns the configured default TTL, or DefaultPrewarmTTL if unset.
func (p *Prewarm) TTL() time.Duration {
	return p.DefaultTTL.DurationOr(DefaultPrewarmTTL)
}

// Validate validates the prewarm configuration. It fails closed: a malformed
// rule stops the controller from starting rather than silently disabling
// prewarming, because a silently disabled forecast looks exactly like a
// forecast that is simply never matching.
//
// Rules are validated even while prewarming is disabled, so a typo surfaces
// when it is written rather than the first time somebody flips the switch on.
func (p *Prewarm) Validate() error {
	if _, err := p.DefaultTTL.ParseDuration(); err != nil {
		return fmt.Errorf("invalid default_ttl: %w", err)
	}

	if err := p.validateRules(); err != nil {
		return err
	}

	if err := p.Preemption.Validate(); err != nil {
		return fmt.Errorf("error validating prewarm preemption: %w", err)
	}

	if !p.Enable {
		return nil
	}

	switch p.Mode {
	case PrewarmModeShadow, PrewarmModeActive:
	case "":
		return fmt.Errorf("prewarm mode must be set to %q or %q", PrewarmModeShadow, PrewarmModeActive)
	default:
		return fmt.Errorf("invalid prewarm mode %q; must be %q or %q", p.Mode, PrewarmModeShadow, PrewarmModeActive)
	}

	if p.MaxSpeculativeRunners == 0 {
		return fmt.Errorf("max_speculative_runners must be greater than 0 when prewarm is enabled")
	}

	if len(p.Rules) == 0 {
		return fmt.Errorf("prewarm is enabled but no rule is defined")
	}

	for idx := range p.Rules {
		rule := &p.Rules[idx]
		if total := rule.TotalTargetCount(); total > p.MaxSpeculativeRunners {
			return fmt.Errorf(
				"prewarm rule %q forecasts %d runners which exceeds max_speculative_runners (%d)",
				rule.ID, total, p.MaxSpeculativeRunners)
		}
	}

	return nil
}

func (p *Prewarm) validateRules() error {
	seenIDs := map[string]struct{}{}
	for idx := range p.Rules {
		rule := &p.Rules[idx]
		if err := rule.Validate(); err != nil {
			return fmt.Errorf("error validating prewarm rule %q: %w", rule.ID, err)
		}
		if _, ok := seenIDs[rule.ID]; ok {
			return fmt.Errorf("duplicate prewarm rule id %q", rule.ID)
		}
		seenIDs[rule.ID] = struct{}{}
	}
	return nil
}

// PrewarmRule matches a single trigger job and describes the fanout it is
// expected to unblock. Every match field is compared exactly; a rule that does
// not match is simply not applied.
type PrewarmRule struct {
	// ID uniquely identifies the rule. It is used in logs and metrics and to
	// deduplicate requests, so it must be stable across restarts.
	ID string `toml:"id" json:"id"`
	// Repository is the "owner/name" the trigger job belongs to.
	Repository string `toml:"repository" json:"repository"`
	// Workflow is the workflow name exactly as GitHub reports it.
	Workflow string `toml:"workflow" json:"workflow"`
	// TriggerJob is the name of the gate job whose queueing starts the forecast.
	TriggerJob string `toml:"trigger_job" json:"trigger-job"`
	// TriggerAction is the workflow_job action to match. Only "queued" is
	// meaningful; it defaults to "queued" when unset.
	TriggerAction string `toml:"trigger_action,omitempty" json:"trigger-action,omitempty"`
	// RunAttempt restricts the rule to a specific run attempt. Zero matches any
	// attempt. Attempt 1 is the usual choice because reruns often route to a
	// different pool set.
	RunAttempt int64 `toml:"run_attempt,omitempty" json:"run-attempt,omitempty"`
	// TTL overrides the global DefaultTTL for runners created by this rule.
	TTL PrewarmTTL `toml:"ttl,omitempty" json:"ttl,omitempty"`
	// Targets is the forecast pool mix, keyed by the label set a downstream job
	// will request.
	Targets []PrewarmTarget `toml:"target,omitempty" json:"target,omitempty"`
}

// Validate validates a single rule.
func (p *PrewarmRule) Validate() error {
	if p.ID == "" {
		return fmt.Errorf("id is mandatory")
	}
	if err := validateRepositorySlug(p.Repository); err != nil {
		return err
	}
	if p.Workflow == "" {
		return fmt.Errorf("workflow is mandatory")
	}
	if p.TriggerJob == "" {
		return fmt.Errorf("trigger_job is mandatory")
	}
	if action := p.Action(); action != prewarmTriggerActionQueued {
		return fmt.Errorf("invalid trigger_action %q; only %q is supported", action, prewarmTriggerActionQueued)
	}
	if p.RunAttempt < 0 {
		return fmt.Errorf("run_attempt must not be negative")
	}
	if _, err := p.TTL.ParseDuration(); err != nil {
		return fmt.Errorf("invalid ttl: %w", err)
	}
	if len(p.Targets) == 0 {
		return fmt.Errorf("at least one target is mandatory")
	}

	seenLabels := map[string]struct{}{}
	for idx := range p.Targets {
		target := &p.Targets[idx]
		if err := target.Validate(); err != nil {
			return fmt.Errorf("error validating target %d: %w", idx, err)
		}
		key := target.LabelKey()
		if _, ok := seenLabels[key]; ok {
			return fmt.Errorf("duplicate target label set [%s]", key)
		}
		seenLabels[key] = struct{}{}
	}

	return nil
}

// Action returns the trigger action, defaulting to "queued".
func (p *PrewarmRule) Action() string {
	if p.TriggerAction == "" {
		return prewarmTriggerActionQueued
	}
	return p.TriggerAction
}

// TotalTargetCount is the number of speculative runners a single match of this
// rule forecasts.
func (p *PrewarmRule) TotalTargetCount() uint {
	var total uint
	for _, target := range p.Targets {
		total += target.Count
	}
	return total
}

// Matches reports whether the rule applies to a queued trigger job. Every field
// is compared exactly so a profile can never route a forecast at a workflow it
// was not written for.
func (p *PrewarmRule) Matches(repository, workflow, jobName, action string, runAttempt int64) bool {
	if !strings.EqualFold(p.Repository, repository) {
		return false
	}
	if p.Workflow != workflow || p.TriggerJob != jobName || p.Action() != action {
		return false
	}
	return p.RunAttempt == 0 || p.RunAttempt == runAttempt
}

// PrewarmPreemption pre-acquires replacement capacity when a spot runner is
// about to be reclaimed.
//
// A preemption is the most expensive thing that can happen to a pull request:
// the job dies part-way through, and its retry starts from zero on a cold
// runner. On the longest path that lands directly on the wall-clock time to a
// green build, and unlike an ordinary fanout there is no gate job ahead of it
// to prewarm from — by the time GitHub queues the retry, the boot has to
// happen.
//
// But the runner knows first. The cloud gives it a preemption notice before the
// machine goes away, and the runner reports that notice to GARM, which starts
// the replacement immediately. By the time the retry is queued the runner is
// warm, and the claim path hands it over exactly as it would a forecast runner.
//
// The zero value is disabled and a no-op on every code path.
type PrewarmPreemption struct {
	// Enable turns preemption replacement on.
	Enable bool `toml:"enable" json:"enable"`
	// TTL is how long an unclaimed replacement is kept. It defaults to
	// DefaultPrewarmPreemptionTTL, which is long enough to cover the rest of a
	// workflow run, because that is what stands between a preemption and the
	// retry it causes.
	TTL PrewarmTTL `toml:"ttl,omitempty" json:"ttl,omitempty"`
	// Replacements maps the label set of a preempted runner to the label set
	// its retry will ask for. A fleet that reruns onto standard twins maps
	// each spot label to its twin; a fleet without twins maps a label to
	// itself.
	Replacements []PrewarmReplacement `toml:"replacement,omitempty" json:"replacement,omitempty"`
}

// Duration returns the configured replacement TTL, or the default if unset.
func (p *PrewarmPreemption) Duration() time.Duration {
	return p.TTL.DurationOr(DefaultPrewarmPreemptionTTL)
}

// Validate validates the preemption configuration. Like the rules, it is
// validated even while disabled so a typo surfaces when it is written.
func (p *PrewarmPreemption) Validate() error {
	if _, err := p.TTL.ParseDuration(); err != nil {
		return fmt.Errorf("invalid ttl: %w", err)
	}

	seenFrom := map[string]struct{}{}
	for idx := range p.Replacements {
		replacement := &p.Replacements[idx]
		if err := replacement.Validate(); err != nil {
			return fmt.Errorf("error validating replacement %d: %w", idx, err)
		}
		key := replacement.FromKey()
		if _, ok := seenFrom[key]; ok {
			return fmt.Errorf("duplicate replacement for label set [%s]", key)
		}
		seenFrom[key] = struct{}{}
	}

	if p.Enable && len(p.Replacements) == 0 {
		return fmt.Errorf("preemption is enabled but no replacement is defined")
	}
	return nil
}

// ReplacementFor returns the label set to pre-acquire for a preempted runner
// that carried the given labels. Labels are compared order- and
// case-insensitively, the same way a target's label set is.
func (p *PrewarmPreemption) ReplacementFor(labels []string) ([]string, bool) {
	if !p.Enable {
		return nil, false
	}
	key := NormalizeLabelKey(labels)
	if key == "" {
		return nil, false
	}
	for idx := range p.Replacements {
		if p.Replacements[idx].FromKey() == key {
			return p.Replacements[idx].To, true
		}
	}
	return nil, false
}

// PrewarmReplacement maps a preempted runner's label set to the one its retry
// will request.
type PrewarmReplacement struct {
	// From is the label set of the runner that was preempted.
	From []string `toml:"from" json:"from"`
	// To is the label set the retry will request. Often the standard twin of
	// From, because a rerun is routed away from spot.
	To []string `toml:"to" json:"to"`
}

// Validate validates a single replacement mapping.
func (p *PrewarmReplacement) Validate() error {
	if err := validateLabelSet(p.From); err != nil {
		return fmt.Errorf("invalid from: %w", err)
	}
	if err := validateLabelSet(p.To); err != nil {
		return fmt.Errorf("invalid to: %w", err)
	}
	return nil
}

// FromKey is the canonical identity of the preempted label set.
func (p *PrewarmReplacement) FromKey() string {
	return NormalizeLabelKey(p.From)
}

// PrewarmTarget is one pool's worth of forecast demand, addressed by the label
// set the downstream jobs will request.
type PrewarmTarget struct {
	// Labels is the exact set of labels a downstream job requests. It must
	// resolve to exactly one enabled pool or scale set for the entity.
	Labels []string `toml:"labels" json:"labels"`
	// Count is how many runners with this label set the fanout is expected to
	// need.
	Count uint `toml:"count" json:"count"`
}

// Validate validates a single target.
func (p *PrewarmTarget) Validate() error {
	if err := validateLabelSet(p.Labels); err != nil {
		return err
	}
	if p.Count == 0 {
		return fmt.Errorf("count must be greater than 0")
	}
	return nil
}

func validateLabelSet(labels []string) error {
	if len(labels) == 0 {
		return fmt.Errorf("at least one label is mandatory")
	}
	for _, label := range labels {
		if strings.TrimSpace(label) == "" {
			return fmt.Errorf("labels must not be empty")
		}
	}
	return nil
}

// LabelKey is the stable identity of a target's label set. Labels are sorted
// and lowercased so that a reordered profile addresses the same target and
// cannot create a second cohort for the same pool.
func (p *PrewarmTarget) LabelKey() string {
	return NormalizeLabelKey(p.Labels)
}

// NormalizeLabelKey builds the canonical key for a set of runner labels.
// GitHub label matching is case insensitive and order independent, so the key
// must be too.
func NormalizeLabelKey(labels []string) string {
	normalized := make([]string, 0, len(labels))
	for _, label := range labels {
		trimmed := strings.TrimSpace(label)
		if trimmed == "" {
			continue
		}
		normalized = append(normalized, strings.ToLower(trimmed))
	}
	slices.Sort(normalized)
	return strings.Join(slices.Compact(normalized), ",")
}

func validateRepositorySlug(slug string) error {
	if slug == "" {
		return fmt.Errorf("repository is mandatory")
	}
	owner, name, ok := strings.Cut(slug, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return fmt.Errorf("repository %q must be in owner/name form", slug)
	}
	return nil
}

// PrewarmTTL is a duration expressed as a TOML string, following the same
// pattern as the JWT time_to_live setting.
type PrewarmTTL string

// ParseDuration parses the TTL. An unset TTL is valid and parses to zero.
func (d *PrewarmTTL) ParseDuration() (time.Duration, error) {
	if *d == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(string(*d))
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, fmt.Errorf("duration %q must be positive", string(*d))
	}
	return duration, nil
}

// DurationOr returns the parsed duration, falling back to fallback when the
// TTL is unset or unparsable.
func (d *PrewarmTTL) DurationOr(fallback time.Duration) time.Duration {
	duration, err := d.ParseDuration()
	if err != nil {
		slog.With(slog.Any("error", err)).Error("failed to parse prewarm ttl")
		return fallback
	}
	if duration == 0 {
		return fallback
	}
	return duration
}

// UnmarshalText validates the duration as the TOML document is decoded, so a
// typo is reported against the offending line instead of silently becoming a
// zero TTL.
func (d *PrewarmTTL) UnmarshalText(text []byte) error {
	if len(text) > 0 {
		if _, err := time.ParseDuration(string(text)); err != nil {
			return fmt.Errorf("invalid duration: %w", err)
		}
	}
	*d = PrewarmTTL(text)
	return nil
}
