// Copyright 2025 Cloudbase Solutions SRL
//
//	Licensed under the Apache License, Version 2.0 (the "License");
//	you may not use this file except in compliance with the License.
//	You may obtain a copy of the License at
//
//		http://www.apache.org/licenses/LICENSE-2.0
//
//	Unless required by applicable law or agreed to in writing, software
//	distributed under the License is distributed on an "AS IS" BASIS,
//	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//	See the License for the specific language governing permissions and
//	limitations under the License.
package scaleset

import (
	"fmt"
	"testing"

	"github.com/cloudbase/garm/params"
)

func TestOutstandingJobsFloorLaggingRunnerStatistic(t *testing.T) {
	listener := &scaleSetListener{outstandingJobs: make(map[string]struct{})}
	firstBatch := scaleSetJobs(1, 5)
	secondBatch := scaleSetJobs(6, 8)

	if got, want := listener.updateOutstandingJobs(5, firstBatch, nil, nil), 5; got != want {
		t.Fatalf("first desired runner count = %d, want %d", got, want)
	}
	if got, want := listener.updateOutstandingJobs(7, secondBatch, nil, nil), 8; got != want {
		t.Fatalf("lagging desired runner count = %d, want %d", got, want)
	}
}

func TestOutstandingJobsTracksStartedAndCompletedJobs(t *testing.T) {
	listener := &scaleSetListener{outstandingJobs: make(map[string]struct{})}
	jobs := scaleSetJobs(1, 3)
	listener.updateOutstandingJobs(3, jobs, nil, nil)

	if got, want := listener.updateOutstandingJobs(2, nil, jobs[:1], jobs[1:]), 2; got != want {
		t.Fatalf("desired runner count after completions = %d, want %d", got, want)
	}
	if got, want := len(listener.outstandingJobs), 1; got != want {
		t.Fatalf("outstanding job count = %d, want %d", got, want)
	}
}

func TestOutstandingJobsUsesHigherReportedStatistic(t *testing.T) {
	listener := &scaleSetListener{outstandingJobs: make(map[string]struct{})}

	if got, want := listener.updateOutstandingJobs(4, scaleSetJobs(1, 2), nil, nil), 4; got != want {
		t.Fatalf("desired runner count = %d, want %d", got, want)
	}
}

func scaleSetJobs(first, last int) []params.ScaleSetJobMessage {
	jobs := make([]params.ScaleSetJobMessage, 0, last-first+1)
	for id := first; id <= last; id++ {
		jobs = append(jobs, params.ScaleSetJobMessage{JobID: fmt.Sprintf("job-%d", id)})
	}
	return jobs
}
