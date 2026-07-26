// Copyright 2026 Cloudbase Solutions SRL
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

package scalesets

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"

	"github.com/cloudbase/garm/params"
)

func TestNewActionsRequestSnapshotsCredentialsUnderLock(t *testing.T) {
	const validJWT = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjQwNzA5MDg4MDB9.c2ln"
	registrationToken := "registration-token"
	credentialPairs := []params.ActionsServiceAdminInfoResponse{
		{URL: "https://actions-a.example/", Token: validJWT + "-a"},
		{URL: "https://actions-b.example/", Token: validJWT + "-b"},
	}
	expectedToken := map[string]string{
		"actions-a.example": credentialPairs[0].Token,
		"actions-b.example": credentialPairs[1].Token,
	}

	client := &ScaleSetClient{
		runnerRegistrationToken: &github.RegistrationToken{
			Token: &registrationToken,
			ExpiresAt: &github.Timestamp{
				Time: time.Now().UTC().Add(time.Hour),
			},
		},
		actionsServiceInfo: &credentialPairs[0],
	}

	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stopWriter:
				return
			default:
			}
			client.mux.Lock()
			client.actionsServiceInfo = &credentialPairs[i%len(credentialPairs)]
			client.mux.Unlock()
		}
	}()

	const callers = 16
	const requestsPerCaller = 250
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			for range requestsPerCaller {
				req, err := client.newActionsRequest(context.Background(), "GET", "runner", nil)
				if err != nil {
					t.Errorf("newActionsRequest() error = %v", err)
					return
				}
				want, ok := expectedToken[req.URL.Host]
				if !ok {
					t.Errorf("unexpected actions host %q", req.URL.Host)
					return
				}
				if got := req.Header.Get("Authorization"); got != fmt.Sprintf("Bearer %s", want) {
					t.Errorf("authorization = %q for host %q, want token from same snapshot", got, req.URL.Host)
					return
				}
			}
		}()
	}
	callersDone.Wait()
	close(stopWriter)
	<-writerDone
}
