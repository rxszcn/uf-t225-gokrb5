package service

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcmturner/gokrb5/v8/iana/nametype"
	"github.com/jcmturner/gokrb5/v8/types"
)

func testServiceName(name string) types.PrincipalName {
	return types.PrincipalName{
		NameType:   nametype.KRB_NT_PRINCIPAL,
		NameString: []string{"HTTP", name},
	}
}

// TestIsReplayCrossServicePresentation verifies that presenting the same authenticator material
// to two services in the same process does not overwrite the first service's record, so a replay
// to the service that saw the material first is still rejected.
func TestIsReplayCrossServicePresentation(t *testing.T) {
	t.Parallel()
	c := newCache(time.Minute)
	svcA := testServiceName("svc-a.example.test")
	svcB := testServiceName("svc-b.example.test")
	a := newTestAuthenticator(*getClient().Credentials)

	if c.IsReplay(svcA, a) {
		t.Fatal("first presentation to service A was reported as a replay")
	}
	if c.IsReplay(svcB, a) {
		t.Fatal("first presentation of the same material to service B was reported as a replay")
	}
	if !c.IsReplay(svcA, a) {
		t.Fatal("replay of the material to service A was not detected after it was presented to service B")
	}
	if !c.IsReplay(svcB, a) {
		t.Fatal("replay of the material to service B was not detected")
	}
}

// TestIsReplayConcurrentPresentation verifies that the check and the insert are atomic: when the
// same material is presented many times concurrently, exactly one presentation is accepted.
func TestIsReplayConcurrentPresentation(t *testing.T) {
	t.Parallel()
	c := newCache(time.Minute)
	svcA := testServiceName("svc-a.example.test")
	a := newTestAuthenticator(*getClient().Credentials)

	const n = 64
	var accepted int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			if !c.IsReplay(svcA, a) {
				atomic.AddInt32(&accepted, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&accepted); got != 1 {
		t.Fatalf("concurrent presentations of the same material were accepted %d times, want exactly 1", got)
	}
}

// TestIsReplayConcurrentCrossServicePresentation verifies the atomicity guarantee is per
// (client, service, time) combination: concurrent presentation of the same material to two
// services admits exactly one presentation per service.
func TestIsReplayConcurrentCrossServicePresentation(t *testing.T) {
	t.Parallel()
	c := newCache(time.Minute)
	svcs := []types.PrincipalName{
		testServiceName("svc-a.example.test"),
		testServiceName("svc-b.example.test"),
	}
	a := newTestAuthenticator(*getClient().Credentials)

	const perService = 32
	var accepted [2]int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(len(svcs) * perService)
	for i := 0; i < perService; i++ {
		for j := range svcs {
			svc, idx := svcs[j], j
			go func() {
				defer wg.Done()
				<-start
				if !c.IsReplay(svc, a) {
					atomic.AddInt32(&accepted[idx], 1)
				}
			}()
		}
	}
	close(start)
	wg.Wait()

	for i, svc := range svcs {
		if got := atomic.LoadInt32(&accepted[i]); got != 1 {
			t.Fatalf("concurrent presentations to %s were accepted %d times, want exactly 1", svc.PrincipalNameString(), got)
		}
	}
}

// TestCacheRetentionOnlyGrows verifies the singleton retention is the maximum duration any caller
// registered, so a later service with a shorter clock skew cannot evict records still needed by a
// service with a longer one.
func TestCacheRetentionOnlyGrows(t *testing.T) {
	t.Parallel()
	c := newCache(time.Minute)
	if got := c.currentRetention(); got != time.Minute {
		t.Fatalf("initial retention = %v, want %v", got, time.Minute)
	}
	c.setRetention(2 * time.Minute)
	if got := c.currentRetention(); got != 2*time.Minute {
		t.Fatalf("retention after extending = %v, want %v", got, 2*time.Minute)
	}
	c.setRetention(30 * time.Second)
	if got := c.currentRetention(); got != 2*time.Minute {
		t.Fatalf("retention after a shorter call = %v, want %v", got, 2*time.Minute)
	}
}
