package main

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func initTestStore() {
	storeMu.Lock()
	store = make(map[string]string)
	ttl = make(map[string]time.Time)
	storeMu.Unlock()
}

func TestConcurrentStoreAccess(t *testing.T) {
	initTestStore()

	var wg sync.WaitGroup
	numWorkers := 30
	iterations := 100

	// Concurrent SET workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ctx := &ClientContext{id: workerID}
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("key_%d", (workerID*iterations+i)%20)
				val := strconv.Itoa(i)
				resp, _ := handleCommand(ctx, []string{"SET", key, val}, 0)
				if resp != "+OK\r\n" {
					t.Errorf("SET failed: %s", resp)
				}
			}
		}(w)
	}

	// Concurrent GET workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ctx := &ClientContext{id: 1000 + workerID}
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("key_%d", (workerID*iterations+i)%20)
				handleCommand(ctx, []string{"GET", key}, 0)
			}
		}(w)
	}

	// Concurrent INCR workers
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ctx := &ClientContext{id: 2000 + workerID}
			for i := 0; i < iterations; i++ {
				handleCommand(ctx, []string{"INCR", "counter"}, 0)
			}
		}(w)
	}

	wg.Wait()
}

func TestTransactionIsolation(t *testing.T) {
	initTestStore()

	client1 := &ClientContext{id: 1}
	client2 := &ClientContext{id: 2}

	// Client 1 begins MULTI
	resp, _ := handleCommand(client1, []string{"MULTI"}, 0)
	if resp != "+OK\r\n" {
		t.Fatalf("MULTI returned: %s", resp)
	}

	// Client 1 queues SET
	resp, _ = handleCommand(client1, []string{"SET", "tx_key", "tx_val"}, 0)
	if resp != "+QUEUED\r\n" {
		t.Fatalf("SET in MULTI returned: %s", resp)
	}

	// Client 2 should NOT see queued command or transaction state
	resp, _ = handleCommand(client2, []string{"GET", "tx_key"}, 0)
	if resp != "$-1\r\n" {
		t.Fatalf("Client 2 saw uncommitted transaction state: %s", resp)
	}

	// Client 1 executes EXEC
	resp, _ = handleCommand(client1, []string{"EXEC"}, 0)
	expectedExec := "*1\r\n+OK\r\n"
	if resp != expectedExec {
		t.Fatalf("EXEC returned %q; want %q", resp, expectedExec)
	}

	// Client 2 now sees committed value
	resp, _ = handleCommand(client2, []string{"GET", "tx_key"}, 0)
	if resp != "$6\r\ntx_val\r\n" {
		t.Fatalf("Client 2 expected tx_val, got: %s", resp)
	}
}

func TestEmptyStringVsNull(t *testing.T) {
	initTestStore()
	ctx := &ClientContext{id: 1}

	// 1. Missing key must return null ($-1\r\n)
	resp, _ := handleCommand(ctx, []string{"GET", "nonexistent"}, 0)
	if resp != "$-1\r\n" {
		t.Fatalf("GET nonexistent = %q; want \"$-1\\r\\n\"", resp)
	}

	// 2. Setting key to empty string ""
	resp, _ = handleCommand(ctx, []string{"SET", "empty_key", ""}, 0)
	if resp != "+OK\r\n" {
		t.Fatalf("SET empty_key = %q", resp)
	}

	// 3. GET empty_key must return empty bulk string ($0\r\n\r\n), NOT null ($-1\r\n)
	resp, _ = handleCommand(ctx, []string{"GET", "empty_key"}, 0)
	if resp != "$0\r\n\r\n" {
		t.Fatalf("GET empty_key = %q; want \"$0\\r\\n\\r\\n\"", resp)
	}
}

func TestTTLRaceAndExpiration(t *testing.T) {
	initTestStore()
	ctx := &ClientContext{id: 1}

	// SET with 50ms TTL
	handleCommand(ctx, []string{"SET", "exp_key", "temporary", "px", "50"}, 0)

	// Immediate GET
	resp, _ := handleCommand(ctx, []string{"GET", "exp_key"}, 0)
	if resp != "$9\r\ntemporary\r\n" {
		t.Fatalf("Immediate GET failed, got: %s", resp)
	}

	// Wait for expiration
	time.Sleep(70 * time.Millisecond)

	// Post-expiration GET must return null
	resp, _ = handleCommand(ctx, []string{"GET", "exp_key"}, 0)
	if resp != "$-1\r\n" {
		t.Fatalf("Expired GET expected \"$-1\\r\\n\", got: %q", resp)
	}
}

func TestNestedMultiError(t *testing.T) {
	initTestStore()
	ctx := &ClientContext{id: 1}

	resp, _ := handleCommand(ctx, []string{"MULTI"}, 0)
	if resp != "+OK\r\n" {
		t.Fatalf("MULTI returned: %s", resp)
	}

	// Nested MULTI must return an error
	resp, _ = handleCommand(ctx, []string{"MULTI"}, 0)
	expectedErr := "-ERR MULTI calls can not be nested\r\n"
	if resp != expectedErr {
		t.Fatalf("Nested MULTI returned %q, want %q", resp, expectedErr)
	}
}
