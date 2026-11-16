package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "Target RESP server address")
	concurrency := flag.Int("c", 20, "Number of concurrent worker connections")
	requests := flag.Int("n", 50000, "Total number of requests to execute")
	commandType := flag.String("t", "mixed", "Benchmark workload: ping, set, get, mixed")
	flag.Parse()

	fmt.Printf("=== RESP Storage Engine Benchmark ===\n")
	fmt.Printf("Target: %s | Concurrency: %d | Total Requests: %d | Workload: %s\n\n",
		*addr, *concurrency, *requests, *commandType)

	// Test connection
	testConn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
	if err != nil {
		fmt.Printf("Error: cannot connect to server at %s: %v\n", *addr, err)
		fmt.Println("Please ensure the resp-storage-engine server is running.")
		os.Exit(1)
	}
	testConn.Close()

	reqsPerWorker := *requests / *concurrency
	var completed uint64

	latencies := make([][]time.Duration, *concurrency)
	var wg sync.WaitGroup

	startTime := time.Now()

	for workerID := 0; workerID < *concurrency; workerID++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", *addr)
			if err != nil {
				fmt.Printf("Worker %d failed to connect: %v\n", wID, err)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			workerLatencies := make([]time.Duration, 0, reqsPerWorker)

			for i := 0; i < reqsPerWorker; i++ {
				var cmd string
				key := fmt.Sprintf("bench_key_%d", (wID*reqsPerWorker+i)%1000)
				val := strconv.Itoa(i)

				switch *commandType {
				case "ping":
					cmd = "*1\r\n$4\r\nPING\r\n"
				case "set":
					cmd = fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
				case "get":
					cmd = fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
				default: // mixed (50% SET, 50% GET)
					if i%2 == 0 {
						cmd = fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
					} else {
						cmd = fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
					}
				}

				t0 := time.Now()
				_, writeErr := conn.Write([]byte(cmd))
				if writeErr != nil {
					break
				}

				// Read response
				line, readErr := reader.ReadString('\n')
				if readErr != nil {
					break
				}
				if len(line) > 0 && line[0] == '$' {
					length, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
					if length > 0 {
						body := make([]byte, length+2)
						reader.Read(body)
					}
				}
				workerLatencies = append(workerLatencies, time.Since(t0))
				atomic.AddUint64(&completed, 1)
			}
			latencies[wID] = workerLatencies
		}(workerID)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Consolidate latencies
	var allLatencies []time.Duration
	for _, wl := range latencies {
		allLatencies = append(allLatencies, wl...)
	}

	if len(allLatencies) == 0 {
		fmt.Println("No requests completed successfully.")
		return
	}

	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	opsPerSec := float64(len(allLatencies)) / duration.Seconds()
	p50 := allLatencies[len(allLatencies)*50/100]
	p90 := allLatencies[len(allLatencies)*90/100]
	p99 := allLatencies[len(allLatencies)*99/100]
	p999 := allLatencies[len(allLatencies)*999/1000]

	fmt.Printf("\n--- Results ---\n")
	fmt.Printf("Completed:    %d ops in %v\n", len(allLatencies), duration)
	fmt.Printf("Throughput:   %.2f ops/sec\n", opsPerSec)
	fmt.Printf("Min Latency:  %v\n", allLatencies[0])
	fmt.Printf("P50 Latency:  %v\n", p50)
	fmt.Printf("P90 Latency:  %v\n", p90)
	fmt.Printf("P99 Latency:  %v\n", p99)
	fmt.Printf("P99.9 Lat:    %v\n", p999)
	fmt.Printf("Max Latency:  %v\n", allLatencies[len(allLatencies)-1])
}
