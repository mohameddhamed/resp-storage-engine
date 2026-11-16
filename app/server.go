package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type replica struct {
	conn   net.Conn
	offset int
}

type ClientContext struct {
	id         int
	conn       net.Conn
	inMulti    bool
	queuedCmds [][]string
}

type serverConfig struct {
	port          int
	role          string
	replid        string
	replOffset    int
	replicaofHost string
	replicaofPort int
	dir           string
	dbFileName    string
}

var (
	ackReceived chan bool

	storeMu sync.RWMutex
	store   map[string]string
	ttl     map[string]time.Time

	replicaMu sync.Mutex
	replicas  []replica

	config serverConfig
)

func main() {
	flag.IntVar(&config.port, "port", 6379, "listen on specified port")
	flag.StringVar(&config.replicaofHost, "replicaof", "", "start server in replica mode of given host and port")
	flag.StringVar(&config.dir, "dir", "", "the path to the directory where the RDB file is stored")
	flag.StringVar(&config.dbFileName, "dbfilename", "", "the name of the RDB file ")

	flag.Parse()

	store = make(map[string]string)
	ttl = make(map[string]time.Time)
	ackReceived = make(chan bool)

	configure()

	if config.role == "slave" {
		masterConn, err := net.Dial("tcp", net.JoinHostPort(config.replicaofHost, strconv.Itoa(config.replicaofPort)))
		if err != nil {
			fmt.Printf("Failed to connect to master %v\n", err)
			os.Exit(1)
		}
		defer masterConn.Close()

		reader := handshake(masterConn)
		receiveRDB(reader)
		totalProcessedBytes := 0

		go handlePropagation(reader, masterConn, totalProcessedBytes)
	}

	if len(config.dir) > 0 && len(config.dbFileName) > 0 {
		rdbPath := filepath.Join(config.dir, config.dbFileName)
		err := readRDB(rdbPath)
		if err != nil {
			fmt.Printf("Failed to load '%s': %v\n", rdbPath, err)
		}
	}

	connect()
}

func handleCommand(ctx *ClientContext, cmd []string, byteCount int) (response string, resynch bool) {
	if len(cmd) == 0 {
		return "", false
	}

	cmdName := strings.ToUpper(cmd[0])

	// Handle transactional state if client is in MULTI
	if ctx != nil && ctx.inMulti {
		switch cmdName {
		case "EXEC":
			queued := ctx.queuedCmds
			ctx.inMulti = false
			ctx.queuedCmds = nil

			if len(queued) == 0 {
				return "*0\r\n", false
			}

			var b strings.Builder
			b.WriteString("*" + strconv.Itoa(len(queued)) + "\r\n")
			for _, queuedCmd := range queued {
				subResp, _ := handleCommand(ctx, queuedCmd, 0)
				b.WriteString(subResp)
			}
			return b.String(), false

		case "DISCARD":
			ctx.inMulti = false
			ctx.queuedCmds = nil
			return "+OK\r\n", false

		default:
			ctx.queuedCmds = append(ctx.queuedCmds, cmd)
			return "+QUEUED\r\n", false
		}
	}

	isWrite := false

	switch cmdName {
	case "COMMAND":
		response = "+OK\r\n"

	case "REPLCONF":
		if len(cmd) >= 2 {
			subCmd := strings.ToUpper(cmd[1])
			if subCmd == "GETACK" {
				response = encodeStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(byteCount)})
			} else if config.role == "master" && subCmd == "ACK" {
				select {
				case ackReceived <- true:
				default:
				}
				response = "+OK\r\n"
			} else {
				response = "+OK\r\n"
			}
		}

	case "PSYNC":
		if len(cmd) == 3 {
			response = fmt.Sprintf("+FULLRESYNC %s 0\r\n", config.replid)
			resynch = true
		}

	case "PING":
		response = "+PONG\r\n"

	case "ECHO":
		if len(cmd) > 1 {
			response = encodeBulkString(cmd[1])
		} else {
			response = encodeError("wrong number of arguments for 'echo' command")
		}

	case "INFO":
		if len(cmd) >= 2 && strings.ToUpper(cmd[1]) == "REPLICATION" {
			infoStr := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				config.role, config.replid, config.replOffset)
			response = encodeBulkString(infoStr)
		} else {
			response = encodeBulkString(fmt.Sprintf("role:%s", config.role))
		}

	case "MULTI":
		if ctx != nil {
			ctx.inMulti = true
			ctx.queuedCmds = nil
		}
		response = "+OK\r\n"

	case "EXEC":
		response = encodeError("EXEC without MULTI")

	case "DISCARD":
		response = encodeError("DISCARD without MULTI")

	case "SET":
		if len(cmd) < 3 {
			response = encodeError("wrong number of arguments for 'set' command")
			break
		}
		isWrite = true
		key, value := cmd[1], cmd[2]

		storeMu.Lock()
		store[key] = value
		if len(cmd) >= 5 && strings.ToUpper(cmd[3]) == "PX" {
			expiration, _ := strconv.Atoi(cmd[4])
			ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
		} else {
			delete(ttl, key)
		}
		storeMu.Unlock()
		response = "+OK\r\n"

	case "GET":
		if len(cmd) < 2 {
			response = encodeError("wrong number of arguments for 'get' command")
			break
		}
		key := cmd[1]

		storeMu.RLock()
		value, ok := store[key]
		if !ok {
			storeMu.RUnlock()
			response = encodeNull()
			break
		}
		expiration, exists := ttl[key]
		if exists && time.Now().After(expiration) {
			storeMu.RUnlock()
			storeMu.Lock()
			if exp, stillExists := ttl[key]; stillExists && time.Now().After(exp) {
				delete(ttl, key)
				delete(store, key)
			}
			storeMu.Unlock()
			response = encodeNull()
			break
		}
		storeMu.RUnlock()
		response = encodeBulkString(value)

	case "INCR":
		if len(cmd) < 2 {
			response = encodeError("wrong number of arguments for 'incr' command")
			break
		}
		isWrite = true
		key := cmd[1]

		storeMu.Lock()
		value, ok := store[key]
		if ok {
			numValue, err := strconv.Atoi(value)
			if err != nil {
				storeMu.Unlock()
				response = encodeError("value is not an integer or out of range")
				break
			}
			numValue++
			store[key] = strconv.Itoa(numValue)
			storeMu.Unlock()
			response = encodeInteger(numValue)
		} else {
			store[key] = "1"
			delete(ttl, key)
			storeMu.Unlock()
			response = encodeInteger(1)
		}

	case "WAIT":
		numReplicas := 0
		timeout := 0
		if len(cmd) >= 2 {
			numReplicas, _ = strconv.Atoi(cmd[1])
		}
		if len(cmd) >= 3 {
			timeout, _ = strconv.Atoi(cmd[2])
		}
		response = handleWait(numReplicas, timeout)

	case "KEYS":
		storeMu.RLock()
		now := time.Now()
		keys := make([]string, 0, len(store))
		for key := range store {
			if exp, exists := ttl[key]; exists && now.After(exp) {
				continue
			}
			keys = append(keys, key)
		}
		storeMu.RUnlock()
		response = encodeStringArray(keys)

	case "CONFIG":
		if len(cmd) >= 3 && strings.ToLower(cmd[1]) == "get" {
			switch strings.ToLower(cmd[2]) {
			case "dir":
				response = encodeStringArray([]string{"dir", config.dir})
			case "dbfilename":
				response = encodeStringArray([]string{"dbfilename", config.dbFileName})
			default:
				response = encodeStringArray([]string{})
			}
		} else {
			response = encodeStringArray([]string{})
		}

	default:
		response = encodeError(fmt.Sprintf("unknown command '%s'", cmd[0]))
	}

	if isWrite {
		propagate(cmd)
	}

	return response, resynch
}
