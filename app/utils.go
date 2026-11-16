package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func configure() {
	if len(config.replicaofHost) == 0 {
		config.role = "master"
		config.replid = randReplid()
	} else {
		config.role = "slave"
		replicaofArgs := strings.Split(config.replicaofHost, " ")

		switch len(replicaofArgs) {
		case 1:
			config.replicaofPort = 6379
		case 2:
			config.replicaofPort, _ = strconv.Atoi(replicaofArgs[1])
		default:
			flag.Usage()
			os.Exit(1)
		}
		config.replicaofHost = replicaofArgs[0]
	}
}

func connect() {
	listener, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", strconv.Itoa(config.port)))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", config.port)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Printf("Listening on: %s\n", listener.Addr().String())

	for id := 1; ; id++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			os.Exit(1)
		}
		go serveClient(id, conn)
	}
}

func randReplid() string {
	const chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	result := make([]byte, 40)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func serveClient(id int, conn net.Conn) {
	defer conn.Close()
	ctx := &ClientContext{id: id, conn: conn}

	reader := bufio.NewReader(conn)

	for {
		cmd := []string{}
		var arrSize, strSize int

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			token := strings.TrimRight(line, "\r\n")
			cmd, arrSize, strSize = parseCommands(token, arrSize, strSize, cmd)

			if arrSize == 0 {
				break
			}
		}

		if len(cmd) == 0 {
			break
		}

		response, resynch := handleCommand(ctx, cmd, 0)

		_, err := conn.Write([]byte(response))
		if err != nil {
			break
		}

		if resynch {
			sendRDB(conn)
			replicaMu.Lock()
			replicas = append(replicas, replica{conn: conn, offset: 0})
			replicaMu.Unlock()
		}
	}
}

func handleWait(count, timeout int) string {
	getAckCmd := []byte(encodeStringArray([]string{"REPLCONF", "GETACK", "*"}))

	replicaMu.Lock()
	snapshot := make([]replica, len(replicas))
	copy(snapshot, replicas)
	replicaMu.Unlock()

	acks := 0

	for i := 0; i < len(snapshot); i++ {
		if snapshot[i].offset > 0 {
			bytesWritten, err := snapshot[i].conn.Write(getAckCmd)
			if err == nil {
				replicaMu.Lock()
				for j := range replicas {
					if replicas[j].conn == snapshot[i].conn {
						replicas[j].offset += bytesWritten
						break
					}
				}
				replicaMu.Unlock()

				go func(conn net.Conn) {
					buffer := make([]byte, 1024)
					_, err := conn.Read(buffer)
					if err == nil {
						select {
						case ackReceived <- true:
						default:
						}
					}
				}(snapshot[i].conn)
			}
		} else {
			acks++
		}
	}

	timer := time.After(time.Duration(timeout) * time.Millisecond)

outer:
	for acks < count {
		select {
		case <-ackReceived:
			acks++
		case <-timer:
			break outer
		}
	}

	return encodeInteger(acks)
}
