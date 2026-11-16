package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func handshake(masterConn net.Conn) *bufio.Reader {
	reader := bufio.NewReader(masterConn)
	masterConn.Write([]byte(encodeStringArray([]string{"PING"})))
	reader.ReadString('\n')
	masterConn.Write([]byte(encodeStringArray([]string{"REPLCONF", "listening-port", strconv.Itoa(config.port)})))
	reader.ReadString('\n')
	masterConn.Write([]byte(encodeStringArray([]string{"REPLCONF", "capa", "psync2"})))
	reader.ReadString('\n')
	masterConn.Write([]byte(encodeStringArray([]string{"PSYNC", "?", "-1"})))
	reader.ReadString('\n')
	return reader
}

func propagate(cmd []string) {
	replicaMu.Lock()
	defer replicaMu.Unlock()

	if len(replicas) == 0 {
		return
	}

	msg := []byte(encodeStringArray(cmd))
	var activeReplicas []replica

	for _, rep := range replicas {
		bytesWritten, err := rep.conn.Write(msg)
		if err != nil {
			fmt.Printf("Disconnected replica: %s (%v)\n", rep.conn.RemoteAddr().String(), err)
			rep.conn.Close()
			continue
		}
		rep.offset += bytesWritten
		activeReplicas = append(activeReplicas, rep)
	}

	replicas = activeReplicas
}

func handlePropagation(reader *bufio.Reader, masterConn net.Conn, totalProcessedBytes int) {
	slaveCtx := &ClientContext{id: 0, conn: masterConn}

	for {
		cmd := []string{}
		var arrSize, strSize int
		byteCount := 0

		for {
			token, err := reader.ReadString('\n')
			byteCount += len(token)

			if err != nil {
				return
			}
			token = strings.TrimRight(token, "\r\n")

			cmd, arrSize, strSize = parseCommands(token, arrSize, strSize, cmd)

			if arrSize == 0 {
				break
			}
		}

		if len(cmd) == 0 {
			break
		}

		response, _ := handleCommand(slaveCtx, cmd, totalProcessedBytes)
		totalProcessedBytes += byteCount

		if strings.ToUpper(cmd[0]) == "REPLCONF" {
			_, err := masterConn.Write([]byte(response))
			if err != nil {
				fmt.Printf("Error responding to master: %v\n", err.Error())
				break
			}
		}
	}
}
