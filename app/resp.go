package main

import (
	"strconv"
	"strings"
)

func parseCommands(token string, arrSize int, strSize int, cmd []string) ([]string, int, int) {
	if len(token) == 0 {
		return cmd, arrSize, strSize
	}

	switch token[0] {
	case '*':
		arrSize, _ = strconv.Atoi(token[1:])
	case '$':
		strSize, _ = strconv.Atoi(token[1:])
	default:
		if len(token) != strSize {
			break
		}
		arrSize--
		strSize = 0
		cmd = append(cmd, token)
	}
	return cmd, arrSize, strSize
}

// encodeBulkString encodes a bulk string in RESP2 format: $<len>\r\n<data>\r\n
// An empty string is encoded as "$0\r\n\r\n".
func encodeBulkString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 16)
	b.WriteByte('$')
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteString("\r\n")
	b.WriteString(s)
	b.WriteString("\r\n")
	return b.String()
}

// encodeNull encodes a null bulk string in RESP2 format: "$-1\r\n"
func encodeNull() string {
	return "$-1\r\n"
}

// encodeSimpleString encodes a simple status string: +<message>\r\n
func encodeSimpleString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 3)
	b.WriteByte('+')
	b.WriteString(s)
	b.WriteString("\r\n")
	return b.String()
}

// encodeInteger encodes an integer: :<number>\r\n
func encodeInteger(number int) string {
	var b strings.Builder
	b.Grow(16)
	b.WriteByte(':')
	b.WriteString(strconv.Itoa(number))
	b.WriteString("\r\n")
	return b.String()
}

// encodeError encodes a RESP error string: -ERR <message>\r\n
func encodeError(message string) string {
	var b strings.Builder
	b.Grow(len(message) + 8)
	b.WriteString("-ERR ")
	b.WriteString(message)
	b.WriteString("\r\n")
	return b.String()
}

// encodeStringArray encodes an array of bulk strings in RESP2: *<len>\r\n$<l1>\r\n<s1>...
func encodeStringArray(arr []string) string {
	var b strings.Builder
	b.Grow(32 + len(arr)*32)
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(arr)))
	b.WriteString("\r\n")
	for _, s := range arr {
		b.WriteString(encodeBulkString(s))
	}
	return b.String()
}
