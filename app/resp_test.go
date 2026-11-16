package main

import (
	"testing"
)

func TestEncodeBulkString(t *testing.T) {
	// Crucial RESP bug fix verification: empty string MUST encode to $0\r\n\r\n
	if got := encodeBulkString(""); got != "$0\r\n\r\n" {
		t.Fatalf("encodeBulkString(\"\") = %q; want \"$0\\r\\n\\r\\n\"", got)
	}

	if got := encodeBulkString("hello"); got != "$5\r\nhello\r\n" {
		t.Fatalf("encodeBulkString(\"hello\") = %q; want \"$5\\r\\nhello\\r\\n\"", got)
	}
}

func TestEncodeNull(t *testing.T) {
	if got := encodeNull(); got != "$-1\r\n" {
		t.Fatalf("encodeNull() = %q; want \"$-1\\r\\n\"", got)
	}
}

func TestEncodeInteger(t *testing.T) {
	if got := encodeInteger(42); got != ":42\r\n" {
		t.Fatalf("encodeInteger(42) = %q; want \":42\\r\\n\"", got)
	}
	if got := encodeInteger(0); got != ":0\r\n" {
		t.Fatalf("encodeInteger(0) = %q; want \":0\\r\\n\"", got)
	}
	if got := encodeInteger(-7); got != ":-7\r\n" {
		t.Fatalf("encodeInteger(-7) = %q; want \":-7\\r\\n\"", got)
	}
}

func TestEncodeStringArray(t *testing.T) {
	arr := []string{"foo", "bar"}
	expected := "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	if got := encodeStringArray(arr); got != expected {
		t.Fatalf("encodeStringArray(%v) = %q; want %q", arr, got, expected)
	}
}
