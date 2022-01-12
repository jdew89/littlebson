package main

import "testing"

func TestBytesToInt32(t *testing.T) {
	testBytes := []byte{255, 0, 0, 0}
	result := readInt32Value(testBytes[:], 0)
	if result != 255 {
		t.Errorf("Int32 was incorrect: got %d, want %d", result, 255)
	}
}
