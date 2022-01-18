package main

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

func TestBytesToInt32(t *testing.T) {
	testBytes := []byte{255, 0, 0, 0}
	result := readInt32Value(testBytes[:], 0)
	if result != 255 {
		t.Errorf("Int32 was incorrect: got %d, want %d", result, 255)
	}
}

func TestReadDocumentValue(t *testing.T) {
	fmt.Println("Testing")

	//id := genLilBsonID()
	id := LilBsonID(10898982113560907787)

	type smallTestStruct struct {
		MyInt64  int64
		MyUint64 uint64
		MyInt32  int32
		MyFloat  float64
	}
	type testStruct struct {
		Id        LilBsonID
		HelloArr  []string
		Binary    []byte
		SubStruct smallTestStruct
		Null      interface{}
		True      bool
	}

	smallStruct := smallTestStruct{int64(-64), uint64(64), int32(-32), float64(64.64)}
	stringArr := []string{"hello", "world"}
	binaryArr := []byte{0x62, 0x69, 0x6E, 0x20, 0x61, 0x72, 0x72}
	var myNull interface{}

	initialStruct := testStruct{id, stringArr[:], binaryArr[:], smallStruct, myNull, true}
	fmt.Println("Building Bytes")
	writeBytes := buildDocumentBytes(initialStruct)
	fmt.Println("Done.")
	formattedBytes := hex.EncodeToString(writeBytes)
	fmt.Println("Bytes: ", formattedBytes)
	needed := "AC 00 00 00 07 49 64 00 0B 50 22 AC 7C F6 40 97 04 48 65 6C 6C 6F 41 72 72 00 1F 00 00 00 02 30 00 06 00 00 00 68 65 6C 6C 6F 00 02 31 00 06 00 00 00 77 6F 72 6C 64 00 00 05 42 69 6E 61 72 79 00 07 00 00 00 00 62 69 6E 20 61 72 72 03 53 75 62 53 74 72 75 63 74 00 46 00 00 00 12 4D 79 49 6E 74 36 34 00 C0 FF FF FF FF FF FF FF 11 4D 79 55 69 6E 74 36 34 00 40 00 00 00 00 00 00 00 10 4D 79 49 6E 74 33 32 00 E0 FF FF FF 01 4D 79 46 6C 6F 61 74 00 29 5C 8F C2 F5 28 50 40 00 0A 4E 75 6C 6C 00 08 54 72 75 65 00 01 00"
	needed = strings.ReplaceAll(needed, " ", "")
	needed = strings.ToLower(needed)
	fmt.Println("needed:", needed)
	fmt.Println("compare:", formattedBytes == needed)
	fmt.Println("Reading Bytes to Document")

	returnedDoc, docSize := readDocumentValue(writeBytes, 0)

	returnedDocInterface := returnedDoc.Interface()
	fmt.Println("DocSize: ", docSize)

	fmt.Println("Doc:", returnedDocInterface)

}
