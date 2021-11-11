package main

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strconv"
)

// This file has all of the functions related to binary reading/writing/converting

//TODO: could change this to accept a doc pointer rather than the object. Could save execution time.
//Pass a struct into this func.
//It will build a BSON document from it and return the byte array.
func buildDocumentBytes(doc interface{}) []byte {
	docInterface := reflect.ValueOf(doc)
	docTypes := docInterface.Type() //used to get field names
	var data []byte

	fmt.Println("in build docbytes")
	fmt.Println("type: ", docTypes)
	fmt.Println("kind: ", docInterface.Kind())
	fmt.Println("value: ", docInterface)
	if docInterface.Kind() == reflect.Slice || docInterface.Kind() == reflect.Array {
		fmt.Println("It's a: ", docInterface.Kind())
		interface_slice := docInterface.Interface().([]interface{})
		for i := 0; i < len(interface_slice); i++ {
			if reflect.ValueOf(interface_slice[i]).Kind() == reflect.String {
				data = append(data, STRING_TYPE)                                         //var type - String
				data = append(data, fieldNameBytes(strconv.Itoa(i))...)                  //converts the int to a string for the name
				data = append(data, generateStringBytes(interface_slice[i].(string))...) //add length of string value (add 1 for null terminator)
			}
			if reflect.ValueOf(interface_slice[i]).Kind() == reflect.Int {
				data = append(data, INT64_TYPE)                                       //var type
				data = append(data, fieldNameBytes(strconv.Itoa(i))...)               //converts the int to a string for the name
				data = append(data, int64ToBytes(int64(interface_slice[i].(int)))...) //add length of string value (add 1 for null terminator)
			}
			if reflect.ValueOf(interface_slice[i]).Kind() == reflect.Int64 {
				data = append(data, INT64_TYPE)                         //var type
				data = append(data, fieldNameBytes(strconv.Itoa(i))...) //converts the int to a string for the name
				data = append(data, int64ToBytes(interface_slice[i].(int64))...)
			}
		}

		data = endOfDocumentBytes(data[:])
		return data[:]
	}
	/*
		//since arrays are also documents, if this sees an array, build it like a document
		switch docTypes {
		case reflect.TypeOf(make([]uint8, 0)):

		case reflect.TypeOf(make([]string, 0)):
			str_slice := docInterface.Interface().([]string)

			for i := 0; i < len(str_slice); i++ {
				data = append(data, STRING_TYPE)                               //var type - String
				data = append(data, fieldNameBytes(strconv.Itoa(i))...)        //converts the int to a string for the name
				data = append(data, int32ToBytes(int32(len(str_slice[i])))...) //add length of string value (add 1 for null terminator)
				data = append(data, []byte(str_slice[i])...)                   //field value
				data = append(data, uint8(0))                                  //terminate the string
			}

			data = endOfDocumentBytes(data[:])
			return data[:]
		case reflect.TypeOf(make([]int64, 0)):
			str_slice := docInterface.Interface().([]string)

			for i := 0; i < len(str_slice); i++ {
				data = append(data, STRING_TYPE)                               //var type - String
				data = append(data, fieldNameBytes(strconv.Itoa(i))...)        //converts the int to a string for the name
				data = append(data, int32ToBytes(int32(len(str_slice[i])))...) //add length of string value (add 1 for null terminator)
				data = append(data, []byte(str_slice[i])...)                   //field value
				data = append(data, uint8(0))                                  //terminate the string
			}

			data = endOfDocumentBytes(data[:])
			return data[:]
		}*/

	//loops through fields of the struct
	for i := 0; i < docInterface.NumField(); i++ {
		field := docInterface.Field(i)

		switch field.Kind() {
		case reflect.String:
			data = append(data, STRING_TYPE) //var type - String
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
			data = append(data, generateStringBytes(field.String())...) //add length of string value (add 1 for null terminator)
		case reflect.Int:
			//https://golang.org/doc/install/source#environment
			bit_32_list := [5]string{"386", "arm", "mipsle", "mips", "wasm"}
			is_32_bit := false
			for i := range bit_32_list {
				fmt.Println(runtime.GOARCH, " == ", bit_32_list[i], " ", runtime.GOARCH == bit_32_list[i])
				if runtime.GOARCH == bit_32_list[i] {
					data = append(data, INT32_TYPE) //type of field
					data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
					data = append(data, int32ToBytes(int32(field.Int()))...)
					is_32_bit = true
					break
				}
			}

			if !is_32_bit {
				data = append(data, INT64_TYPE) //type of field
				data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
				data = append(data, int64ToBytes(int64(field.Int()))...)
			}
		case reflect.Int64:
			data = append(data, INT64_TYPE) //type of field
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
			data = append(data, int64ToBytes(int64(field.Int()))...)
		case reflect.Int32:
			data = append(data, INT32_TYPE) //type of field
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
			data = append(data, int32ToBytes(int32(field.Int()))...)
		case reflect.Uint64: //timestamp
			data = append(data, UINT64_TYPE) //type of field
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
			data = append(data, uint64ToBytes(uint64(field.Uint()))...)
		case reflect.Uint: //always 64-bit
			data = append(data, UINT64_TYPE) //type of field
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
			data = append(data, uint64ToBytes(uint64(field.Uint()))...)
		case reflect.Bool:
			data = append(data, BOOL_TYPE) //type of field
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
			data = append(data, boolToBytes(bool(field.Bool()))...)
		case reflect.Interface: //this is null case??
			data = append(data, NULL_TYPE) //type of field
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)

		case reflect.Float64:
			data = append(data, FLOAT64_TYPE) //type of field
			data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
			data = append(data, float64ToBytes(float64(field.Float()))...)
		case reflect.Slice: //all slices, including binary data
			//fmt.Println("in buildBytes: ", field.Type())
			switch field.Type() {
			case reflect.TypeOf(make([]uint8, 0)):
				//binary data
				data = append(data, BINARY_TYPE) //var type - binary data
				data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
				data = append(data, int32ToBytes(int32(len(field.Bytes())))...) //add length of binary value
				//TODO add function for subtypes
				data = append(data, uint8(0x00))              //Add the subtype
				data = append(data, []byte(field.Bytes())...) //field value
			//case reflect.TypeOf(make([]string, 0)):
			default: //default is all other array types
				data = append(data, ARRAY_TYPE) //type of field
				data = append(data, fieldNameBytes(docTypes.Field(i).Name)...)
				data = append(data, buildDocumentBytes(field.Interface())...)
			}

			//TODO do the same things as slices for Arrays
			//case reflect.Array:

		}
	}

	//data = append(data, uint8(0))                            //terminate the document
	//data = append(int32ToBytes(int32(len(data)+4)), data...) //append document size to front, adds the size of int32

	data = endOfDocumentBytes(data[:])

	return data[:]

}

//generates bytes for a string. Includes the length at the beginning and null value at the end.
func generateStringBytes(str string) []byte {
	data := make([]byte, 0)
	data = append(data, int32ToBytes(int32(len(str)+1))...) //add length of string value (add 1 for null terminator)
	data = append(data, []byte(str)...)                     //field value
	data = append(data, uint8(0))                           //terminate the string

	return data[:]
}

//returns the bytes used to finish a document including the total size of the document
func endOfDocumentBytes(data []byte) []byte {
	data = append(data, uint8(0))                            //terminate the document
	data = append(int32ToBytes(int32(len(data)+4)), data...) //append document size to front, adds the size of int32

	return data[:]
}

//return the bytes for the name of a field plus the null byte at the end
func fieldNameBytes(name string) []byte {
	data := []byte(name)
	data = append(data, uint8(0)) //terminate the name string

	return data[:]
}

//pass the doc_bytes slice and the pointer value
//gets the field name
//moved the pointer by reference
func readFieldName(doc_bytes []byte, p *int32) string {
	k := *p
	//finds the null byte after field name
	for doc_bytes[k] != byte(0x00) {
		k++
	}

	//fieldname, err := reader.ReadString(byte(0x00)) //null byte as delimiter
	fieldname := string(doc_bytes[*p:k])
	//fmt.Println("Field:", fieldname)

	//move pointer past null
	*p = k + 1

	return fieldname
}

/////////////////
// <<<<<<<<<<<<<< READS VALUES FROM BYTES
/////////////////

//reads a Bool value
//pass the docbytes slice and array pointer
//returns the Bool value and pointer location after the string
func readBoolValue(doc_bytes []byte, p *int32) bool {
	int_val := bytesToBool(doc_bytes[*p])

	*p += 1

	return int_val
}

//reads a Float64 value
//pass the docbytes slice and array pointer
//returns the Float64 value and pointer location after the string
func readFloat64Value(doc_bytes []byte, p *int32) float64 {
	int_val := bytesToFloat64(doc_bytes[*p : *p+8])

	*p += 8

	return int_val
}

//reads a int64 value
//pass the docbytes slice and array pointer
//returns the int64 value and pointer location after the string
func readUint64Value(doc_bytes []byte, p *int32) uint64 {
	int_val := bytesToUint64(doc_bytes[*p : *p+8])

	*p += 8

	return int_val
}

//reads a int64 value
//pass the docbytes slice and array pointer
//returns the int64 value and pointer location after the string
func readInt64Value(doc_bytes []byte, p *int32) int64 {
	int_val := bytesToInt64(doc_bytes[*p : *p+8])

	*p += 8

	return int_val
}

//reads a int32 value
//pass the docbytes slice and array pointer
//returns the int64 value and pointer location after the string
func readInt32Value(doc_bytes []byte, p *int32) int32 {
	int_val := bytesToInt32(doc_bytes[*p : *p+4])

	*p += 4

	return int_val
}

//reads a string value
//pass the docbytes slice and array pointer
//returns the string value
//moves the pointer by reference
func readStringValue(doc_bytes []byte, p *int32) *string {
	str_len := bytesToInt32(doc_bytes[*p : *p+4])
	*p = *p + 4

	//fmt.Println("str len:", str_len)

	field_string := string(doc_bytes[*p : *p+str_len-1]) //-1 for the null byte at the end
	*p = *p + str_len
	//fmt.Println("field str:", field_string, " p: ", *p)

	return &field_string
}

func readBinaryDataValue(doc_bytes []byte, p *int32) *[]byte {
	bytes_len := bytesToInt32(doc_bytes[*p : *p+4])
	*p = *p + 4

	// TODO: add logic for subtypes?
	//subtype := byte(doc_bytes[*p])
	*p = *p + 1
	/*switch subtype {
	case 0x00: //generic binary
		return doc_bytes[*p : *p+bytes_len]
	default:
	}*/

	byte_array := doc_bytes[*p : *p+bytes_len]
	*p = *p + bytes_len

	return &byte_array

}

///////////////
// <<<<<<<<<<<< BYTES TO TYPE
///////////////

func bytesToBool(b byte) bool {
	if b == 1 {
		return true
	} else if b == 0 {
		return false
	}
	panic("Boolean byte should be 0 or 1.")
}

func bytesToInt64(b []byte) int64 {
	var data int64
	data = 0
	for p := len(b) - 1; p >= 0; p-- {
		data = data << 8
		data += int64(b[p])
	}

	return data
}

func bytesToUint64(b []byte) uint64 {
	var data uint64
	data = 0
	for p := len(b) - 1; p >= 0; p-- {
		data = data << 8
		data += uint64(b[p])
	}

	return data
}

func bytesToInt32(b []byte) int32 {
	var data int32
	data = 0
	for p := len(b) - 1; p >= 0; p-- {
		data = data << 8
		data += int32(b[p])
	}

	return data
}

func bytesToFloat64(b []byte) float64 {
	bytes_as_uint := bytesToUint64(b[:])

	data := math.Float64frombits(bytes_as_uint)

	return data
}

///////////////
// <<<<<<<<<<<< TYPE TO BYTES
///////////////

//returns in little endian
func int64ToBytes(i int64) []byte {
	var data [8]byte

	for p := 0; p < 8; p++ {
		data[p] = uint8(i)
		i = i >> 8
	}

	return data[:]
}

//returns in little endian
func uint64ToBytes(i uint64) []byte {
	var data [8]byte

	for p := 0; p < 8; p++ {
		data[p] = uint8(i)
		i = i >> 8
	}

	return data[:]
}

//returns in little endian
func float64ToBytes(f float64) []byte {
	var data [8]byte

	i := math.Float64bits(f)

	for p := 0; p < 8; p++ {
		data[p] = uint8(i)
		i = i >> 8
	}

	return data[:]
}

//returns in little endian
func boolToBytes(i bool) []byte {
	var data [1]byte

	if i {
		data[0] = uint8(1)
	} else {
		data[0] = uint8(0)
	}

	return data[:]
}

//returns in little endian
func int32ToBytes(i int32) []byte {
	var data [4]byte

	for p := 0; p < 4; p++ {
		data[p] = uint8(i)
		i = i >> 8
	}

	return data[:]
}
