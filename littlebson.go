package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"runtime"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Athing struct {
	TestStr string
	Num64   int64
	Num32   int32
	Uint64  uint64
	Boolean bool
	Blah    interface{}
	Float   float64
}

type Blarg struct {
	TestStr string
	Num64   int64
	Num32   int32
	Uint64  uint64
	Boolean bool
	Float   float64
}

type NullValue interface {
	null() interface{}
}

//needed to read null values
func null() interface{} {
	var n interface{}
	return n
}

func main() {
	//something := Athing{"Howedy", -1, 2000, 32134, true, nil, 12.34}
	something := Blarg{"Duuude", -100, 100, 1234, false, 56.91}

	daw := "test"

	fmt.Println(reflect.ValueOf(daw).String() == reflect.ValueOf("test").String())

	fmt.Printf("%+v\n", something)

	//fmt.Println(reflect.TypeOf(something.Name))

	writedata := buildDocumentBytes(something)
	//fmt.Println(writedata)
	//writeBSON(writedata[:])
	//return
	/*
		f, err := os.Open("data.db")
		check(err)
		defer f.Close()
		reader := bufio.NewReader(f)

		doc, err := readOneDocument(reader)
		check(err)

		val := reflect.ValueOf(doc).Elem()
		fmt.Println("<<<<<<<< BACK IN MAIN >>>>>>>")
		fmt.Println(val.Interface())
		//fmt.Println(val.NumField())

		doc, err = readOneDocument(reader)
		check(err)
		val = reflect.ValueOf(doc).Elem()
		fmt.Println("<<<<<<<< BACK IN MAIN >>>>>>>")
		fmt.Println(val.Interface())

		doc, err = readOneDocument(reader)
		if err != nil {
			fmt.Println("EOF")
		}
	*/

	//var i int64
	//i = -100
	//findOne("data", "Num64", i)
	doc, err := findOne("data", "Num64", -100)
	check(err)
	val := reflect.ValueOf(doc).Elem()
	fmt.Println(val.Interface())

}

//TODO - create a collection library which has this function and other searching/editing. Helps split this up
//finds first document by searching the fieldname for given value
//panics on bad collection name
//returns document, or error if no matches found
func findOne(collection_name string, field_name string, field_val interface{}) (interface{}, error) {
	f, err := os.Open(collection_name + ".db")
	check(err)
	defer f.Close()

	reader := bufio.NewReader(f)

	fmt.Println("Finding...", field_name, ": ", field_val)

	//this swtich converts ints to int64's
	//this is because golang converts int's to the underlying architecture.
	//If the architecture is 32, it will convert to int64 just fine.
	switch field_val.(type) {
	case int:
		val := int64(reflect.ValueOf(field_val).Interface().(int))
		field_val = val
	case uint:
		val := uint64(reflect.ValueOf(field_val).Interface().(uint))
		field_val = val
	}

	var doc interface{}
	found := false
	for !found {
		doc, err = readOneDocument(reader)
		if err != nil {
			break
		}

		doc_val := reflect.ValueOf(doc).Elem()
		/*
			fmt.Println(doc_val.Interface())
			fmt.Println("doc_val: ", doc_val.FieldByName(field_name))
			fmt.Println("find_val: ", reflect.ValueOf(field_val))
			fmt.Println("doc_type", doc_val.FieldByName(field_name).Type())
			fmt.Println("find_type", reflect.ValueOf(field_val).Type())
			fmt.Println("interfaces eq: ", doc_val.FieldByName(field_name).Interface() == reflect.ValueOf(field_val).Interface())
			//fmt.Println("string vals eq: ",doc_val.FieldByName(field_name).String() == reflect.ValueOf(field_val).String())
		*/
		found = doc_val.FieldByName(field_name).Interface() == reflect.ValueOf(field_val).Interface()
	}

	return doc, err
}

//Pass a struct into this func.
//It will build a BSON document from it and return the byte array.
func buildDocumentBytes(doc interface{}) []byte {
	docInterface := reflect.ValueOf(doc)
	docTypes := docInterface.Type() //used to get field names
	var data []byte

	for i := 0; i < docInterface.NumField(); i++ {
		field := docInterface.Field(i)
		switch field.Kind() {
		case reflect.String:
			data = append(data, uint8(0x02))                                   //var type - String
			data = append(data, []byte(docTypes.Field(i).Name)...)             //field name
			data = append(data, uint8(0))                                      //terminate the name string
			data = append(data, int32ToBytes(int32(len(field.String())+1))...) //add length of string value (add 1 for null terminator)
			data = append(data, []byte(field.String())...)                     //field value
			data = append(data, uint8(0))                                      //terminate the string
		case reflect.Int:
			//https://golang.org/doc/install/source#environment
			bit_32_list := [5]string{"386", "arm", "mipsle", "mips", "wasm"}
			is_32_bit := false
			for i := range bit_32_list {
				if runtime.GOARCH == bit_32_list[i] {
					data = append(data, uint8(0x10))                       //type of next var
					data = append(data, []byte(docTypes.Field(i).Name)...) //field name
					data = append(data, uint8(0))                          //terminate the string
					data = append(data, int32ToBytes(int32(field.Int()))...)
					is_32_bit = true
					break
				}
			}

			if !is_32_bit {
				data = append(data, uint8(0x12))                       //type of next var
				data = append(data, []byte(docTypes.Field(i).Name)...) //field name
				data = append(data, uint8(0))                          //terminate the string
				data = append(data, int64ToBytes(int64(field.Int()))...)
			}
		case reflect.Int64:
			data = append(data, uint8(0x12))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
			data = append(data, int64ToBytes(int64(field.Int()))...)
		case reflect.Int32:
			data = append(data, uint8(0x10))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
			data = append(data, int32ToBytes(int32(field.Int()))...)
		case reflect.Uint64: //timestamp
			data = append(data, uint8(0x11))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
			data = append(data, uint64ToBytes(uint64(field.Uint()))...)
		case reflect.Uint: //always 64-bit
			data = append(data, uint8(0x11))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
			data = append(data, uint64ToBytes(uint64(field.Uint()))...)
		case reflect.Bool:
			data = append(data, uint8(0x08))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
			data = append(data, boolToBytes(bool(field.Bool()))...)
		case reflect.Interface: //this is null case??
			data = append(data, uint8(0x0A))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
		case reflect.Float64:
			data = append(data, uint8(0x01))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
			data = append(data, float64ToBytes(float64(field.Float()))...)
		default:
			panic("Unknown data type.")
		}

	}
	data = append(data, uint8(0)) //terminate the document

	data = append(int32ToBytes(int32(len(data)+4)), data...) //append document size to front, adds the size of int32

	return data[:]

}

//pass a slice to this function for fastest speed
func writeBSON(data []byte) {
	file, err := os.OpenFile("data.db", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)

	//in_f, err := os.Create("data.db")
	check(err)

	defer file.Close()

	n1, err := file.Write(data)

	fmt.Printf("Wrote %d bytes\n", n1)
}

func readOneDocument(reader *bufio.Reader) (interface{}, error) {
	docLenBytes := make([]byte, 4)
	docLenBytes, err := reader.Peek(4) //gets the first document length
	docLen := bytesToInt32(docLenBytes[:])
	//fmt.Println("doc length: ", docLenBytes)
	//fmt.Println("doc len: ", docLen)

	if docLen < 4 {
		docLen = 4
	}

	docBytes := make([]byte, docLen)
	_, err = io.ReadFull(reader, docBytes)
	if err != nil {
		return nil, err
	}

	//start pointer past the document size
	var p int32
	p = 4

	type store_values struct {
		FieldName     string
		FieldTypeByte byte
		FieldValue    interface{}
		//ValBytes []byte //could change this to an interface, so I can keep most of my code???
	}

	//working here. store all the values in a map of this struct.
	//Then iterate through the map (using a range) and create the reflect struct and add the values

	field_num := -1
	doc_map := make(map[int]store_values)

	for p < docLen {
		thetypebyte := docBytes[p]
		//fmt.Println("byte type:", thetypebyte, " p: ", p, " fieldnum:", field_num)
		//fmt.Println("type:", BSONType(thetypebyte))
		//if the type byte is null, move the pointer to the end of document and terminate loop
		if thetypebyte == 0x00 {
			p += 1
			//fmt.Println("found null byte, p:", p)
			break
		}
		p += 1
		field_num += 1

		fieldname := readFieldName(docBytes[:], &p)

		//val_map[field_num] = store_values{thetypebyte, docBytes[]}
		field_val := readFieldValue(thetypebyte, docBytes[:], &p)

		doc_data := store_values{fieldname, thetypebyte, field_val}
		doc_map[field_num] = doc_data

		//fmt.Println("end of loop, p:", p, "len of doc_map:", len(doc_map))
	}

	struct_fields := make([]reflect.StructField, len(doc_map))
	//add fields to struct
	for k, doc := range doc_map {
		structfield := initStructField(doc.FieldName, doc.FieldTypeByte)
		struct_fields[k] = *structfield
	}

	base_document := reflect.StructOf(struct_fields[:])
	//var document reflect.Value
	document := reflect.New(base_document).Elem()

	//add field values to struct
	for key, doc := range doc_map {
		setDocumentFieldValue(&document, doc.FieldValue, doc.FieldTypeByte, key)
	}

	return document.Addr().Interface(), nil

	//offset := 0 //tracks the offset in file
	//b1, err := f.Read(docLenBytes)
	//check(err)
}

//sets the value of the given field with the appropiate type
func setDocumentFieldValue(document *reflect.Value, field_value interface{}, typebyte byte, field_num int) {
	switch typebyte {
	case 0x01:
		document.Field(field_num).SetFloat(reflect.ValueOf(field_value).Float())
	case 0x02:
		document.Field(field_num).SetString(reflect.ValueOf(field_value).String())
	case 0x10:
		document.Field(field_num).SetInt(reflect.ValueOf(field_value).Int())
	case 0x08:
		document.Field(field_num).SetBool(reflect.ValueOf(field_value).Bool())
	case 0x0A: //null
		//document.Field(field_num)
	case 0x11: //timestamp
		document.Field(field_num).SetUint(reflect.ValueOf(field_value).Uint())
	case 0x12:
		document.Field(field_num).SetInt(reflect.ValueOf(field_value).Int())
	}
}

//determins what type to read from bytes. Reads the bytes and moves the pointer to after the value
//returns the value
func readFieldValue(typebyte byte, doc_bytes []byte, p *int32) interface{} {
	switch typebyte {
	case 0x01:
		fieldvalue := readFloat64Value(doc_bytes[:], p)
		return fieldvalue
	case 0x02:
		fieldvalue := readStringValue(doc_bytes[:], p)
		return *fieldvalue
	case 0x10:
		fieldvalue := readInt32Value(doc_bytes[:], p)
		return fieldvalue
	case 0x08:
		fieldvalue := readBoolValue(doc_bytes[:], p)
		return fieldvalue
		//return reflect.TypeOf(true)
	case 0x0A:
		return nil
		//var i interface{}
		//return reflect.TypeOf(i)
	case 0x11: //timestamp
		fieldvalue := readUint64Value(doc_bytes[:], p)
		return fieldvalue
	case 0x12:
		fieldvalue := readInt64Value(doc_bytes[:], p)
		return fieldvalue
	}
	panic("Cannot read field value.")
}

//pass name of struct and the type byte
//returns a reflect structfield
func initStructField(name string, typebyte byte) *reflect.StructField {
	structfield := reflect.StructField{
		Name: name,
		Type: BSONType(typebyte),
	}

	return &structfield
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

//pass the byte type in the BSON
func BSONType(b byte) reflect.Type {
	switch b {
	case 0x00:
		return nil
	case 0x01:
		return reflect.TypeOf(float64(0))
	case 0x02:
		return reflect.TypeOf(string(""))
	case 0x10:
		return reflect.TypeOf(int32(0))
	case 0x08:
		return reflect.TypeOf(true)
	case 0x0A:
		//must return a closure that returns nil - otherwise reflect sees no type and is invalid
		return reflect.TypeOf(func() interface{} { return nil })
	case 0x11: //timestamp
		return reflect.TypeOf(uint64(0))
	case 0x12:
		return reflect.TypeOf(int64(0))
	}
	panic("Invalid type for BSON field.")
	//return nil
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
