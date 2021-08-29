package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Athing struct {
	Test    string
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
}

func main() {

	//something := Athing{"Howedy", -1, 2000, 32134, true, nil, 12.34}
	something := Blarg{"Howedy", 1234}

	fmt.Printf("%+v\n", something)
	//typeofstruct(something)

	//fmt.Println(reflect.TypeOf(something.Name))

	//writedata := buildDocumentBytes(something)
	//fmt.Println(writedata)
	//writeBSON(writedata[:])
	//readBSON2()
	val := reflect.ValueOf(readBSON2()).Elem()
	fmt.Println("<<<<<<<< BACK IN MAIN >>>>>>>")
	fmt.Println(val.Interface())
	fmt.Println(val.NumField())
	//fmt.Println(val.Type().Field(0).Name)
	//fmt.Println(val.FieldByName(val.Type().Field(0).Name))

	//writedata := buildDocumentBytes(val.Interface())
	//writeBSON(writedata[:])

	//int64ToBytes(256)
}

func typeofstruct(x interface{}) {
	s := reflect.ValueOf(x)

	typeOfStruct := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		fmt.Printf("%d: %s %s = %v\n", i, typeOfStruct.Field(i).Name, f.Type(), f.Interface())

		if s.Field(i).Kind() == reflect.Int64 {
			fmt.Println("ITS AN INT64")
		} else {

			fmt.Println("ITS NOT AN INT")
		}
	}
}

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

func readBSON2() interface{} {
	/*dat, err := ioutil.ReadFile("data.db")
	check(err)
	fmt.Printf("%x\n", dat)*/

	f, err := os.Open("data.db")
	check(err)
	defer f.Close()

	reader := bufio.NewReader(f)
	docLenBytes := make([]byte, 4)
	docLenBytes, err = reader.Peek(4) //gets the first document length
	docLen := bytesToInt32(docLenBytes[:])
	fmt.Println("doc length: ", docLenBytes)
	fmt.Println("doc len: ", docLen)

	docBytes := make([]byte, docLen)
	_, err = io.ReadFull(reader, docBytes)
	check(err)

	//start pointer past the document size
	var p int32
	p = 4

	type store_values struct {
		FieldName     string
		FieldTypeByte byte
		//ValBytes []byte //could change this to an interface, so I can keep most of my code???
		FieldValue interface{}
	}

	//working here. store all the values in a map of this struct.
	//Then iterate through the map (using a range) and create the reflect struct and add the values

	field_num := -1
	doc_map := make(map[int]store_values)

	for p < docLen {
		thetypebyte := docBytes[p]
		//fmt.Println("byte type:", thetypebyte, " p: ", p, " fieldnum:", field_num)
		fmt.Println("type:", BSONType(thetypebyte))
		//if the type byte is null, move the pointer to the end of document and terminate loop
		if thetypebyte == 0x00 {
			p += 1
			fmt.Println("found null byte, p:", p)
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
	//document.Field(field_num).SetString(reflect.ValueOf(doc_map[0].FieldValue).String())
	for key, doc := range doc_map {
		setDocumentFieldValue(&document, doc.FieldValue, doc.FieldTypeByte, key)
		//document.Field((key)).SetString(doc.FieldValue)
	}

	return document.Addr().Interface()

	//offset := 0 //tracks the offset in file
	//b1, err := f.Read(docLenBytes)
	//check(err)
}

func setDocumentFieldValue(document *reflect.Value, field_value interface{}, typebyte byte, field_num int) {
	switch typebyte {
	case 0x01:
		//return reflect.TypeOf(float64(0))
	case 0x02:
		document.Field(field_num).SetString(reflect.ValueOf(field_value).String())
	case 0x10:
		//return reflect.TypeOf(int32(0))
	case 0x08:
		//return reflect.TypeOf(true)
	case 0x0A:
	//var i interface{}
	//return reflect.TypeOf(i)
	case 0x11: //timestamp
		//return reflect.TypeOf(uint64(0))
	case 0x12:
		document.Field(field_num).SetInt(reflect.ValueOf(field_value).Int())
	}
}

//determins what type to read from bytes. Reads the bytes and moves the pointer to after the value
//returns the value
func readFieldValue(typebyte byte, doc_bytes []byte, p *int32) interface{} {
	switch typebyte {
	case 0x01:
		//return reflect.TypeOf(float64(0))
	case 0x02:
		fieldvalue := readStringValue(doc_bytes[:], p)
		return *fieldvalue
	case 0x10:
		//return reflect.TypeOf(int32(0))
	case 0x08:
		//return reflect.TypeOf(true)
	case 0x0A:
		//var i interface{}
		//return reflect.TypeOf(i)
	case 0x11: //timestamp
		//return reflect.TypeOf(uint64(0))
	case 0x12:
		fmt.Println("p:", *p)
		fieldvalue := readInt64Value(doc_bytes[:], p)
		return fieldvalue
	}
	panic("Cannot read field value.")
}

//reads a int64 value
//pass the docbytes slice and array pointer
//returns the int64 value and pointer location after the string
func readInt64Value(doc_bytes []byte, p *int32) int64 {
	int_val := bytesToInt64(doc_bytes[*p : *p+8])

	*p += 8

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

	field_string := string(doc_bytes[*p : *p+str_len])
	*p = *p + str_len
	//fmt.Println("field str:", field_string, " p: ", *p)

	return &field_string
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
		var i interface{}
		return reflect.TypeOf(i)
	case 0x11: //timestamp
		return reflect.TypeOf(uint64(0))
	case 0x12:
		return reflect.TypeOf(int64(0))
	}
	panic("Invalid type for BSON field.")
	//return nil
}

//this is a testing func for creating a reflect struct
func readBSON() interface{} {
	typ := reflect.StructOf([]reflect.StructField{
		{
			Name: "Name",
			Type: reflect.TypeOf(string("")),
		},
		{
			Name: "Num",
			Type: reflect.TypeOf(int64(0)),
		},
	})

	v := reflect.New(typ).Elem()
	v.Field((0)).SetString("TEST")
	v.Field((1)).SetInt(123)

	return v.Addr().Interface()
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

func bytesToInt32(b []byte) int32 {
	var data int32
	data = 0
	for p := len(b) - 1; p >= 0; p-- {
		data = data << 8
		data += int32(b[p])
	}

	return data
}

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
