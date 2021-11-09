package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"reflect"
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
	Binary  []byte
	StrArr  []string
}

type SearchDocument struct {
	Name  string
	Value interface{}
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
	mybytes := make([]byte, 2)
	mybytes[0] = 0x68
	mybytes[1] = 0x69

	mystringarr := make([]string, 3)
	mystringarr[0] = "hello "
	mystringarr[1] = "cruel "
	mystringarr[2] = "world!"

	/*test := reflect.ValueOf(mystringarr)
	fmt.Println(test)
	fmt.Println(test.Kind())
	fmt.Println(test.Type())
	fmt.Println(test.Type() == reflect.TypeOf(make([]string, 0)))*/

	//return

	//something := Athing{"Howedy", -1, 2000, 32134, true, nil, 12.34}
	something := Blarg{"Duuude", -100, 100, 1234, false, 56.91, mybytes[:], mystringarr[:]}
	//insertOne("data", mystringarr)
	insertOne("data", something)

	//var something Blarg

	for i := 0; i < 1000; i++ {
		//something = Blarg{"Duuude" + fmt.Sprint(i), int64(i), 100 + int32(i), 1000 + uint64(i), false, 56.91 + float64(i)}
		//insertOne("data", something)
	}

	//fmt.Printf("%+v\n", something)

	query := make([]SearchDocument, 3)
	query[0] = SearchDocument{"TestStr", "Duuude6"}
	query[1] = SearchDocument{"Num64", 6}
	query[2] = SearchDocument{"Num32", int32(106)}
	query[0] = SearchDocument{"TestStr", "Duuude"}
	query[1] = SearchDocument{"Num64", -100}
	query[2] = SearchDocument{"Num32", int32(100)}
	doc, err := findOne("data", query)
	if err == nil {
		val := reflect.ValueOf(doc).Elem()
		fmt.Println(val.Interface())
	} else {
		fmt.Println("Not found.")
	}
}

//pass a slice to this function for fastest speed
func writeBSON(file *os.File, data []byte) error {
	_, err := file.Write(data)

	//fmt.Printf("Wrote %d bytes\n", n1)
	return err
}

//reads 1 full document into memory and returns it as an interface
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
	case 0x05:
		document.Field(field_num).SetBytes(reflect.ValueOf(field_value).Bytes())
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
	case 0x05:
		fieldvalue := readBinaryDataValue(doc_bytes[:], p)
		return *fieldvalue
	case 0x08:
		fieldvalue := readBoolValue(doc_bytes[:], p)
		return fieldvalue
		//return reflect.TypeOf(true)
	case 0x0A:
		return nil
		//var i interface{}
		//return reflect.TypeOf(i)
	case 0x10:
		fieldvalue := readInt32Value(doc_bytes[:], p)
		return fieldvalue
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

//pass the byte type in the BSON
func BSONType(b byte) reflect.Type {
	switch b {
	case 0x00:
		return nil
	case 0x01:
		return reflect.TypeOf(float64(0))
	case 0x02:
		return reflect.TypeOf(string(""))
	case 0x05:
		return reflect.TypeOf(make([]byte, 0))
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
