package main

import (
	"bufio"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/denisbrodbeck/machineid"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

//byte values for bson types
const (
	FLOAT64_TYPE  uint8 = 0x01
	STRING_TYPE         = 0x02
	DOCUMENT_TYPE       = 0x03
	ARRAY_TYPE          = 0x04
	BINARY_TYPE         = 0x05
	ID_TYPE             = 0x07
	BOOL_TYPE           = 0x08
	NULL_TYPE           = 0x0A
	INT32_TYPE          = 0x10
	UINT64_TYPE         = 0x11
	INT64_TYPE          = 0x12
)

type LilBsonID uint64

type Athing struct {
	Id      LilBsonID
	TestStr string
	Num64   int64
	Num32   int32
	Uint64  uint64
	Boolean bool
	BlahArr []string
	Float   float64
}

type Small struct {
	TestStr string
	Num32   int32
	Boolean bool
}

type Blarg struct {
	TestStr  string
	Num64    int64
	Num32    int32
	Uint64   uint64
	Boolean  bool
	Float    float64
	Binary   []byte
	Array    []interface{}
	MyStruct interface{}
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

	//genLilBsonId()

	runTest()

}

func runTest() {
	type myfloat float64
	afloat := myfloat(5.5)
	myarr := make([]interface{}, 7)
	myarr[0] = genLilBsonID()
	myarr[1] = "IT WORKS"
	myarr[2] = int32(4321)
	myarr[3] = afloat
	myarr[4] = true
	myarr[5] = []int64{9, 8, 7}
	myarr[6] = Small{"small struct", int32(32), true}

	//return
	mystrarr := make([]string, 2)
	mystrarr[0] = "hello"
	mystrarr[1] = "world"

	type tester struct {
		Array []interface{}
	}

	something := Athing{genLilBsonID(), "Duuude", -100, int32(100), 32134, true, mystrarr, 12.34}
	//something := tester{myarr[:]}
	//something := Small{"small struct", int32(32), true}
	//something := Blarg{"Duuude", -100, 100, 1234, false, 56.91, mybytes[:], myarr[:], Blarg{"Duuude", -100, 100, 1234, false, 56.91, mybytes[:], myarr[:], Small{}}}
	//insertOne("data", myarr[:])
	insertOne("data", something)
	file, _ := xml.MarshalIndent(something, "", " ")
	_ = ioutil.WriteFile("notes1.xml", file, 0644)

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
		val := reflect.ValueOf(doc)
		fmt.Println(val.Interface())
	} else {
		fmt.Println("Not found.")
	}
}

/*
	my id
	64 bit uint
	[10 bit machine id][41 bits of time][13 bit rand num]
	[time] with custom epoch of 1 jan 2021, 00:00:00
*/
func genLilBsonID() LilBsonID {

	time_bits := getTimeBits()
	fmt.Println("time bits: ", strconv.FormatUint(time_bits, 2))
	machine_bits := getMachineIdBits()
	fmt.Println("machine bits: ", strconv.FormatUint(machine_bits, 2))
	rand_bits := genRandBits()
	fmt.Println("rand bits: ", strconv.FormatUint(rand_bits, 2))

	var new_id uint64 = 0
	new_id = machine_bits << 54
	new_id += time_bits << 13
	new_id += rand_bits
	fmt.Println("id bits: ", strconv.FormatUint(new_id, 2))
	fmt.Println("id hex: ", strconv.FormatUint(new_id, 16))

	bson_id := LilBsonID(new_id)
	return bson_id
}

func getTimeBits() uint64 {
	var time_bits int64
	keep_bits := int64(0x000001ffffffffff) //keep 41 bits of mili time

	newepoch := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	utcepoch := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)

	mytime := time.Now()
	dif := utcepoch.Sub(newepoch)
	mytime = mytime.Add(dif)

	time_bits = mytime.UnixMilli() & keep_bits

	return uint64(time_bits)
}

//10 bits from hashed ID
//uses 5 bits from the front, and 5 from the back
func getMachineIdBits() uint64 {
	var id_bits uint64 = 0

	machine_str, err := machineid.ProtectedID("littlebson")
	if err != nil {
		panic("Cannot determine machine ID!")
	}
	machineid_bytes, err := hex.DecodeString(machine_str)
	if err != nil {
		panic("Cannot convert machine ID to hex!")
	}

	bytes := [2]byte{0, 0}
	bytes[0] = machineid_bytes[0] & 0b11111000                      //keep first 5 bits, zero rest
	bytes[1] = machineid_bytes[len(machineid_bytes)-1] & 0b00011111 //keep last 5 bits, zero rest

	id_bits = uint64(bytes[0]) << 2
	id_bits = id_bits + uint64(bytes[1])

	return id_bits
}

func genRandBits() uint64 {
	var bits uint64 = 0

	rand_source := rand.NewSource(time.Now().UnixNano())
	time_rand := rand.New(rand_source)
	bits = time_rand.Uint64() & 0x0000000000001fff //keep 13 bits

	return bits
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
	p = 0

	document, doc_size := readDocumentValue(docBytes[:], p)
	p += doc_size

	return document, nil

}

func readArrayValue(doc_bytes []byte, p int32) ([]interface{}, int32) {
	var arr_val []interface{}

	arr_len := bytesToInt32(doc_bytes[p : p+4])
	//fmt.Println("arr_len:", arr_len)

	array_bytes := doc_bytes[p : p+arr_len]
	p = p + arr_len

	//start past the array size bytes
	for i := int32(4); i < arr_len; {
		thetypebyte := array_bytes[i]
		//fmt.Println("byte type:", thetypebyte, " i: ", i, "arr size: ", len(arr_val))

		//if the type byte is 0x00, move the pointer to the end of document and terminate loop. This is the end of the document.
		if thetypebyte == 0x00 {
			//*p += 1
			//fmt.Println("found null byte, p:", p)
			break
		}
		i += 1

		_, name_size := readFieldName(array_bytes[:], i)
		i += name_size

		field_val, field_size := readFieldValue(thetypebyte, array_bytes[:], i)
		i += field_size

		arr_val = append(arr_val, field_val)

	}

	return arr_val, arr_len
}

func readDocumentValue(main_doc_bytes []byte, p int32) (interface{}, int32) {
	//var doc_val interface{}

	doc_len := bytesToInt32(main_doc_bytes[p : p+4])
	//fmt.Println("doc_len:", doc_len)

	sub_doc_bytes := main_doc_bytes[p : p+doc_len]
	p = p + doc_len

	type store_values struct {
		FieldName     string
		FieldTypeByte byte
		FieldValue    interface{}
	}

	//Then iterate through the map (using a range) and create the reflect struct and add the values

	field_num := -1
	doc_map := make(map[int]store_values)

	//start past the doc size bytes
	for i := int32(4); i < doc_len; {
		thetypebyte := sub_doc_bytes[i]
		//fmt.Println("byte type:", thetypebyte, " i: ", i, " fieldnum:", field_num)

		//if the type byte is 0x00, move the pointer to the end of document and terminate loop. This is the end of the document.
		if thetypebyte == 0x00 {
			//*p += 1
			//fmt.Println("found null byte, p:", p)
			break
		}
		i += 1
		field_num += 1

		field_name, name_size := readFieldName(sub_doc_bytes[:], i)
		i += name_size

		field_val, field_size := readFieldValue(thetypebyte, sub_doc_bytes[:], i)
		i += field_size

		doc_data := store_values{field_name, thetypebyte, field_val}
		doc_map[field_num] = doc_data
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

	return document.Interface(), doc_len

	//return doc_val, doc_len
}

//sets the value of the given field with the appropiate type
func setDocumentFieldValue(document *reflect.Value, field_value interface{}, typebyte byte, field_num int) {
	switch typebyte {
	case FLOAT64_TYPE:
		document.Field(field_num).SetFloat(field_value.(float64))
	case STRING_TYPE:
		document.Field(field_num).SetString(field_value.(string))
	case DOCUMENT_TYPE:
		document.Field(field_num).Set(reflect.ValueOf(field_value))
	case ARRAY_TYPE:
		document.Field(field_num).Set(reflect.ValueOf(field_value))
	case BINARY_TYPE:
		document.Field(field_num).SetBytes(field_value.([]byte))
	case ID_TYPE:
		document.Field(field_num).Set(reflect.ValueOf(field_value))
	case BOOL_TYPE:
		document.Field(field_num).SetBool(field_value.(bool))
	case NULL_TYPE: //null
		//document.Field(field_num)
	case INT32_TYPE:
		document.Field(field_num).SetInt(reflect.ValueOf(field_value).Int())
	case UINT64_TYPE: //timestamp
		document.Field(field_num).SetUint(field_value.(uint64))
	case INT64_TYPE:
		document.Field(field_num).SetInt(field_value.(int64))
	}
}

//determins what type to read from bytes. Reads the bytes and moves the pointer to after the value
//returns the value
func readFieldValue(typebyte byte, doc_bytes []byte, p int32) (interface{}, int32) {
	switch typebyte {
	case FLOAT64_TYPE:
		fieldvalue := readFloat64Value(doc_bytes[:], p)
		return fieldvalue, 8
	case STRING_TYPE:
		fieldvalue, string_size := readStringValue(doc_bytes[:], p)
		return *fieldvalue, string_size
	case DOCUMENT_TYPE:
		fieldvalue, doc_size := readDocumentValue(doc_bytes[:], p)
		return fieldvalue, doc_size
	case ARRAY_TYPE:
		fieldvalue, arr_size := readArrayValue(doc_bytes[:], p)
		return fieldvalue, arr_size
	case BINARY_TYPE:
		fieldvalue, binary_size := readBinaryDataValue(doc_bytes[:], p)
		return *fieldvalue, binary_size
	case ID_TYPE:
		fieldvalue := LilBsonID(readUint64Value(doc_bytes[:], p))
		return fieldvalue, 8
	case BOOL_TYPE:
		fieldvalue := readBoolValue(doc_bytes[:], p)
		return fieldvalue, 1
		//return reflect.TypeOf(true)
	case NULL_TYPE:
		return nil, 0
		//var i interface{}
		//return reflect.TypeOf(i)
	case INT32_TYPE:
		fieldvalue := readInt32Value(doc_bytes[:], p)
		return fieldvalue, 4
	case UINT64_TYPE: //timestamp
		fieldvalue := readUint64Value(doc_bytes[:], p)
		return fieldvalue, 8
	case INT64_TYPE:
		fieldvalue := readInt64Value(doc_bytes[:], p)
		return fieldvalue, 8
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
	case FLOAT64_TYPE:
		return reflect.TypeOf(float64(0))
	case STRING_TYPE:
		return reflect.TypeOf(string(""))
	case DOCUMENT_TYPE:
		struct_fields := make([]reflect.StructField, 0)
		return reflect.StructOf(struct_fields[:])
	case ARRAY_TYPE:
		return reflect.TypeOf(make([]interface{}, 0))
	case BINARY_TYPE:
		return reflect.TypeOf(make([]byte, 0))
	case ID_TYPE:
		return reflect.TypeOf(LilBsonID(0))
	case BOOL_TYPE:
		return reflect.TypeOf(true)
	case NULL_TYPE:
		//must return a closure that returns nil - otherwise reflect sees no type and is invalid
		return reflect.TypeOf(func() interface{} { return nil })
	case INT32_TYPE:
		return reflect.TypeOf(int32(0))
	case UINT64_TYPE: //timestamp
		return reflect.TypeOf(uint64(0))
	case INT64_TYPE:
		return reflect.TypeOf(int64(0))
	}
	panic("Invalid type for BSON field.")
}
