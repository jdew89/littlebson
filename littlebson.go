package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"sort"
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
	Float   float64
}

type SearchField struct {
	FieldName   string
	FieldValue  interface{}
	CompareType string //eq neq gt lt gte lte rgx
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
	/*
		fmt.Println("Testing")

		id := genLilBsonID()

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
		err := insertOne("test", initialStruct)
	*/

	fmt.Println("building lbson")
	start := time.Now()

	testarr := make([]Athing, 12)
	for i := 0; i < len(testarr); i++ {
		something := Athing{genLilBsonID(), "Duuude" + fmt.Sprint(i), int64(i), int32(100) + int32(i), 1000 + uint64(i), false, 50.91 + float64(i)}
		testarr[i] = something
	}

	//fmt.Println(xmltestarr)
	err := insertMany("data", testarr[:])
	check(err)

	duration := time.Since(start)
	fmt.Println("bson write time:", duration.Milliseconds())

	start = time.Now()
	//fmt.Printf("%+v\n", something)

	query := make([]SearchField, 0)
	//query[0] = SearchField{"TestStr", "(?i)DuUude"}
	//query[0] = SearchField{"TestStr", "Duuude[6,7,8]", "rgx"}
	//query[0] = SearchField{"TestStr", "Duuude6", "eq"}
	//query[1] = SearchField{"Num64", 8, "lt"}
	//query[1] = SearchField{"Nzzz", 8, "ne"}
	//query[2] = SearchField{"Num32", int32(106)}

	doc, err := findOne("data", query)
	if err == nil {
		//val := reflect.ValueOf(doc[0])
		fmt.Println("found one:", doc)

	} else {
		fmt.Println("Not found.")
	}
	docs, err := findMany("data", query)
	if err == nil && len(docs) > 0 {
		//val := reflect.ValueOf(doc[0])
		fmt.Println("found: ", len(docs))
		fmt.Println(docs[0])
	} else {
		fmt.Println("Not found.")
	}

	count, err := FindCount("data", query)
	fmt.Println("found docs count:", count)

	duration = time.Since(start)
	fmt.Println("lbson read time:", duration.Milliseconds())

	//updateDoc := make([]SearchField, 2)
	//updateDoc[0] = SearchField{"TestStr", "Duuude6updated"}
	//updateDoc[1] = SearchField{"Num64", int64(66)}
	//fmt.Println("callig query")
	//err = UpdateOne("data", query[:], updateDoc[:])

	err = DeleteOne("data", query[:])
	if err == nil {
		fmt.Println("deleted")
	}
	//err = DeleteMany("data", query[:])
	//err = UpdateMany("data", query[:], updateDoc[:])
}

/*
	my id
	64 bit uint
	[10 bit machine id][41 bits of time][13 bit rand num]
	[time] with custom epoch of 1 jan 2021, 00:00:00
*/
func genLilBsonID() LilBsonID {

	time_bits := getTimeBits()
	//fmt.Println("time bits: ", strconv.FormatUint(time_bits, 2))
	machine_bits := getMachineIdBits()
	//fmt.Println("machine bits: ", strconv.FormatUint(machine_bits, 2))
	rand_bits := genRandBits()
	//fmt.Println("rand bits: ", strconv.FormatUint(rand_bits, 2))

	var new_id uint64 = 0
	new_id = machine_bits << 54
	new_id += time_bits << 13
	new_id += rand_bits
	//fmt.Println("id bits: ", strconv.FormatUint(new_id, 2))
	//fmt.Println("id hex: ", strconv.FormatUint(new_id, 16))

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
	//wait until the next nano second to seed the random
	time.Sleep(1 * time.Nanosecond)
	rand_source := rand.NewSource(time.Now().UnixNano())
	time_rand := rand.New(rand_source)
	bits = time_rand.Uint64() & 0x0000000000001fff //keep 13 bits

	return bits
}

//pass a slice to this function
func writeBSON(file *os.File, data []byte) error {
	_, err := file.Write(data)

	//fmt.Printf("Wrote %d bytes\n", n1)
	return err
}

//writes to a temp file, then renames to main file
func UpdateBSON(collectionName string, updatedDocLocation int64, updatedDocBytes []byte, reader *bufio.Reader, dbFile *os.File) error {
	dbFile.Seek(0, 0)
	reader.Reset(dbFile)

	//tempFile, err := os.CreateTemp("C:\\Users\\JD\\Documents\\golang", "lbson*")
	tempFile, err := os.CreateTemp("", "lbson*")
	check(err)

	readBuffer := make([]byte, updatedDocLocation)
	_, err = io.ReadFull(reader, readBuffer)
	check(err)

	tempFile.Write(readBuffer)
	tempFile.Write(updatedDocBytes)

	//skip over the old document
	docLenBytes, err := reader.Peek(4)
	check(err)
	docLen := bytesToInt32(docLenBytes[:])
	_, err = reader.Discard(int(docLen))
	check(err)

	//copy old data and insert updated documents in their place
	for err == nil {
		docLenBytes, err = reader.Peek(4)
		if err == io.EOF {
			break
		}
		check(err)
		docLen = bytesToInt32(docLenBytes[:])

		readBuffer = make([]byte, docLen)
		_, err = io.ReadFull(reader, readBuffer)
		check(err)

		_, err = tempFile.Write(readBuffer)
		check(err)
	}
	//if EOF reading, then move the updated DB to the colletion loc
	if err == io.EOF {
		err = MoveTempFile(tempFile, dbFile)
		check(err)
	}
	return err
}

//writes to a temp file, then renames to main file
func UpdateManyBSON(collectionName string, updatedDocumentBytes map[int][]byte, reader *bufio.Reader, dbFile *os.File) error {
	fileInfo, err := dbFile.Stat()
	check(err)

	dbFile.Seek(0, 0)
	reader.Reset(dbFile)

	//tempFile, err := os.CreateTemp("C:\\Users\\JD\\Documents\\golang\\littlebson", "lbson*")
	//tempFile, err := os.CreateTemp("C:\\Users\\i31586\\Documents\\go\\littlebson", "lbson*")
	tempFile, err := os.CreateTemp("", "lbson*")
	check(err)

	var file_byte_pointer int = 0
	updatedDocumentByteKeys := SortUpdatedDocumentKeys(updatedDocumentBytes)
	for i := range updatedDocumentByteKeys {
		//read the difference between current pointer and the updated document
		fmt.Println("doc pointer:", updatedDocumentByteKeys[i], "file pointer:", file_byte_pointer)

		pointerDistanceToDoc := updatedDocumentByteKeys[i] - file_byte_pointer
		//only write old stuff if we read bytes
		if pointerDistanceToDoc > 0 {
			readBuffer := make([]byte, pointerDistanceToDoc)
			_, err = io.ReadFull(reader, readBuffer)
			check(err)

			tempFile.Write(readBuffer[:])
		}
		tempFile.Write(updatedDocumentBytes[updatedDocumentByteKeys[i]][:])

		//skip over the old document
		oldDocLenBytes, err := reader.Peek(4)
		check(err)
		oldDocLen := bytesToInt32(oldDocLenBytes[:])
		_, err = reader.Discard(int(oldDocLen))
		check(err)

		file_byte_pointer += pointerDistanceToDoc + int(oldDocLen)
		fmt.Println("pointer:", file_byte_pointer)
	}

	//read to end of file and copy it over after all the updated docs are written
	fmt.Println("file size:", fileInfo.Size(), "pointer:", file_byte_pointer)
	readBuffer := make([]byte, fileInfo.Size()-int64(file_byte_pointer))
	_, err = io.ReadFull(reader, readBuffer)
	check(err)
	tempFile.Write(readBuffer)

	err = MoveTempFile(tempFile, dbFile)
	check(err)

	return err
}

func DeleteOneBSON(collectionName string, deletedDocLoc int64, reader *bufio.Reader, dbFile *os.File) error {
	fileInfo, err := dbFile.Stat()
	dbFile.Seek(0, 0)
	reader.Reset(dbFile)

	var file_byte_pointer int64 = 0

	//tempFile, err := os.CreateTemp("C:\\Users\\JD\\Documents\\golang", "lbson*")
	tempFile, err := os.CreateTemp("", "lbson*")
	check(err)

	readBuffer := make([]byte, deletedDocLoc)
	_, err = io.ReadFull(reader, readBuffer)
	check(err)

	tempFile.Write(readBuffer)

	//skip over the old document
	docLenBytes, err := reader.Peek(4)
	check(err)
	docLen := bytesToInt32(docLenBytes[:])
	_, err = reader.Discard(int(docLen))
	check(err)

	file_byte_pointer = deletedDocLoc + int64(docLen)

	//read to end of file and copy it over after all the updated docs are written
	fmt.Println("file size:", fileInfo.Size(), "pointer:", file_byte_pointer)
	readBuffer = make([]byte, fileInfo.Size()-file_byte_pointer)
	_, err = io.ReadFull(reader, readBuffer)
	check(err)
	tempFile.Write(readBuffer)

	err = MoveTempFile(tempFile, dbFile)
	check(err)

	return err
}

//writes to a temp file, then renames to main file
func DeleteManyBSON(collectionName string, deleteDocOffsets []int, reader *bufio.Reader, dbFile *os.File) error {
	fileInfo, err := dbFile.Stat()
	dbFile.Seek(0, 0)
	reader.Reset(dbFile)

	var file_byte_pointer int = 0

	//tempFile, err := os.CreateTemp("C:\\Users\\JD\\Documents\\golang", "lbson*")
	tempFile, err := os.CreateTemp("", "lbson*")
	check(err)

	for _, offset := range deleteDocOffsets {
		readBuffer := make([]byte, offset-file_byte_pointer)
		_, err = io.ReadFull(reader, readBuffer)
		check(err)

		tempFile.Write(readBuffer)

		//skip over the old document
		docLenBytes, err := reader.Peek(4)
		check(err)
		docLen := bytesToInt32(docLenBytes[:])
		_, err = reader.Discard(int(docLen))
		check(err)

		file_byte_pointer = offset + int(docLen)
	}

	//read to end of file and copy it over after all the updated docs are written
	//fmt.Println("file size:", fileInfo.Size(), "pointer:", file_byte_pointer)
	readBuffer := make([]byte, fileInfo.Size()-int64(file_byte_pointer))
	_, err = io.ReadFull(reader, readBuffer)
	check(err)
	tempFile.Write(readBuffer)

	err = MoveTempFile(tempFile, dbFile)
	check(err)

	return err
}

func MoveTempFile(tempFile *os.File, dbFile *os.File) error {
	tempFile.Close()
	dbFile.Close()

	dbFileName := dbFile.Name()

	fmt.Println(tempFile.Name())

	err := os.Rename(dbFileName, dbFileName+".bak")
	check(err)

	err = os.Rename(tempFile.Name(), dbFileName)
	check(err)

	err = os.Remove(dbFileName + ".bak")
	check(err)

	return err
}

//sorts keys in a map and returns the keys in order
func SortUpdatedDocumentKeys(m map[int][]byte) []int {
	keys := make([]int, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Ints(keys)
	fmt.Println("KEYS: ", keys[:])
	return keys[:]
}

//reads 1 full document into memory and returns it as an interface
//returns the doucment and pointer location in the file
func readOneDocument(reader *bufio.Reader, p int64) (reflect.Value, int64, error) {
	//docLenBytes := make([]byte, 4)
	docLenBytes, err := reader.Peek(4) //gets the first document length
	if err != nil {
		var empty interface{}
		return reflect.ValueOf(empty), p, err
	}
	docLen := bytesToInt32(docLenBytes[:])
	//fmt.Println("doc length: ", docLenBytes)
	//fmt.Println("doc len: ", docLen)

	if docLen < 4 {
		docLen = 4
	}

	docBytes := make([]byte, docLen)
	_, err = io.ReadFull(reader, docBytes)
	if err != nil {
		var empty interface{}
		return reflect.ValueOf(empty), p, err
	}

	reflectDocument, doc_size := readDocumentValue(docBytes[:], 0)
	p += int64(doc_size)

	return reflectDocument, p, nil

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

//reads a full document and returns the interface of that document and byte size
//returns document, document length
func readDocumentValue(main_doc_bytes []byte, p int32) (reflect.Value, int32) {

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
	//reads the bytes into a key-value map for created the reflect.struct fields
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
		if doc.FieldTypeByte == DOCUMENT_TYPE {
			embeded_doc := doc.FieldValue
			structfield := InitStructField(doc.FieldName, doc.FieldTypeByte, embeded_doc)

			struct_fields[k] = *structfield
		} else {
			structfield := InitStructField(doc.FieldName, doc.FieldTypeByte, nil)

			struct_fields[k] = *structfield
		}

	}

	base_document := reflect.StructOf(struct_fields[:])
	//var document reflect.Value
	document := reflect.New(base_document).Elem()

	//add field values to struct
	for key, doc := range doc_map {
		SetDocumentFieldValue(&document, doc.FieldValue, doc.FieldTypeByte, key)
	}

	return document, doc_len

	//return doc_val, doc_len
}

//sets the value of the given field with the appropiate type
func SetDocumentFieldValue(document *reflect.Value, field_value interface{}, typebyte byte, field_num int) {
	//fmt.Println(field_value)
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
		fieldReflectValue, doc_size := readDocumentValue(doc_bytes[:], p)
		fieldvalue := fieldReflectValue.Interface()
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
func InitStructField(name string, typebyte byte, embeded_doc interface{}) *reflect.StructField {
	structfield := reflect.StructField{
		Name: name,
		Type: BSONType(typebyte, embeded_doc),
	}

	return &structfield
}

//pass the byte type in the BSON
func BSONType(b byte, embeded_doc interface{}) reflect.Type {
	switch b {
	case 0x00:
		return nil
	case FLOAT64_TYPE:
		return reflect.TypeOf(float64(0))
	case STRING_TYPE:
		return reflect.TypeOf(string(""))
	case DOCUMENT_TYPE:
		reflected_embeded_doc_type := reflect.ValueOf(embeded_doc).Type()
		struct_fields := make([]reflect.StructField, reflected_embeded_doc_type.NumField())
		//set up embeded struct structure
		for i := 0; i < reflected_embeded_doc_type.NumField(); i++ {
			struct_fields[i].Name = reflected_embeded_doc_type.Field(i).Name
			struct_fields[i].Type = reflected_embeded_doc_type.Field(i).Type
		}
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
