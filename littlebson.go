package main

import (
	"fmt"
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

func main() {

	something := Athing{"Howedy", -1, 2000, 32134, true, nil, 12.34}

	fmt.Printf("%+v\n", something)
	//typeofstruct(something)

	//fmt.Println(reflect.TypeOf(something.Name))

	//writedata := buildDocumentBytes(something)
	//fmt.Println(writedata)
	//writeBSON(writedata[:])
	val := reflect.ValueOf(readBSON()).Elem()
	fmt.Println(val.Interface())
	fmt.Println(val.NumField())
	fmt.Println(val.Type().Field(0).Name)
	fmt.Println(val.FieldByName(val.Type().Field(0).Name))

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
			data = append(data, uint8(0x02))                            //var type - String
			data = append(data, []byte(docTypes.Field(i).Name)...)      //field name
			data = append(data, uint8(0), uint8(len(field.String())+1)) //terminate the name string, and add length of string value (add 1 for null terminator)
			data = append(data, []byte(field.String())...)              //field value
			data = append(data, uint8(0))                               //terminate the string
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

/*func readBSON2() {
	dat, err := ioutil.ReadFile("data.db")
	check(err)
	fmt.Printf("%x\n", dat)

	f, err := os.Open("data.db")

	docLenBytes := make([]byte, 4)
	b1, err := f.Read(elem1)
	check(err)
	docLen := bytesToInt64(docLenBytes[:])
	fmt.Println("elem1: ", docLenBytes)
	fmt.Println("doc len: ", docLen)

	offset := 4

	offset, err = f.Seek(4, 0)
	check(err)

	prop1 := make([]byte, 1)
	b1, err = f.Read(prop1)
	check(err)

	f.close()
}*/

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
	v.Field((1)).SetInt(2)

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
