package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Athing struct {
	Test string
	Num  int64
}

func main() {

	something := Athing{"test", -1}

	fmt.Printf("%+v\n", something)
	//typeofstruct(something)

	//fmt.Println(reflect.TypeOf(something.Name))

	//data := []byte{120, 200, 222}

	writedata := buildDocumentBytes(something)
	fmt.Println(writedata)
	writeBSON(writedata[:])
	readBSON()

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
			data = append(data, uint64ToBytes(uint64(field.Int()))...)
		case reflect.Bool:
			data = append(data, uint8(0x08))                       //type of next var
			data = append(data, []byte(docTypes.Field(i).Name)...) //field name
			data = append(data, uint8(0))                          //terminate the string
			data = append(data, uint64ToBytes(uint64(field.Int()))...)
		}
	}
	data = append(data, uint8(0)) //terminate the document

	data = append(int32ToBytes(int32(len(data))), data...) //append document size to front

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

func readBSON() {
	dat, err := ioutil.ReadFile("data.db")
	check(err)
	fmt.Printf("%x\n", dat)
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
func int32ToBytes(i int32) []byte {
	var data [4]byte

	for p := 0; p < 4; p++ {
		data[p] = uint8(i)
		i = i >> 8
	}

	return data[:]
}
