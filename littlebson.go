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

func main() {

	type athing struct {
		name string
		num  int64
	}

	something := athing{name: "test", num: 100}
	fmt.Printf("%+v\n", something)
	typeofstruct(something)

	fmt.Println(reflect.TypeOf(something.name))

	in_f, err := os.Create("data.db")
	check(err)

	defer in_f.Close()

	data := []byte{120, 222}
	n1, err := in_f.Write(data)

	fmt.Printf("Wrote %d bytes\n", n1)

	dat, err := ioutil.ReadFile("data.db")
	check(err)
	fmt.Printf("%x\n", dat)

	/*
		f, err := os.Open("data.db")
		check(err)

		b1 := make([]byte, 5)
		n1, err := f.Read(b1)
		check(err)
		fmt.Printf("%d bytes: %s\n", n1, string(b1[:n1]))

		o2, err := f.Seek(6, 0)
		check(err)
		b2 := make([]byte, 2)
		n2, err := f.Read(b2)
		check(err)
		fmt.Printf("%d bytes @ %d: ", n2, o2)
		fmt.Printf("%v\n", string(b2[:n2]))

		o3, err := f.Seek(6, 0)
		check(err)
		b3 := make([]byte, 2)
		n3, err := io.ReadAtLeast(f, b3, 2)
		check(err)
		fmt.Printf("%d bytes @ %d: %s\n", n3, o3, string(b3))

		_, err = f.Seek(0, 0)
		check(err)

		r4 := bufio.NewReader(f)
		b4, err := r4.Peek(5)
		check(err)
		fmt.Printf("5 bytes: %s\n", string(b4))

		f.Close()
	*/
}

func typeofstruct(x interface{}) {
	fmt.Println(reflect.TypeOf(x))

}
