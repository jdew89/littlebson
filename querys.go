package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
)

//This file has all of the possible query functions.

func openCollection(collection_path string) (*bufio.Reader, *os.File) {
	f, err := os.Open(collection_path + ".db")
	check(err)

	reader := bufio.NewReader(f)

	return reader, f
}

//TODO: could change this to accept a doc pointer rather than the object. Could save execution time.
//inserts 1 document at the end of specified collection
func insertOne(collection_name string, doc interface{}) error {
	doc_bytes := buildDocumentBytes(doc)

	file, err := os.OpenFile(collection_name+".db", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	check(err)
	defer file.Close()

	//fmt.Println(doc_bytes)
	err = writeBSON(file, doc_bytes[:])

	return err
}

//finds first document by searching the fieldname for given value
//panics on bad collection name
//returns document, or error if no matches found
func findOne(collection_name string, search_arr []SearchDocument) (interface{}, error) {
	reader, f := openCollection(collection_name)
	defer f.Close()
	var err error

	fmt.Println("Finding...", search_arr)

	for i, obj := range search_arr {
		//this swtich converts ints to int64's
		//this is because golang converts int's to the underlying architecture.
		//If the architecture is 32, it will convert to int64 just fine.
		switch obj.Value.(type) {
		case int:
			val := int64(reflect.ValueOf(obj.Value).Interface().(int))
			search_arr[i].Value = val
		case uint:
			val := uint64(reflect.ValueOf(obj.Value).Interface().(uint))
			search_arr[i].Value = val
		}
	}

	var doc interface{}
	found := false
	for !found {
		doc, err = readOneDocument(reader)
		if err != nil {
			fmt.Println("err finding", err)
			doc = nil
			break
		}

		doc_val := reflect.ValueOf(doc)

		//fmt.Println(doc_val.Interface())
		//fmt.Println("doc_val: ", doc_val.FieldByName(field_name))
		//fmt.Println("find_val: ", reflect.ValueOf(field_val))
		//fmt.Println("interfaces eq: ", doc_val.FieldByName(field_name).Interface() == reflect.ValueOf(field_val).Interface())
		//fmt.Println("string vals eq: ",doc_val.FieldByName(field_name).String() == reflect.ValueOf(field_val).String())

		//if the field does not exist, ignore it
		if doc_val.FieldByName(search_arr[0].Name).IsValid() {
			found = doc_val.FieldByName(search_arr[0].Name).Interface() == reflect.ValueOf(search_arr[0].Value).Interface()
			//check the rest of the query params if found first
			if found {
				fmt.Println("found first")
				for _, obj := range search_arr {
					found = doc_val.FieldByName(obj.Name).Interface() == reflect.ValueOf(obj.Value).Interface()

					//fmt.Println("doc_type", doc_val.FieldByName(obj.Name).Type())
					//fmt.Println("find_type", reflect.ValueOf(obj.Value).Type())

					//if one doesn't match, break
					if !found {
						break
					}
				}
			}
		}
	}

	return doc, err
}
