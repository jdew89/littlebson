package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
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
	doc_type := reflect.ValueOf(doc)
	if doc_type.Kind() != reflect.Struct {
		return errors.New("Must pass a struct to insertOne.")
	}

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

//inserts many documents. Must be given an array or it throws an error.
func insertMany(collection_name string, doc_array interface{}) error {
	array_interface := reflect.ValueOf(doc_array)
	var all_doc_bytes []byte = make([]byte, 0)

	if array_interface.Kind() == reflect.Slice || array_interface.Kind() == reflect.Array {
		for i := 0; i < array_interface.Len(); i++ {
			doc := array_interface.Index(i).Interface()
			all_doc_bytes = append(all_doc_bytes, buildDocumentBytes(doc)...)
		}
	} else {
		return errors.New("Must pass an array or slice to insertMany.")
	}

	file, err := os.OpenFile(collection_name+".db", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	check(err)
	defer file.Close()

	//fmt.Println(doc_bytes)
	err = writeBSON(file, all_doc_bytes[:])

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
		switch obj.FieldValue.(type) {
		case int:
			val := int64(reflect.ValueOf(obj.FieldValue).Interface().(int))
			search_arr[i].FieldValue = val
		case uint:
			val := uint64(reflect.ValueOf(obj.FieldValue).Interface().(uint))
			search_arr[i].FieldValue = val
		}
	}

	var doc interface{}
	found := false
	for !found {
		doc, _, err = readOneDocument(reader, 0)
		if err != nil {
			fmt.Println("err finding", err)
			doc = nil
			break
		}

		doc_val := reflect.ValueOf(doc)

		//if the field does not exist, ignore it
		if doc_val.FieldByName(search_arr[0].FieldName).IsValid() {
			//check all fields, must match all of them
			for _, srch_obj := range search_arr {
				// if the field is a string, use regex
				if reflect.ValueOf(srch_obj.FieldValue).Kind() == reflect.String && doc_val.FieldByName(srch_obj.FieldName).Kind() == reflect.String {
					found, err = regexp.MatchString(srch_obj.FieldValue.(string), doc_val.FieldByName(srch_obj.FieldName).Interface().(string))
					if err != nil {
						return nil, err
					}
				} else {
					found = doc_val.FieldByName(srch_obj.FieldName).Interface() == srch_obj.FieldValue
				}

				//if one doesn't match, break
				if !found {
					break
				}
			}
		}
	}

	return doc, err
}

//finds all documents by searching the fieldname for given value
//panics on bad collection name
//returns documents, or zero documents if none found
func findMany(collection_name string, search_arr []SearchDocument) ([]interface{}, error) {
	reader, f := openCollection(collection_name)
	defer f.Close()
	var err error

	fmt.Println("Finding...", search_arr)

	for i, obj := range search_arr {
		//this swtich converts ints to int64's
		//this is because golang converts int's to the underlying architecture.
		//If the architecture is 32, it will convert to int64 just fine.
		switch obj.FieldValue.(type) {
		case int:
			val := int64(reflect.ValueOf(obj.FieldValue).Interface().(int))
			search_arr[i].FieldValue = val
		case uint:
			val := uint64(reflect.ValueOf(obj.FieldValue).Interface().(uint))
			search_arr[i].FieldValue = val
		}
	}

	var found_docs []interface{}
	//found := false
	// loops until it reaches EOF
	for {
		doc, _, err := readOneDocument(reader, 0)
		if err != nil {
			//fmt.Println("End of file.", err)
			doc = nil
			break
		}

		doc_val := reflect.ValueOf(doc)

		//if the field does not exist, ignore it
		if doc_val.FieldByName(search_arr[0].FieldName).IsValid() {
			found := false

			//check all fields, must match all of them
			for _, srch_obj := range search_arr {
				// if the field is a string, use regex
				if reflect.ValueOf(srch_obj.FieldValue).Kind() == reflect.String && doc_val.FieldByName(srch_obj.FieldName).Kind() == reflect.String {
					found, err = regexp.MatchString(srch_obj.FieldValue.(string), doc_val.FieldByName(srch_obj.FieldName).Interface().(string))
					if err != nil {
						return nil, err
					}
				} else {
					//found = doc_val.FieldByName(search_arr[0].FieldName).Interface() == reflect.ValueOf(search_arr[0].FieldValue).Interface()
					found = doc_val.FieldByName(srch_obj.FieldName).Interface() == srch_obj.FieldValue
				}

				//if one doesn't match, break
				if !found {
					break
				}
			}
			if found {
				found_docs = append(found_docs, doc)
			}

		}
	}

	return found_docs[:], err
}

//counts all documents with given query
//returns -1 and error if an error occurs
func FindCount(collection_name string, search_arr []SearchDocument) (int64, error) {
	reader, f := openCollection(collection_name)
	defer f.Close()
	var err error

	var count int64 = 0

	fmt.Println("Counting...", search_arr)

	for i, obj := range search_arr {
		//this swtich converts ints to int64's
		//this is because golang converts int's to the underlying architecture.
		//If the architecture is 32, it will convert to int64 just fine.
		switch obj.FieldValue.(type) {
		case int:
			val := int64(reflect.ValueOf(obj.FieldValue).Interface().(int))
			search_arr[i].FieldValue = val
		case uint:
			val := uint64(reflect.ValueOf(obj.FieldValue).Interface().(uint))
			search_arr[i].FieldValue = val
		}
	}

	for {
		doc, _, err := readOneDocument(reader, 0)
		if err != nil {
			//fmt.Println("End of file.", err)
			doc = nil
			break
		}

		doc_val := reflect.ValueOf(doc)

		//if the field does not exist, ignore it
		if doc_val.FieldByName(search_arr[0].FieldName).IsValid() {
			found := false

			//check all fields, must match all of them
			for _, srch_obj := range search_arr {
				// if the field is a string, use regex
				if reflect.ValueOf(srch_obj.FieldValue).Kind() == reflect.String && doc_val.FieldByName(srch_obj.FieldName).Kind() == reflect.String {
					found, err = regexp.MatchString(srch_obj.FieldValue.(string), doc_val.FieldByName(srch_obj.FieldName).Interface().(string))
					if err != nil {
						return -1, err
					}
				} else {
					//found = doc_val.FieldByName(search_arr[0].FieldName).Interface() == reflect.ValueOf(search_arr[0].FieldValue).Interface()
					found = doc_val.FieldByName(srch_obj.FieldName).Interface() == srch_obj.FieldValue
				}

				//if one doesn't match, break
				if !found {
					break
				}
			}
			if found {
				count++
			}

		}
	}

	return count, err
}

func UpdateOne(collection_name string, search_arr []SearchDocument, update_document []SearchDocument) error {
	reader, f := openCollection(collection_name)
	defer f.Close()
	var err error
	var file_loc_pointer int64 = 0

	fmt.Println("Finding...", search_arr)

	for i, obj := range search_arr {
		//this swtich converts ints to int64's
		//this is because golang converts int's to the underlying architecture.
		//If the architecture is 32, it will convert to int64 just fine.
		switch obj.FieldValue.(type) {
		case int:
			val := int64(reflect.ValueOf(obj.FieldValue).Interface().(int))
			search_arr[i].FieldValue = val
		case uint:
			val := uint64(reflect.ValueOf(obj.FieldValue).Interface().(uint))
			search_arr[i].FieldValue = val
		}
	}

	var doc interface{}
	found := false
	for !found {
		doc, file_loc_pointer, err = readOneDocument(reader, file_loc_pointer)
		if err != nil {
			fmt.Println("err finding", err)
			doc = nil
			break
		}

		doc_val := reflect.ValueOf(doc)

		//if the field does not exist, ignore it
		if doc_val.FieldByName(search_arr[0].FieldName).IsValid() {
			//check all fields, must match all of them
			for _, srch_obj := range search_arr {
				// if the field is a string, use regex
				if reflect.ValueOf(srch_obj.FieldValue).Kind() == reflect.String && doc_val.FieldByName(srch_obj.FieldName).Kind() == reflect.String {
					found, err = regexp.MatchString(srch_obj.FieldValue.(string), doc_val.FieldByName(srch_obj.FieldName).Interface().(string))
					if err != nil {
						return err
					}
				} else {
					found = doc_val.FieldByName(srch_obj.FieldName).Interface() == srch_obj.FieldValue
				}

				//if one doesn't match, break
				if !found {
					break
				}
			}
		}
	}

	//if found, update the document
	if found {
		//reader.WriteTo()
		peeked_val, _ := reader.Peek(1)
		fmt.Println("found for update - peek: ", peeked_val)

		fmt.Println("doc: ", doc)
		docReflectValue := reflect.ValueOf(doc)

		//need to copy the struct to an editable struct because the interface that is passed to this func
		//is showing as type interface when using a pointer but type struct without it.
		fieldValueMap := make(map[string]interface{})
		struct_fields := make([]reflect.StructField, docReflectValue.Type().NumField())

		for i := 0; i < docReflectValue.Type().NumField(); i++ {
			fieldValueMap[docReflectValue.Type().Field(i).Name] = docReflectValue.Field(i).Interface()

			struct_fields[i] = reflect.StructField{
				Name: docReflectValue.Type().Field(i).Name,
				Type: reflect.TypeOf(docReflectValue.Field(i).Interface()),
			}
		}

		base_document := reflect.StructOf(struct_fields[:])
		//var document reflect.Value
		updatedDocument := reflect.New(base_document).Elem()

		//updatedDocument.FieldByName("TestStr").SetString("TEST")
		for i := 0; i < docReflectValue.Type().NumField(); i++ {
			updatedDocument.FieldByName(docReflectValue.Type().Field(i).Name).Set(docReflectValue.Field(i))
		}

		for i := 0; i < len(update_document); i++ {
			updatedDocument.FieldByName(update_document[i].FieldName).Set(reflect.ValueOf(update_document[0].FieldValue))
		}

		fmt.Println("updated doc:", updatedDocument)

		TODO 
		I can just have readOneDocument return a reflect.value and then edit the fields. This would elim the need to copy to a new struct again.base_document
		Will need to refactor the other funcs that use it.
	}


	return err
}

