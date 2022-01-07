package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
)

//This file has all of the possible query functions.

func openCollection(collection_path string) (*bufio.Reader, *os.File) {
	f, err := os.Open(collection_path + ".db")
	check(err)

	//TODO: This has a default buffer size of 4096. I need to test reading a doc bigger than that
	reader := bufio.NewReader(f)

	return reader, f
}

func OpenCollectionForWriting(collection_name string) (*os.File, error) {
	file, err := os.OpenFile(collection_name+".db", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return file, err
}

//inserts 1 document at the end of specified collection
func insertOne(collection_name string, doc interface{}) error {
	doc_type := reflect.ValueOf(doc)
	if doc_type.Kind() != reflect.Struct {
		return errors.New("must pass a struct to insertOne")
	}

	doc_bytes := buildDocumentBytes(doc)

	file, err := OpenCollectionForWriting(collection_name)
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
		return errors.New("must pass an array or slice to insertMany")
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

	//var doc interface{}
	var doc_val reflect.Value
	found := false
	for !found {
		doc_val, _, err = readOneDocument(reader, 0)

		if err != nil {
			fmt.Println("err finding", err)
			return nil, err
		}

		//doc_val := reflect.ValueOf(doc)

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

	return doc_val.Interface(), err
}

//finds all documents by searching the fieldname for given value
//panics on bad collection name
//returns documents, or zero documents if none found
func findMany(collection_name string, search_arr []SearchDocument) ([]interface{}, error) {
	reader, f := openCollection(collection_name)
	defer f.Close()

	fmt.Println("Finding Many...", search_arr)

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
		doc_val, _, err := readOneDocument(reader, 0)

		if err != nil {
			if err == io.EOF { //if EOF return found docs
				return found_docs[:], nil
			} else { //if some other error, return it
				return nil, err
			}
		}

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
				found_docs = append(found_docs, doc_val.Interface())
			}

		}
	}
}

//counts all documents with given query
//returns -1 and error if an error occurs
func FindCount(collection_name string, search_arr []SearchDocument) (int64, error) {
	reader, f := openCollection(collection_name)
	defer f.Close()

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
		doc_val, _, err := readOneDocument(reader, 0)
		if err != nil {
			//fmt.Println("End of file.", err)
			return count, err
		}
		if err != nil {
			if err == io.EOF { //return count if EOF
				return count, nil

			} else { //if some other error, return it
				return -1, err
			}
		}

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
}

func UpdateOne(collection_name string, search_arr []SearchDocument, update_document_fields []SearchDocument) error {
	reader, f := openCollection(collection_name)
	defer f.Close()
	var err error

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

	//var doc interface{}
	var doc_val reflect.Value
	var curr_doc_pointer int64 = 0 //tracks current position in file
	var prev_doc_pointer int64 = 0 //tracks previous doc loc

	found := false
	for !found {
		doc_val, curr_doc_pointer, err = readOneDocument(reader, curr_doc_pointer)
		//fmt.Println("prev", prev_doc_pointer, " - curr", curr_doc_pointer)

		if err != nil {
			fmt.Println("err finding", err)
			return err
		}

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
		if !found {
			//move pointer to next document if not found
			prev_doc_pointer = curr_doc_pointer
		}
	}

	//if found, update the document
	if found {
		//update the fields
		for i := 0; i < len(update_document_fields); i++ {
			doc_val.FieldByName(update_document_fields[i].FieldName).Set(reflect.ValueOf(update_document_fields[i].FieldValue))
		}

		updatedDocBytes := buildDocumentBytes(doc_val.Interface())

		err = UpdateBSON(collection_name, prev_doc_pointer, updatedDocBytes[:], reader, f)
		check(err)
	}

	return err
}

func UpdateMany(collection_name string, search_arr []SearchDocument, update_document_fields []SearchDocument) error {
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

	found_docs := make(map[int]reflect.Value) //stores the file loc of doc and updated value

	var curr_doc_pointer int64 = 0 //tracks current position in file
	var prev_doc_pointer int64 = 0 //tracks previous doc loc

	// loops until it reaches EOF
	for {
		var doc_val reflect.Value
		doc_val, curr_doc_pointer, err = readOneDocument(reader, curr_doc_pointer)
		//fmt.Println("prev", prev_doc_pointer, " - curr", curr_doc_pointer)
		//fmt.Println(doc_val.Interface())
		if err != nil {
			if err == io.EOF { //only break if EOF
				break
			} else { //if some other error, return it
				return err
			}
		}

		//if the field does not exist, ignore it
		//TODO THIS SHOULD PROBABLY BE IN THE LOOP
		if doc_val.FieldByName(search_arr[0].FieldName).IsValid() {
			found := false

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
			if found {
				found_docs[int(prev_doc_pointer)] = doc_val
			}
		}
		prev_doc_pointer = curr_doc_pointer
	}

	//if found, update the document
	if len(found_docs) > 0 {
		fmt.Println("docs to update:", len(found_docs))
		fmt.Println("before update:")
		for k, elem := range found_docs {
			fmt.Print(k, ":", elem.Interface())
		}

		updatedDocBytes := make(map[int][]byte)
		for k, elem := range found_docs {
			//update the fields
			for i := 0; i < len(update_document_fields); i++ {
				elem.FieldByName(update_document_fields[i].FieldName).Set(reflect.ValueOf(update_document_fields[i].FieldValue))
			}

			updatedDocBytes[k] = buildDocumentBytes(elem.Interface())
		}

		fmt.Println("")
		fmt.Println("after:")
		for k, elem := range found_docs {
			fmt.Print(k, ":", elem.Interface())
			fmt.Println("")
		}

		err = UpdateManyBSON(collection_name, updatedDocBytes, reader, f)
		//check(err)
	}

	return err
}

func DeleteOne(collection_name string, search_arr []SearchDocument) error {
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

	//var doc interface{}
	var doc_val reflect.Value
	var curr_doc_pointer int64 = 0 //tracks current position in file
	var prev_doc_pointer int64 = 0 //tracks previous doc loc

	found := false
	for !found {
		doc_val, curr_doc_pointer, err = readOneDocument(reader, curr_doc_pointer)
		//fmt.Println("prev", prev_doc_pointer, " - curr", curr_doc_pointer)

		if err != nil {
			fmt.Println("err finding", err)
			return err
		}

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
		if !found {
			//move pointer to next document if not found
			prev_doc_pointer = curr_doc_pointer
		}
	}

	//if found, delete the document
	if found {
		err = DeleteOneBSON(collection_name, prev_doc_pointer, reader, f)
		check(err)
	}

	return err
}

func DeleteMany(collection_name string, search_arr []SearchDocument) error {
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

	found_docs := make(map[int]reflect.Value) //stores the file loc of doc and updated value

	var curr_doc_pointer int64 = 0 //tracks current position in file
	var prev_doc_pointer int64 = 0 //tracks previous doc loc

	// loops until it reaches EOF
	for {
		var doc_val reflect.Value
		doc_val, curr_doc_pointer, err = readOneDocument(reader, curr_doc_pointer)
		//fmt.Println("prev", prev_doc_pointer, " - curr", curr_doc_pointer)
		//fmt.Println(doc_val.Interface())
		if err != nil {
			if err == io.EOF { //only break if EOF
				break
			} else { //if some other error, return it
				return err
			}
		}

		//if the field does not exist, ignore it
		//TODO THIS SHOULD PROBABLY BE IN THE LOOP
		if doc_val.FieldByName(search_arr[0].FieldName).IsValid() {
			found := false

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
			if found {
				found_docs[int(prev_doc_pointer)] = doc_val
			}
		}
		prev_doc_pointer = curr_doc_pointer
	}

	//if found, update the document
	if len(found_docs) > 0 {
		fmt.Println("docs to delete:", len(found_docs))

		//gets map keys
		deleteDocsLocs := make([]int, len(found_docs))
		i := 0
		for k := range found_docs {
			deleteDocsLocs[i] = k
			i++
		}
		sort.Ints(deleteDocsLocs)
		fmt.Println("KEYS: ", deleteDocsLocs[:])

		err = DeleteManyBSON(collection_name, deleteDocsLocs[:], reader, f)
		//check(err)
	}

	return err
}
