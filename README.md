# littlebson
A local BSON database like sqlite.

The following info has been gathered from the official BSON specifications located on https://bsonspec.org/spec.html.

---

BSON is a binary format in which zero or more ordered key/value pairs are stored as a single entity. We call this entity a document.

The following grammar specifies version 1.1 of the BSON standard. We've written the grammar using a pseudo-BNF syntax. Valid BSON data is represented by the document non-terminal.

# Binary
## Basic Types {#basic-types}
The following basic types are used as terminals in the rest of the grammar. Each type must be serialized in little-endian format.

|Type|Bytes|Bits|
|----|-----|----|
|byte|1|8-bit|
|int32|4|32-bit signed, two's compliment|
|int64|8|64-bit signed, two's compliment|
|uint64|8|64-bit unsigned integer|
|double|8|64-bit IEEE 754-2008 binary floating point|
|decimal128|16|128-bit IEEE 754-2008 decimal floating point|

## Non-terminals {#non-terminals}
The following specifies the rest of the BSON grammar. Note that quoted strings represent terminals, and should be interpreted with C semantics (e.g. "\x01" represents the byte 0000 0001). Also note that we use the * operator as shorthand for repetition (e.g. ("\x01"*2) is "\x01\x01"). When used as a unary operator, * means that the repetition can occur 0 or more times.

### document {#bson-document}

document ::= [int32](#basic-types) [e_list](#bson-e_list) "\x00" BSON

|Name|::=|type|type|type|Description|
|-|-|-|-|-|-|
|document|::=|int32|e_list|"\x00"|BSON Document, in32 is the total number of bytes comprising the document.

### e_list {#bson-e_list}



### element {#bson-element}

### string {#bson-string}

### cstring {#bson-cstring}

### binary {#bson-binary}

### subtype {#bson-subtype}


