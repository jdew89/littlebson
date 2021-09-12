# littlebson
A local BSON database like sqlite.

The following info has been gathered from the official BSON specifications located on https://bsonspec.org/spec.html.

---

BSON is a binary format in which zero or more ordered key/value pairs are stored as a single entity. We call this entity a document.

The following grammar specifies version 1.1 of the BSON standard. We've written the grammar using a pseudo-BNF syntax. Valid BSON data is represented by the document non-terminal.

# Binary
## Basic Types
The following basic types are used as terminals in the rest of the grammar. Each type must be serialized in little-endian format.

|Type|Bytes|Bits|
|----|-----|----|
|byte|1|8-bit|
|int32|4|32-bit signed, two's compliment|
|int64|8|64-bit signed, two's compliment|
|uint64|8|64-bit unsigned integer|
|double|8|64-bit IEEE 754-2008 binary floating point|
|decimal128|16|128-bit IEEE 754-2008 decimal floating point|

## Non-terminals
The following specifies the rest of the BSON grammar. Note that quoted strings represent terminals, and should be interpreted with C semantics (e.g. "\x01" represents the byte 0000 0001). Also note that we use the * operator as shorthand for repetition (e.g. ("\x01"*2) is "\x01\x01"). When used as a unary operator, * means that the repetition can occur 0 or more times.

### document structure

```
document ::= int32 e_list "\x00" BSON 
```
BSON Document. int32 is the total number of bytes comprising the document.

### e_list structure

```
e_list ::= element e_list
         | ""
```

### element structure
```
element ::= "\x01" e_name double    64-bit binary floating point
          | "\x02" e_name string    UTF-8 string
          | "\x08" e_name "\x00"    Boolean false
          | "\x08" e_name "\x01"    Boolean true
          | "\x0A" e_name           Null value
          | "\x10" e_name int32     32-bit integer
          | "\x11" e_name uint64    Timestamp
          | "\x02" e_name int64     64-bit integer
```
### e_name structure
```
e_name ::= cstring
```


### string structure
```
string ::= int32 (byte*) "\x00   
```
The int32 is the number bytes in the (byte*) + 1 (for the trailing '\x00'). The (byte*) is zero or more UTF-8 encoded characters.

### cstring structure
```
cstring ::= (byte*) "\x00"
```
Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*) MUST NOT contain '\x00', hence it is not full UTF-8.

### binary structure
```
binary ::= int32 subtype (byte*)
```
The int32 is the number of bytes in the (byte*).

### subtype structure
```
subtype ::= "\x00"  Generic binary subtype
          | "\x01"  Function
          | "\x02"  Binary (Old)
          | "\x03"  UUID (Old)
          | "\x04"  UUID
          | "\x05"  MD5
          | "\x06"  Encrytped BSON value
          | "\x80"  User Defined
```

