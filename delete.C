#include "catalog.h"
#include "query.h"
#include <stdexcept>
#include <cassert>


/**
 * Deletes records from a specified relation. 
 * This function will delete all tuples in relation
 * satisfying the predicate specified by attrName, op, 
 * and the constant attrValue. type denotes the type 
 * of the attribute. 
 * 
 * You can locate all the qualifying tuples using 
 * a filtered HeapFileScan.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
//check if input argument is null
if (relation.empty()){
	return UNIXERR;
}

Status status;
HeapFileScan scanner(relation, status);
if (status != OK){
	return status;
}

//get the offset and length
AttrCatalog cat(status);
if (status != OK){
	return status;
}
AttrDesc desc; 
if (! attrName.empty()){
	status = cat.getInfo(relation, attrName, desc);
	if (status != OK){
		return status;
	}
	//assert(desc.attrType == type);
}

//declare variables
int attrValueInt;
float attrValueFloat;
void *ptr = nullptr;
//check if attrName is NULL
if (attrName.empty()){
	//scan without filtering
	//Set startScan’s offset and length to 0, type to string, filter to NULL
	status = scanner.startScan(0,0, STRING, NULL, op);
}
// convert attribute
// locate all the qualifying tuples using a filtered HeapFileScan.
else if (type == INTEGER){
	attrValueInt = atoi(attrValue);
	ptr = &(attrValueInt);
	status = scanner.startScan(desc.attrOffset,desc.attrLen, INTEGER, reinterpret_cast<char*> (ptr), op);
}
else if (type == FLOAT){
	attrValueFloat = atof(attrValue);
	ptr = &(attrValueFloat);
	status = scanner.startScan(desc.attrOffset,desc.attrLen, FLOAT, reinterpret_cast<char*> (ptr), op);
}
else if (type == STRING){
	status = scanner.startScan(desc.attrOffset,desc.attrLen, STRING, attrValue, op);
}
else{
	throw std::invalid_argument( "Error!!!!!!!!" );
}
if (status != OK){
	return status;
}

// Iterate over records and delete
RID rid;
while (true) {
	status = scanner.scanNext(rid);
	if (status == FILEEOF){
		break;
	}
	if (status != OK){
		return status;
	}
	status = scanner.deleteRecord();
	if (status != OK){
		return status;
	}
}
status = scanner.endScan();
if (status != OK){
	return status;
}
return OK;
}