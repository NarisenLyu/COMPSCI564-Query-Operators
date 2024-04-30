#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * must use catalogue to make sure the offset is collect
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// cout<<"Inside Insert";
// part 6
Status status;
RelDesc relDesc;

// check the given attributes
if(relation.empty() || attrCnt == NULL){
	return BADCATPARM;
} 

// attribute count must be same as relation count
status = relCat->getInfo(relation, relDesc);
if(status != OK) {return status;}
int relCnt = relDesc.attrCnt;
if(relCnt != attrCnt) {return BADCATPARM;}

// unpack info, checking the values' existance
AttrDesc descriptions[attrCnt];
int recordLength = 0;
for (int i = 0; i<attrCnt; i++){
	// cout<<"Inside first for";

	// add file into temp array
	status = attrCat->getInfo(attrList[i].relName, attrList[i].attrName, descriptions[i]);
	// remove if the input is jenky.
	if(status != OK){ 
		return status;
	}
	if (attrList[i].attrValue == NULL){
		return BADCATPARM;
	}

	// get the total length of my record
	recordLength += descriptions[i].attrLen; 
}


// get the data and put it into recordData. Need to convert all to chars.
char recordData[recordLength];
char* toInsert;
char* toConvert;
int* intPointer;
float* floatPointer;
for (int i = 0; i < attrCnt; i++){
	toConvert = (char*)attrList[i].attrValue;
	if(descriptions[i].attrType == INTEGER){
		int tmp = atoi(toConvert);
		intPointer = &tmp;
		toInsert = (char*) intPointer;
		// value = (char*)attrList[i].attrValue;
		
	} else if(descriptions[i].attrType == FLOAT){
		float tmp = (float)atof(toConvert);
		floatPointer = &tmp;
		toInsert = (char*) floatPointer;
		// value = (char*)attrList[i].attrValue;
	} else{
		toInsert = (char*) attrList[i].attrValue;
	}
	memcpy((recordData + descriptions[i].attrOffset), toInsert, descriptions[i].attrLen);
}

// take the data and make a record
Record record;
record.data = recordData;
record.length = recordLength;
RID myRID;

// insert file
InsertFileScan fileScan(relation, status);
if(status != OK) {return status;}
status = fileScan.insertRecord(record, myRID);
if(status != OK) {return status;}

return OK;

}


