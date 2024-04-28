#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/**
 * Selects records from the specified relation.
 * 
 * A selection is implemented using a filtered HeapFileScan.  
 * The result of the selection is stored in the result relation 
 * called result (a  heapfile with this name will be created by 
 * the parser before QU_Select() is called).  The project list 
 * is defined by the parameters projCnt and projNames.  
 * Projection should be done on the fly as each result tuple 
 * is being appended to the result table.  A final note: the 
 * search value is always supplied as the character string attrValue.  
 * You should convert it to the proper type based on the type of attr. 
 * You can use the atoi() function to convert a char* to an integer and 
 * atof() to convert it to a float.   
 * If attr is NULL, an unconditional scan of the input table should 
 * be performed.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	// Make sure to give ScanSelect the proper input
	// To go from attrInfo to attrDesc, need to consult the catalog (attrCat and relCat, global variables)
	// go through the projection list and look up each in the attr catalog to get an AttrDesc structure (for offset, length, etc)
	Status status;
	AttrDesc *desc = new AttrDesc[projCnt];
	int length = 0;
	AttrCatalog cat(status);
	if (status != OK){
		cout<<"1";
		return status;
	}
	for (int i = 0; i < projCnt; ++i) {
		//get the offset and length
		status = cat.getInfo(projNames[i].relName, projNames[i].attrName, desc[i]);
		if (status != OK){
			cout<<"2";
			return status;
		}
		length += desc[i].attrLen;
	}
	//If attr is NULL, an unconditional scan of the input table should be performed.
	AttrDesc attrDesc;
	if (attr == NULL){
		//TODO how to set it up??
		attrDesc.attrLen = 0;
		//attrDesc.attrName = NULL;
		attrDesc.attrName[0] = NULL;
		attrDesc.attrOffset = 0;
		attrDesc.attrType = STRING;
		//attrDesc.relName = NULL;
		strcpy(attrDesc.relName, projNames[0].relName);
		attrValue = NULL;
	}
	else{
		status = cat.getInfo(attr->relName, attr->attrName, attrDesc);
		if (status != OK){
			cout<<"3";
			return status;
		}
	}
	status = ScanSelect(result, projCnt, desc, &attrDesc, op, attrValue, length);
	if (status != OK){
		cout<<"4";
		return status;
	}
	return OK;
}

const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
	// have a temporary record for output table

	// open "result" as an InsertFileScan object
	InsertFileScan fileScanner(result, status);
	if (status != OK){
		cout<<"5";
		return status;
	}
	// open current table (to be scanned) as a HeapFileScan object + check if an unconditional scan is required
	HeapFileScan heapScanner(attrDesc->relName, status);
	if (status != OK){
		cout<<"6";
		return status;
	}
	// check attrType: INTEGER, FLOAT, STRING
	//if (attrDesc->attrName == NULL){
	if (filter == NULL){
	//scan without filtering
	//Set startScan’s offset and length to 0, type to string, filter to NULL
		status = heapScanner.startScan(0,0, STRING, nullptr, op);
	}
	// convert attribute
	// locate all the qualifying tuples using a filtered HeapFileScan.
	else if (attrDesc->attrType == INTEGER){
		int attrValueInt = atoi(filter);
		void *ptr = &(attrValueInt);
		status = heapScanner.startScan(attrDesc->attrOffset,attrDesc->attrLen, INTEGER, reinterpret_cast<char*> (ptr), op);
	}
	else if (attrDesc->attrType == FLOAT){
		float attrValueFloat = atof(filter);
		void *ptr = &(attrValueFloat);
		status = heapScanner.startScan(attrDesc->attrOffset,attrDesc->attrLen, FLOAT, reinterpret_cast<char*> (ptr), op);
	}
	else if (attrDesc->attrType == STRING){
		status = heapScanner.startScan(attrDesc->attrOffset,attrDesc->attrLen, STRING, filter, op);
	}
	else{
		throw std::invalid_argument( "Error!!!!!!!!" );
	}
	if (status != OK){
		cout<<"7";
		return status;
	}

	// scan the current table
	// if find a record, then copy stuff over to the temporary record (memcpy)
	// insert into the output table
	RID rid;
	Record record; 
	while (true) {
		Record outRecord;
		outRecord.data = new char[reclen];
        outRecord.length = reclen;
		int offset = 0;
		RID outRid;
		status = heapScanner.scanNext(rid);
		if (status == FILEEOF){
			break;
		}
		if (status != OK){
			cout<<"8";
			return status;
		}
		status = heapScanner.getRecord(record);
		if (status != OK){
			cout<<"9";
			return status;
		}
		for (int i = 0; i < projCnt; i++) {
            memcpy((char*)outRecord.data + offset, (char*)record.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
		}
		status = fileScanner.insertRecord(outRecord, outRid);
		if (status != OK){
			cout<<"10";
			return status;
		}
	}

	status = heapScanner.endScan();
	if (status != OK){
		cout<<"11";
		return status;
	}
	return OK;
}
