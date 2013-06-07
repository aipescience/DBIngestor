/*  
 *  Copyright (c) 2012, Adrian M. Partl <apartl@aip.de>, 
 *                      eScience team AIP Potsdam
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. You may obtain a copy
 *  of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "DBIngestBuffer.h"
#include "dbingestor_error.h"
#include <assert.h>
#include "DBType.h"
#include <string.h>
#include <stdlib.h>

using namespace DBIngest;
using namespace std;

DBIngestBuffer::DBIngestBuffer() {
    currSize = 0;
    myDBSchema = NULL;
    myDBAbstractor = NULL;
    bufferArray = NULL;
    isNullArray = NULL;
    colLookupArray = NULL;
    preparedStmt = NULL;
    lenPreparedStmt = 0;
    preparedStmtRemain = NULL;
    lenPreparedStmtRemain = 0;
    basicSizeRow = 0;
    currRowItemId = 0;
    
    setBufferSize(1);
}

DBIngestBuffer::~DBIngestBuffer() {
    if(preparedStmt != NULL) {
        myDBAbstractor->finalizePreparedStatement(preparedStmt);
    }
    
    if(bufferArray != NULL) {
        clear();
        free(bufferArray);
    }

    if(isNullArray != NULL) {
        free(isNullArray);
    }

    if(colLookupArray != NULL) {
        free(colLookupArray);
    }
}


DBIngestBuffer::DBIngestBuffer(DBDataSchema::Schema * newSchema, DBServer::DBAbstractor * newDBAbstractor) {
    assert(newSchema != NULL);
    assert(newDBAbstractor != NULL);

    currSize = 0;
    myDBSchema = NULL;
    myDBAbstractor = NULL;
    bufferArray = NULL;
    isNullArray = NULL;
    colLookupArray = NULL;
    preparedStmt = NULL;
    lenPreparedStmt = 0;
    preparedStmtRemain = NULL;
    lenPreparedStmtRemain = 0;
    basicSizeRow = 0;
    currRowItemId = 0;
    
    setBufferSize(1);

    setDBSchema(newSchema);
    setDBAbstractor(newDBAbstractor);
}

int DBIngestBuffer::newRow() {
    assert(bufferArray != NULL);
    assert(colLookupArray != NULL);
    assert(basicSizeRow > 0);
    
    //if buffer is full, commit
    if(currSize >= bufferSize) {
        if(isDryRun != true) {
            commit();
        }
        
        clear();
    }
    
    if(bufferArray[currSize] == NULL) {
        bufferArray[currSize] = (void*)malloc(basicSizeRow);
        isNullArray[currSize] = (bool*)malloc(myDBSchema->getNumActiveItems() * sizeof(bool));
        if(bufferArray[currSize] == NULL || isNullArray[currSize] == NULL) {
            DBIngestor_error("DBIngestBuffer: Not enough memory for allocating new row in the buffer.\n");
        }
    }
    
    //set stuff to 0
    memset(bufferArray[currSize], 0, basicSizeRow);
    memset((void*)isNullArray[currSize], 0, myDBSchema->getNumActiveItems() * sizeof(bool));
    
    currSize++;
    currRowItemByte = 0;
    currRowItemId = 0;
    
	return 1;
}

int DBIngestBuffer::addToRow(void* value, bool isNull, DBDataSchema::SchemaItem * thisSchemaItem) {
    assert(bufferArray != NULL);
    assert(colLookupArray != NULL);
    assert(basicSizeRow > 0);
    assert(value != NULL);
    assert(thisSchemaItem != NULL);
    
    //adding value to the current buffer row
    DBDataSchema::DataObjDesc * currObj = thisSchemaItem->getDataDesc();
    char * currRow = (char*)bufferArray[currSize-1];
    
    if(isNull == false) {
        castDTypeToDBType(value, currObj->getDataObjDType(), thisSchemaItem->getColumnDBType(), (currRow+currRowItemByte));
        isNullArray[currSize-1][currRowItemId] = 0;
    } else {
        //check if this row can be null
        if(thisSchemaItem->getIsNotNull() == true)
            return 0;
        
        isNullArray[currSize-1][currRowItemId] = 1;
    }
        
    currRowItemByte += getByteLenOfDBType(thisSchemaItem->getColumnDBType());
    currRowItemId++;
    
    return 1;
}

int DBIngestBuffer::clear() {
    int32_t numStrings = 0;
    
    //first find all the strings in the Schema to free, free them, then free the rows
    for(int i=0; i<myDBSchema->getArrSchemaItems().size(); i++) {
        if(myDBSchema->getArrSchemaItems().at(i)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        DBDataSchema::DataObjDesc * currObj = myDBSchema->getArrSchemaItems().at(i)->getDataDesc();        
        
        if(currObj->getDataObjDType() == DBDataSchema::DT_STRING) {
            numStrings++;
        }
    }
    
    int32_t * arrayOfStrings = (int32_t*)malloc(numStrings*sizeof(int32_t));
    if(arrayOfStrings == NULL) {
        DBIngestor_error("DBIngestBuffer - clear: could not allocate array for freeing strings.\n");
    }
    
    int counter = 0;
    currRowItemByte = 0;
    currRowItemId = 0;
    for(int i=0; i<myDBSchema->getArrSchemaItems().size(); i++) {
        if(myDBSchema->getArrSchemaItems().at(i)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        DBDataSchema::DataObjDesc * currObj = myDBSchema->getArrSchemaItems().at(i)->getDataDesc();        

        if(currObj->getDataObjDType() == DBDataSchema::DT_STRING) {
            arrayOfStrings[counter] = currRowItemByte;
            counter++;
        }

        switch (currObj->getDataObjDType()) {
            case DBDataSchema::DT_STRING:
                currRowItemByte += sizeof(char*) / sizeof(char);
                break;
            case DBDataSchema::DT_INT1:
                currRowItemByte += sizeof(int8_t) / sizeof(char);
                break;
            case DBDataSchema::DT_INT2:
                currRowItemByte += sizeof(int16_t) / sizeof(char);
                break;
            case DBDataSchema::DT_INT4:
                currRowItemByte += sizeof(int32_t) / sizeof(char);
                break;
            case DBDataSchema::DT_INT8:
                currRowItemByte += sizeof(int64_t) / sizeof(char);
                break;
            case DBDataSchema::DT_UINT1:
                currRowItemByte += sizeof(int8_t) / sizeof(char);
                break;
            case DBDataSchema::DT_UINT2:
                currRowItemByte += sizeof(int16_t) / sizeof(char);
                break;
            case DBDataSchema::DT_UINT4:
                currRowItemByte += sizeof(int32_t) / sizeof(char);
                break;
            case DBDataSchema::DT_UINT8:
                currRowItemByte += sizeof(int64_t) / sizeof(char);
                break;
            case DBDataSchema::DT_REAL4:
                currRowItemByte += sizeof(float) / sizeof(char);
                break;
            case DBDataSchema::DT_REAL8:
                currRowItemByte += sizeof(double) / sizeof(char);
                break;

        }
    }
    
    //loop through all the rows and free the strings
    for(int i=0; i<currSize; i++) {
        char * currRow = (char*)bufferArray[i];
        
        for(int j=0; j<numStrings; j++) {
            char * currString = *(char**)(currRow+arrayOfStrings[j]);
            free(currString);
        }
    }

    currSize = 0;
    
    free(arrayOfStrings);
    
    return 1;
}

int DBIngestBuffer::commit() {
    assert(myDBAbstractor != NULL);
    assert(bufferSize > 0);
    
    if(currSize <= 0) {
        return 0;
    }
    
    //check that a prepared statement exisits
    if(preparedStmt == NULL) {
        initPreparedStmt(min(bufferSize, myDBAbstractor->maxRowsPerStmt(myDBSchema)));
    }
    
    int numLoops = (int)((float)currSize/(float)lenPreparedStmt);
    int remainder = currSize % lenPreparedStmt;
    
    for(int i=0; i<numLoops; i++) {
        for(int j=0; j<lenPreparedStmt; j++) {
            myDBAbstractor->bindOneRowToStmt(myDBSchema, (void*)bufferArray[i*lenPreparedStmt + j], isNullArray[i*lenPreparedStmt + j], preparedStmt, j);
        }
        
        if(myDBAbstractor->executeStmt(preparedStmt) == -2) {
            initPreparedStmt(min(bufferSize, myDBAbstractor->maxRowsPerStmt(myDBSchema)));

            //resetting preparedStmtRemain through lenPreparedStmtRemain
            lenPreparedStmtRemain = 0;
        }
    }
    
    if(remainder > 0) {
        if(remainder != lenPreparedStmtRemain) {
            //finish this prepared statement off before creating a new one
            if(lenPreparedStmtRemain != 0 && preparedStmtRemain != NULL)
                myDBAbstractor->finalizePreparedStatement(preparedStmtRemain);
            
            preparedStmtRemain = myDBAbstractor->prepareMultiIngestStatement(myDBSchema, remainder);
            lenPreparedStmtRemain = remainder;
        }
        assert(preparedStmtRemain != NULL);
        
        for(int i=0; i<remainder; i++) {
            myDBAbstractor->bindOneRowToStmt(myDBSchema, (void**)bufferArray[numLoops*lenPreparedStmt + i], isNullArray[numLoops*lenPreparedStmt + i], preparedStmtRemain, i);
        }
        
        if(myDBAbstractor->executeStmt(preparedStmtRemain) == -2) {
            initPreparedStmt(min(bufferSize, myDBAbstractor->maxRowsPerStmt(myDBSchema)));
        }
    }
    
    return 1;
}

int DBIngestBuffer::getCurrSize() {
	return currSize;
}

int DBIngestBuffer::getBufferSize() {
	return bufferSize;
}

void DBIngestBuffer::setBufferSize(int newBufferSize) {
    assert(newBufferSize > 0);
    
    if(currSize != 0) {
        DBIngestor_error("DBIngestBuffer: Currently you can only set the buffer size, if the buffer is cleared beforehand.\n");
    }
    
    bufferSize = newBufferSize;
    
    if(bufferArray != NULL) {
        free(bufferArray);
        free(isNullArray);
    }
    
    bufferArray = (void**)malloc(bufferSize * sizeof(void**));
    isNullArray = (bool**)malloc(bufferSize * sizeof(bool**));
    if(bufferArray == NULL || isNullArray == NULL) {
        DBIngestor_error("DBIngestBuffer: Not enough memory for allocating bufferArray.\n");
    }
    
    memset(bufferArray, NULL, bufferSize * sizeof(void*));
    memset(isNullArray, 0, bufferSize * sizeof(bool*));
}

DBDataSchema::Schema * DBIngestBuffer::getDBSchema() {
	return myDBSchema;
}

void DBIngestBuffer::setDBSchema(DBDataSchema::Schema * newSchema) {
    assert(newSchema != NULL);
    
    //allocate the offset lookup array to the size of a row
    if(colLookupArray != NULL) {
        free(colLookupArray);
    }
    
    //count active elements in schema
    int colCount = 0;
    for(int i=0; i<newSchema->getArrSchemaItems().size(); i++) {
        if(newSchema->getArrSchemaItems().at(i)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        colCount++;
    }
    
    colLookupArray = (int64_t *)malloc(colCount * sizeof(int64_t));
    if(colLookupArray == NULL) {
        DBIngestor_error("DBIngestBuffer: Not enough memory for allocating column lookup accellerator array.\n");
    }
    
    //determine minimum size of a row and set the offset lookup array
    int64_t byteCount = 0;
    
    int i = 0;
    for(int j=0; j<newSchema->getArrSchemaItems().size(); j++) {
        if(newSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        DBDataSchema::SchemaItem * currObj = newSchema->getArrSchemaItems().at(j);        
        
        colLookupArray[i] = byteCount / sizeof(char);
        
        if(currObj->getColumnDBType() == DBDataSchema::DBT_CHAR) {
            byteCount += sizeof(char*);
        } else {
            byteCount += DBDataSchema::getByteLenOfDBType(currObj->getColumnDBType());
        }
        
        i++;
    }
    
    basicSizeRow = byteCount;
	myDBSchema = newSchema;
}

DBServer::DBAbstractor * DBIngestBuffer::getDBAbstractor() {
	return myDBAbstractor;
}

void DBIngestBuffer::setDBAbstractor(DBServer::DBAbstractor * newDBAbstractor) {
    assert(newDBAbstractor != NULL);
    
	myDBAbstractor = newDBAbstractor;
}

int DBIngestBuffer::initPreparedStmt(int numRows) {
    assert(numRows > 0);
    assert(myDBSchema != NULL);
    
    preparedStmt = myDBAbstractor->prepareMultiIngestStatement(myDBSchema, numRows);
    
    if(preparedStmt == NULL) {
        DBIngestor_error("DBIngestBuffer: Error in generating prepared statement.\n");
    }
    
    lenPreparedStmt = numRows;
    
    return 1;
}

void DBIngestBuffer::setIsDryRun(bool newIsDryRun) {
    isDryRun = newIsDryRun;
}
