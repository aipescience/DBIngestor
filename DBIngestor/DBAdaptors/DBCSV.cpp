/*  
 *  Copyright (c) 2012 - 2014, Adrian M. Partl <apartl@aip.de>, 
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

#include "DBCSV.h"
#include "SchemaItem.h"
#include "dbingestor_error.h"
#include "DBType.h"
#include "DType.h"
#include <string.h>
#include <stdio.h>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/xpressive/xpressive.hpp>
#ifndef _WIN32
#include <stdint.h>
#else
#include "stdint_win.h"
#endif
#include "DBCommon.h"

using namespace DBServer;
using namespace std;

DBCSV::DBCSV() {
    supportsSchemaRetrieval = false;
    fileHandler = NULL;
    wroteHeader = false;
}

DBCSV::~DBCSV() {
    if(fileHandler != NULL) {
        fclose(fileHandler);
    }
}

//we define that the socket will become the file name of the file to be written
int DBCSV::connect(string usr, string pwd, string host, string port, string socket) {
    fileName = socket;

    if(socket.length() == 0) {
        printf("Error CSV:\n");
        DBIngestor_error("DBCSV: you need to specify a 'socket' to be used as file name\n", NULL);
    }

    fileHandler = fopen(socket.c_str(), "w");

    if(fileHandler == NULL) {
        printf("Error CSV:\n");
        DBIngestor_error("DBCSV: could not open CSV file for writing\n", NULL);
    }
    
    return 1;
}

int DBCSV::disconnect() {
    if(fileHandler != NULL) {
        fclose(fileHandler);
    }
    
    return 1;
}

int DBCSV::setSavepoint() {
    return 1;
}

int DBCSV::rollback() {
    return 1;
}

int DBCSV::releaseSavepoint() {
    return 1;
}

int DBCSV::disableKeys(DBDataSchema::Schema * thisSchema) {
    return 1;
}

int DBCSV::enableKeys(DBDataSchema::Schema * thisSchema) {
    return 1;
}


DBDataSchema::Schema * DBCSV::getSchema(string database, string table) {
    DBDataSchema::Schema * retSchema = new DBDataSchema::Schema;
    
    retSchema->setDbName(database);
    retSchema->setTableName(table);
        
    return retSchema;
}

void* DBCSV::prepareIngestStatement(DBDataSchema::Schema * thisSchema) {
    return NULL;    
}

void* DBCSV::prepareMultiIngestStatement(DBDataSchema::Schema * thisSchema, int numElements) {
	return NULL;  
}

int DBCSV::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData) {
    //construct query string
    string query = "";
    
    //insert column names
    if(wroteHeader == false) {
        for(int i=0; i<thisSchema->getArrSchemaItems().size(); i++) {
            query.append(thisSchema->getArrSchemaItems().at(i)->getColumnName());
            if (i != thisSchema->getArrSchemaItems().size() - 1) {
                query.append(", ");
            } else {
                query.append("\n");
            }
        }

        fprintf(fileHandler, "%s\n", query.c_str());

        wroteHeader = true;

        query.clear();
    }
    
    //add the data to the querry string
    long byteCount = 0;
    
    for(int i=0; i<thisSchema->getArrSchemaItems().size(); i++) {
        DBDataSchema::DataObjDesc * currObj = thisSchema->getArrSchemaItems().at(i)->getDataDesc();        
        unsigned long strLen;
        char * theString;
        
        switch (currObj->getDataObjDType()) {
            case DBDataSchema::DT_STRING:
                theString = *(char**)(thisData+byteCount);
                strLen = strlen(theString);
                query.append("\"");
                query.append(theString, strLen);
                query.append("\"");
                byteCount += sizeof(char*);
                break;
            case DBDataSchema::DT_INT1:
                query.append(boost::str(boost::format("%hd") % *(int8_t*)(thisData+byteCount)));
                byteCount += sizeof(int8_t);
                break;
            case DBDataSchema::DT_INT2:
                query.append(boost::str(boost::format("%hd") % *(int16_t*)(thisData+byteCount)));
                byteCount += sizeof(int16_t);
                break;
            case DBDataSchema::DT_INT4:
                query.append(boost::str(boost::format("%d") % *(int32_t*)(thisData+byteCount)));
                byteCount += sizeof(long);
                break;
            case DBDataSchema::DT_INT8:
                query.append(boost::str(boost::format("%lld") % *(int64_t*)(thisData+byteCount)));
                byteCount += sizeof(int64_t);
                break;
            case DBDataSchema::DT_REAL4:
                //for safety in the cast below
                query.append(boost::str(boost::format("%f") % *(float*)(thisData+byteCount)));
                byteCount += sizeof(float);
                break;
            case DBDataSchema::DT_REAL8:
                query.append(boost::str(boost::format("%lf") % *(double*)(thisData+byteCount)));
                byteCount += sizeof(double);
                break;
            default:
                query.clear();
                return 0;
        }
        
        if (i != thisSchema->getArrSchemaItems().size() - 1) {
            query.append(", ");
        } else {
            query.append("\n");
        }
    }
    
    fprintf(fileHandler, "%s\n", query.c_str());

    return 1;    
}

int DBCSV::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData, void* preparedStatement) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    insertOneRow(thisSchema, thisData);
    
    return 1;
}

int DBCSV::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, void* preparedStatement, int nInStmt) {
    insertOneRow(thisSchema, &thisData);
    
    return 1;
}

//this can handle NULL values
int DBCSV::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, bool* isNullArray, void* preparedStatement, int nInStmt) {
    //construct query string
    string query = "";
    
    //insert column names
    if(wroteHeader == false) {
        for(int i=0; i<thisSchema->getArrSchemaItems().size(); i++) {
            query.append(thisSchema->getArrSchemaItems().at(i)->getColumnName());
            if (i != thisSchema->getArrSchemaItems().size() - 1) {
                query.append(", ");
            } else {
                query.append("\n");
            }
        }

        fprintf(fileHandler, "%s\n", query.c_str());

        wroteHeader = true;

        query.clear();
    }
    
    //add the data to the querry string
    long byteCount = 0;
    
    for(int i=0; i<thisSchema->getArrSchemaItems().size(); i++) {
        DBDataSchema::DataObjDesc * currObj = thisSchema->getArrSchemaItems().at(i)->getDataDesc();        
        unsigned long strLen;
        char * theString;

        if(isNullArray[i] == 1) {
            //adding empty value here
            query.append(", ");
            continue;
        }

        switch (currObj->getDataObjDType()) {
            case DBDataSchema::DT_STRING:
                theString = *(char**)(thisData+byteCount);
                strLen = strlen(theString);
                query.append("\"");
                query.append(theString, strLen);
                query.append("\"");
                byteCount += sizeof(char*);
                break;
            case DBDataSchema::DT_INT1:
                query.append(boost::str(boost::format("%hd") % *(int8_t*)(thisData+byteCount)));
                byteCount += sizeof(int8_t);
                break;
            case DBDataSchema::DT_INT2:
                query.append(boost::str(boost::format("%hd") % *(int16_t*)(thisData+byteCount)));
                byteCount += sizeof(int16_t);
                break;
            case DBDataSchema::DT_INT4:
                query.append(boost::str(boost::format("%d") % *(int32_t*)(thisData+byteCount)));
                byteCount += sizeof(long);
                break;
            case DBDataSchema::DT_INT8:
                query.append(boost::str(boost::format("%lld") % *(int64_t*)(thisData+byteCount)));
                byteCount += sizeof(int64_t);
                break;
            case DBDataSchema::DT_REAL4:
                //for safety in the cast below
                query.append(boost::str(boost::format("%f") % *(float*)(thisData+byteCount)));
                byteCount += sizeof(float);
                break;
            case DBDataSchema::DT_REAL8:
                query.append(boost::str(boost::format("%lf") % *(double*)(thisData+byteCount)));
                byteCount += sizeof(double);
                break;
            default:
                query.clear();
                return 0;
        }
        
        if (i != thisSchema->getArrSchemaItems().size() - 1) {
            query.append(", ");
        } else {
            query.append("\n");
        }
    }
    
    fprintf(fileHandler, "%s\n", query.c_str());
    
    return 1;
}

int DBCSV::executeStmt(void* preparedStatement) {
    return 1;
}

int DBCSV::finalizePreparedStatement(void* preparedStatement) {
    return 1;
}

int DBCSV::maxRowsPerStmt(DBDataSchema::Schema * thisSchema) {
    return 1;
}

void * DBCSV::initGetCompleteTable(DBDataSchema::Schema * thisSchema) {
    return NULL;
}

int DBCSV::getNextRow(DBDataSchema::Schema * thisSchema, void* thisData, void * preparedStatement) {
    return 0;
}


