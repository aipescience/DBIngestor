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

#include "DBSqlite3.h"
#include "SchemaItem.h"
#include "dbingestor_error.h"
#include "DBType.h"
#include "DType.h"
#include <string.h>
#include <stdio.h>
#include <boost/format.hpp>
#ifndef _WIN32
#include <stdint.h>
#else
#include "stdint_win.h"
#endif
#include "DBCommon.h"

using namespace DBServer;
using namespace std;

#define AING_SQLITE_LENQUERRYBUFFER 1000

DBSqlite3::DBSqlite3() {
    dbHandler = NULL;
}

DBSqlite3::~DBSqlite3() {
    if(dbHandler != NULL) {
        disconnect();
    }
}

int DBSqlite3::connect(string usr, string pwd, string host, string port, string socket) {
    int err;
    
	err = sqlite3_open(host.c_str(), &dbHandler);
    if (err != SQLITE_OK) {
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3: could not open a connection to the SQLite3 database\n");
    }
    
    isConnected = true;
    
    return 1;
}

int DBSqlite3::disconnect() {
	sqlite3_close(dbHandler);
    
    dbHandler = NULL;
    
    isConnected = false;
    
    return 1;
}

int DBSqlite3::setSavepoint() {
    int err;
    
    err = sqlite3_exec(dbHandler, "SAVEPOINT dbIngst_sqlite_sp", NULL, NULL, NULL);

	if(err != SQLITE_OK) {
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3: could not set savepoint.\n");
    }
    
    return 1;
}

int DBSqlite3::rollback() {
    int err;
    
    err = sqlite3_exec(dbHandler, "ROLLBACK TO SAVEPOINT dbIngst_sqlite_sp", NULL, NULL, NULL);
    
	if(err != SQLITE_OK) {
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3: rollback not successfull.\n");
    }
    
    return 1;
}

int DBSqlite3::releaseSavepoint() {
    int err;
    
    err = sqlite3_exec(dbHandler, "RELEASE SAVEPOINT dbIngst_sqlite_sp", NULL, NULL, NULL);
    
	if(err != SQLITE_OK) {
        printf("%s\n", sqlite3_errmsg(dbHandler));
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3: savepoint not realeased successfully.\n");
    }
    
    return 1;
}

int DBSqlite3::disableKeys(DBDataSchema::Schema * thisSchema) {
    return 1;
}

int DBSqlite3::enableKeys(DBDataSchema::Schema * thisSchema) {
    return 1;
}

DBDataSchema::Schema * DBSqlite3::getSchema(string database, string table) {
    //SQLite3 apparantly does not support multiple database domains
    //Therefore the database string is ignored
    char queryString[AING_SQLITE_LENQUERRYBUFFER];
    
    sqlite3_stmt *statement;
    int err;
    DBDataSchema::Schema * retSchema = new DBDataSchema::Schema;
    
    //construct query
    sprintf(queryString, "PRAGMA table_info(%s);", table.c_str());
    
    //check if table exists
    err = sqlite3_prepare_v2(dbHandler, queryString, -1, &statement, NULL);
    
    if(err != SQLITE_OK) {
        printf("DBSqlite3: Error");
        printf("%s\n", sqlite3_errmsg(dbHandler));
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - getSchema: an error occured in getSchema\n");
    }
    
    //check if any result has been returned 
    if(sqlite3_column_count(statement) != 6) {
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - getSchema: table not found");
    }
    
    retSchema->setDbName(database);
    retSchema->setTableName(table);
    
    //loop through the results and create Schema item
    while(true) {
        err = sqlite3_step(statement);
        
        if(err == SQLITE_ROW) {
            DBDataSchema::SchemaItem * newItem = new DBDataSchema::SchemaItem;
            
            newItem->setColumnName((char*)sqlite3_column_text(statement, 1));
            newItem->setColumnDBType(DBDataSchema::DBT_ANY);
            //newItem->setDataDesc(NULL);
            
            retSchema->addItemToSchema(newItem);            
        } else {
            break;
        }
    }
    
    sqlite3_finalize(statement);
    
    return retSchema;
}

void* DBSqlite3::prepareIngestStatement(DBDataSchema::Schema * thisSchema) {
    int err;
    sqlite3_stmt *statement;
    
    //construct query string
    string query = "INSERT INTO ";
    query.append(thisSchema->getTableName());
    query.append("(");
    
    //insert column names
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (j != thisSchema->getNumActiveItems() - 1) {
            query.append(", ");
        } else {
            query.append(") VALUES (");
        }
        
        i++;
    }
    
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        query.append("?");
        if (j != thisSchema->getNumActiveItems() - 1) {
            query.append(", ");
        } else {
            query.append(")");
        }
    }
    
    //printf("%s\n", query.c_str());
    
    err = sqlite3_prepare_v2(dbHandler, query.c_str(), -1, &statement, NULL);
    
    if(err != SQLITE_OK) {
        printf("DBSqlite3: Error\n");
        printf("%s\n", sqlite3_errmsg(dbHandler));
        printf("Statement: %s\n", query.c_str());
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - prepareIngestStatement: an error occured in prepareIngestStatement\n");
    }
    
    return (void*)statement;    
}

void* DBSqlite3::prepareMultiIngestStatement(DBDataSchema::Schema * thisSchema, int numElements) {
    if(numElements > maxRowsPerStmt(thisSchema)) {
        printf("DBSqlite3: Error\n");
        printf("SQLITE_LIMIT_COMPOUND_SELECT: %i\n", maxRowsPerStmt(thisSchema));
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - prepareMultiIngestStatement: SQLITE_LIMIT_COMPOUND_SELECT has been violated.\n");
        
    }
    
    //according to stackoverflow question 1609637
    int err;
    sqlite3_stmt *statement;
    
    //construct query string
    string query = "INSERT INTO ";
    query.append(thisSchema->getTableName());
    query.append("(");
    
    //insert column names
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(i)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append(", ");
        } else {
            query.append(") SELECT ");
        }
        
        i++;
    }
    
    //insert column names
    for(int i=0; i<thisSchema->getNumActiveItems(); i++) {
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append(boost::str(boost::format("? as v%i, ") % i));
        } else {
            query.append(boost::str(boost::format("? as v%i ") % i));
        }
    }
    
    for(int j=0; j<numElements-1; j++) {
        query.append("UNION SELECT ");
        for(int i=0; i<thisSchema->getNumActiveItems(); i++) {
            query.append("?");
            if (i != thisSchema->getNumActiveItems() - 1) {
                query.append(", ");
            } else {
                query.append(" ");
            }
        }
    }
    
    //printf("%s\n", query.c_str());
    
    err = sqlite3_prepare_v2(dbHandler, query.c_str(), -1, &statement, NULL);
    
    if(err != SQLITE_OK) {
        printf("DBSqlite3: Error\n");
        printf("%s\n", sqlite3_errmsg(dbHandler));
        printf("Statement: %s\n", query.c_str());
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - prepareMultiIngestStatement: an error occured in prepareIngestStatement\n");
    }
    
    return (void*)statement;    
}

int DBSqlite3::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData) {
    int err;
    
    string query;
    
    query = buildOneRowInsertString(thisSchema, thisData, DBTYPE_SQLITE);
    
    if(query.size() == 0) {
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - insertOneRow: an error occured in insertOneRow in switch while binding\n");
    }
    
    err = sqlite3_exec(dbHandler, query.c_str(), NULL, NULL, NULL);

    if(err != SQLITE_OK) {
        printf("DBSqlite3: Error\n");
        printf("%s\n", sqlite3_errmsg(dbHandler));
        printf("Statement: %s\n", query.c_str());
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - insertOneRow without prepared statement: an error occured in the ingest.\n");
    }
    
    return 1;    
}

int DBSqlite3::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData, void* preparedStatement) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    sqlite3_stmt *statement;
    
    statement = (sqlite3_stmt*)preparedStatement;
    
    bindOneRowToStmt(thisSchema, thisData, statement, 0);    

    executeStmt((void*)statement);

    return 1;
}

int DBSqlite3::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, void* preparedStatement, int nInStmt) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    int err;
    sqlite3_stmt *statement = (sqlite3_stmt*) preparedStatement;
    long byteCount = 0;
    int stride = nInStmt * (int)thisSchema->getNumActiveItems();
    char * currRow = (char*)thisData;
    
    //bind data to the prepared statement
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        DBDataSchema::DataObjDesc * currObj = thisSchema->getArrSchemaItems().at(j)->getDataDesc();        
        unsigned long strLen;
        char * theString;
        int8_t tmpVal3;
        int16_t tmpVal;
        float tmpVal2;
        int8_t tmpVal4;
        int16_t tmpVal5;
        int32_t tmpVal6;
        int64_t tmpVal7;
        
        switch (currObj->getDataObjDType()) {
            case DBDataSchema::DT_STRING:
                theString = *(char**)(currRow+byteCount);
                strLen = strlen(theString);
                err = sqlite3_bind_text(statement, stride+i+1, theString, (int)strLen, SQLITE_TRANSIENT);
                byteCount += strLen + 1;
                break;
            case DBDataSchema::DT_INT1:
                //for safety in the cast below
                memcpy(&tmpVal3, (int8_t*)(currRow+byteCount), sizeof(int8_t));
                err = sqlite3_bind_int(statement, stride+i+1, (int)tmpVal3);
                byteCount += sizeof(int8_t);
                break;
            case DBDataSchema::DT_INT2:
                //for safety in the cast below
                memcpy(&tmpVal, (int16_t*)(currRow+byteCount), sizeof(int16_t));
                err = sqlite3_bind_int(statement, stride+i+1, (int)tmpVal);
                byteCount += sizeof(int16_t);
                break;
            case DBDataSchema::DT_INT4:
                err = sqlite3_bind_int(statement, stride+i+1, *(int32_t*)(currRow+byteCount));
                byteCount += sizeof(int32_t);
                break;
            case DBDataSchema::DT_INT8:
                err = sqlite3_bind_int64(statement, stride+i+1, *(int64_t*)(currRow+byteCount));
                byteCount += sizeof(int64_t);
                break;
            // Sqlite3 has troubles with unsigned stuff... casting to signed type... THIS IS A LIMITATION!
            case DBDataSchema::DT_UINT1:
                //for safety in the cast below
                memcpy(&tmpVal4, (uint8_t*)(currRow+byteCount), sizeof(uint8_t));
                err = sqlite3_bind_int(statement, stride+i+1, (int)tmpVal4);
                byteCount += sizeof(uint8_t);
                break;
            case DBDataSchema::DT_UINT2:
                //for safety in the cast below
                memcpy(&tmpVal5, (uint16_t*)(currRow+byteCount), sizeof(uint16_t));
                err = sqlite3_bind_int(statement, stride+i+1, (int)tmpVal5);
                byteCount += sizeof(uint16_t);
                break;
            case DBDataSchema::DT_UINT4:
                //for safety in the cast below
                memcpy(&tmpVal6, (uint32_t*)(currRow+byteCount), sizeof(uint32_t));
                err = sqlite3_bind_int(statement, stride+i+1, (int)tmpVal6);
                byteCount += sizeof(uint32_t);
                break;
            case DBDataSchema::DT_UINT8:
                //for safety in the cast below
                memcpy(&tmpVal7, (uint64_t*)(currRow+byteCount), sizeof(uint64_t));
                err = sqlite3_bind_int64(statement, stride+i+1, (int64_t)tmpVal7);
                byteCount += sizeof(uint64_t);
                break;
            case DBDataSchema::DT_REAL4:
                //for safety in the cast below
                memcpy(&tmpVal2, (float*)(currRow+byteCount), sizeof(float));
                err = sqlite3_bind_double(statement, stride+i+1, (double)tmpVal2);
                byteCount += sizeof(float);
                break;
            case DBDataSchema::DT_REAL8:
                err = sqlite3_bind_double(statement, stride+i+1, *(double*)(currRow+byteCount));
                byteCount += sizeof(double);
                break;
            default:
                sqlite3_close(dbHandler);
                DBIngestor_error("DBSqlite3 - bindOneRowToStmt: an error occured in bindOneRowToStmt in switch while binding\n");
        }
        
        if(err != SQLITE_OK) {
            sqlite3_close(dbHandler);
            DBIngestor_error("DBSqlite3 - bindOneRowToStmt: an error occured in bindOneRowToStmt while binding\n");
        }
        
        i++;
    }
    
    return 1;
}

int DBSqlite3::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, bool* isNullArray, void* preparedStatement, int nInStmt) {
    
    bindOneRowToStmt(thisSchema, thisData, preparedStatement, nInStmt);
}

int DBSqlite3::executeStmt(void* preparedStatement) {
    assert(preparedStatement != NULL);
    int err;
    sqlite3_stmt *statement = (sqlite3_stmt*) preparedStatement;
    
    err = sqlite3_step(statement);
    if(err != SQLITE_DONE) {
        printf("DBSqlite3: Error\n");
        printf("%s\n", sqlite3_errmsg(dbHandler));
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - executeStatement: error in step.\n");
    }
    
    err = sqlite3_reset(statement);
    if(err != SQLITE_OK) {
        printf("DBSqlite3: Error\n");
        printf("%s\n", sqlite3_errmsg(dbHandler));
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - executeStatement: error in reset.\n");
    }
    
    return 1;
}

int DBSqlite3::finalizePreparedStatement(void* preparedStatement) {
    assert(preparedStatement != NULL);
    int err;
    sqlite3_stmt *statement = (sqlite3_stmt*) preparedStatement;
    
    err = sqlite3_finalize(statement);
    if(err != SQLITE_OK) {
        printf("DBSqlite3: Error\n");
        printf("%s\n", sqlite3_errmsg(dbHandler));
        sqlite3_close(dbHandler);
        DBIngestor_error("DBSqlite3 - finalizePreparedStatement: error in finalize.\n");
    }

    return 1;
}

int DBSqlite3::maxRowsPerStmt(DBDataSchema::Schema * thisSchema) {
    return sqlite3_limit(dbHandler, SQLITE_LIMIT_COMPOUND_SELECT, -1);
}

