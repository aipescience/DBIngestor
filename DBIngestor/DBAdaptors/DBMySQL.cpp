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

#include "DBMySQL.h"
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

#define AING_MYSQL_LENQUERRYBUFFER 1000

typedef struct {
    MYSQL_STMT *stmt;
    MYSQL_BIND *bind;
    int lenBind;
    bool prepared;
    my_bool isNullTrue;
    my_bool isNullFalse;
} MYSQL_prepStmt;


DBMySQL::DBMySQL() {
    dbHandler = NULL;
}

DBMySQL::~DBMySQL() {
    if(dbHandler != NULL) {
        disconnect();
    }
}

int DBMySQL::connect(string usr, string pwd, string host, string port, string socket) {
    const char * socketStr = NULL;
    
    dbHandler = mysql_init(NULL);
    
    if(dbHandler == NULL) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL: could not initialize MySQL database handler\n");
    }
    
    if(socket.length() != 0) 
        socketStr = socket.c_str();
    
    if(mysql_real_connect(dbHandler, host.c_str(), usr.c_str(), pwd.c_str(), NULL, atoi(port.c_str()), socketStr, 0) == NULL) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL: could not connect to MySQL database\n");
    }
    
    isConnected = true;
    
    return 1;
}

int DBMySQL::disconnect() {
    mysql_close(dbHandler);
    
    dbHandler = NULL;
    
    isConnected = false;
    
    return 1;
}

int DBMySQL::setSavepoint() {
    if(mysql_query(dbHandler, "SAVEPOINT dbIngst_mysql_sp")) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL: could not set savepoint.\n");
    }
    
    return 1;
}

int DBMySQL::rollback() {
    if(mysql_query(dbHandler, "ROLLBACK TO SAVEPOINT dbIngst_mysql_sp")) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL: rollback not successfull.\n");
    }
    
    return 1;
}

int DBMySQL::releaseSavepoint() {
    if(mysql_query(dbHandler, "RELEASE SAVEPOINT dbIngst_sqlite_sp")) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        printf("DBMySQL: savepoint not realeased successfully.\n");
    }
    
    return 1;
}

int DBMySQL::disableKeys(DBDataSchema::Schema * thisSchema) {
    //construct query to disable the keys
    string query = "ALTER TABLE ";
    query.append(thisSchema->getDbName());
    query.append(".");
    query.append(thisSchema->getTableName());
    query.append(" DISABLE KEYS");
    
    if(mysql_query(dbHandler, query.c_str())) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL: could not disable keys.\n");
    }
    
    return 1;
}

int DBMySQL::enableKeys(DBDataSchema::Schema * thisSchema) {
    //construct query to disable the keys
    string query = "ALTER TABLE ";
    query.append(thisSchema->getDbName());
    query.append(".");
    query.append(thisSchema->getTableName());
    query.append(" ENABLE KEYS");
    
    if(mysql_query(dbHandler, query.c_str())) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL: could not reenable keys.\n");
    }
    
    return 1;
}


DBDataSchema::Schema * DBMySQL::getSchema(string database, string table) {
    char queryString[AING_MYSQL_LENQUERRYBUFFER];
    
    DBDataSchema::Schema * retSchema = new DBDataSchema::Schema;
    
    //construct query
    sprintf(queryString, "DESCRIBE %s.%s;", database.c_str(), table.c_str());
    
    //check if table exists
    if(mysql_query(dbHandler, queryString)) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - getSchema: table not found");
    }
    
    MYSQL_RES *result = mysql_store_result(dbHandler);
    
    retSchema->setDbName(database);
    retSchema->setTableName(table);
    
    MYSQL_ROW row;
    //loop through the results and create Schema item
    while((row = mysql_fetch_row(result))) {
        DBDataSchema::SchemaItem * newItem = new DBDataSchema::SchemaItem;
        
        string colName(row[0]);
        
        newItem->setColumnName(row[0]);
        
        //parse for type
        newItem->setColumnDBType(getType(row[1]));
        
        //newItem->setDataDesc(NULL);
        
        retSchema->addItemToSchema(newItem);            
    }
    
    mysql_free_result(result);
    
    return retSchema;
}

void* DBMySQL::prepareIngestStatement(DBDataSchema::Schema * thisSchema) {
    MYSQL_prepStmt * stmtContainer;
    stmtContainer = (MYSQL_prepStmt*)malloc(sizeof(MYSQL_prepStmt));
    if (stmtContainer == NULL) {
        DBIngestor_error("DBMySQL - prepareIngestStatement: could not allocate statement container.");
    }
    
    stmtContainer->prepared = false;
    stmtContainer->isNullTrue = true;
    stmtContainer->isNullFalse = false;
    
    MYSQL_STMT *statement;
    MYSQL_BIND *bind;
    bind = (MYSQL_BIND*)malloc(thisSchema->getNumActiveItems() * sizeof(MYSQL_BIND));
    if(bind == NULL) {
        DBIngestor_error("DBMySQL - prepareIngestStatement: could not allocate bind container.");
    }
    //zero the bind structure
    memset(bind, 0, thisSchema->getNumActiveItems() * sizeof(MYSQL_BIND));
    
    statement = mysql_stmt_init(dbHandler);
    if(statement == NULL) {
        printf("DBMySQL: Error\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - prepareIngestStatement: could not allocate statement handler.");
    }
    
    //construct query string
    string query = "INSERT INTO ";
    query.append(thisSchema->getDbName());
    query.append(".");
    query.append(thisSchema->getTableName());
    query.append("(`");
    
    //insert column names
    int i=0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append("`, `");
        } else {
            query.append("`) VALUES (");
        }
        
        //fill the bind data with information about this
        bind[i].buffer_type = translateTypeToMYSQL(thisSchema->getArrSchemaItems().at(j)->getColumnDBType());
        bind[i].is_unsigned = isUnsignedType(thisSchema->getArrSchemaItems().at(j)->getColumnDBType());
        bind[i].is_null = 0;
        bind[i].length = 0;
        
        i++;
    }
    
    i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        query.append("?");
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append(", ");
        } else {
            query.append(")");
        }
        
        i++;
    }
    
    //printf("%s\n", query.c_str());
    
    if (mysql_stmt_prepare(statement, query.c_str(), strlen(query.c_str())) != 0) {
        printf("DBMySQL: Error\n");
        printf("Statement: %s\n", query.c_str());
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - prepareIngestStatement: an error occured in prepareIngestStatement\n");
    }
    
    stmtContainer->stmt = statement;
    stmtContainer->bind = bind;
    stmtContainer->lenBind = (int)thisSchema->getNumActiveItems();
    
    return (void*)stmtContainer;    
}

void* DBMySQL::prepareMultiIngestStatement(DBDataSchema::Schema * thisSchema, int numElements) {
    if(numElements > maxRowsPerStmt(thisSchema)) {
        printf("DBMySQL: Error\n");
        printf("max_prepared_stmt_count: %i\n", maxRowsPerStmt(thisSchema));
        DBIngestor_error("DBMySQL - prepareMultiIngestStatement: max_prepared_stmt_count has been violated.\n");
    }

    MYSQL_prepStmt * stmtContainer;
    stmtContainer = (MYSQL_prepStmt*)malloc(sizeof(MYSQL_prepStmt));
    if (stmtContainer == NULL) {
        DBIngestor_error("DBMySQL - prepareMultiIngestStatement: could not allocate statement container.");
    }
    
    stmtContainer->prepared = false;
    stmtContainer->isNullTrue = true;
    stmtContainer->isNullFalse = false;
    
    MYSQL_STMT *statement;
    MYSQL_BIND *bind;
    bind = (MYSQL_BIND*)malloc(numElements * thisSchema->getNumActiveItems() * sizeof(MYSQL_BIND));
    if(bind == NULL) {
        DBIngestor_error("DBMySQL - prepareMultiIngestStatement: could not allocate bind container.");
    }
    //zero the bind structure
    memset(bind, 0, numElements * thisSchema->getNumActiveItems() * sizeof(MYSQL_BIND));

    statement = mysql_stmt_init(dbHandler);
    if(statement == NULL) {
        printf("DBMySQL: Error\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - prepareMultiIngestStatement: could not allocate statement handler.");
    }

    //construct query string
    string query = "INSERT INTO ";
    query.append(thisSchema->getDbName());
    query.append(".");
    query.append(thisSchema->getTableName());
    query.append("(`");
    
    //insert column names
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append("`, `");
        } else {
            query.append("`) VALUES ");
        }
        
        i++;
    }
    
    for(int j=0; j<numElements; j++) {
        query.append("(");
        i = 0;
        for(int k=0; k<thisSchema->getArrSchemaItems().size(); k++) {
            if(thisSchema->getArrSchemaItems().at(k)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
                continue;
            }

            query.append("?");
            if (i != thisSchema->getNumActiveItems() - 1) {
                query.append(", ");
            } else if (j == numElements - 1) {
                query.append(")");
            } else {
                query.append("), ");
            }
            
            //fill the bind data with information about this
            unsigned long size = thisSchema->getNumActiveItems();
            bind[j*size+i].buffer_type = translateTypeToMYSQL(thisSchema->getArrSchemaItems().at(k)->getColumnDBType());
            bind[j*size+i].is_unsigned = isUnsignedType(thisSchema->getArrSchemaItems().at(k)->getColumnDBType());
            bind[j*size+i].is_null = 0;
            bind[j*size+i].length = 0;
            
            i++;
        }
    }
    
    //printf("%s\n", query.c_str());
    
    if (mysql_stmt_prepare(statement, query.c_str(), strlen(query.c_str())) != 0) {
        printf("DBMySQL: Error\n");
        printf("Statement: %s\n", query.c_str());
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - prepareMultiIngestStatement: an error occured in prepareIngestStatement\n");
    }

    stmtContainer->stmt = statement;
    stmtContainer->bind = bind;
    stmtContainer->lenBind = numElements * (int)thisSchema->getNumActiveItems();
    
    return (void*)stmtContainer;    
}

int DBMySQL::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData) {
    string query;
    
    query = buildOneRowInsertString(thisSchema, thisData, DBTYPE_MYSQL);

    if(query.size() == 0) {
        DBIngestor_error("DBMySQL - insertOneRow: an error occured in insertOneRow in switch while binding\n");
    }
    
    if( mysql_query(dbHandler, query.c_str()) != 0) {
        printf("DBMySQL: Error\n");
        printf("Statement: %s\n", query.c_str());
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - insertOneRow without prepared statement: an error occured in the ingest.\n");
    }
    
    return 1;    
}

int DBMySQL::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData, void* preparedStatement) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    MYSQL_prepStmt *statement;
    
    statement = (MYSQL_prepStmt*)preparedStatement;
    
    bindOneRowToStmt(thisSchema, thisData, statement, 0);    
    
    executeStmt((void*)statement);
    
    return 1;
}

int DBMySQL::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, void* preparedStatement, int nInStmt) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    MYSQL_prepStmt *statement = (MYSQL_prepStmt*) preparedStatement;
    long byteCount = 0;
    int stride = nInStmt * (int)thisSchema->getNumActiveItems();
    char * currRow = (char*)thisData;
    
    //bind data to the prepared statement
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        unsigned long strLen;
        char * theString;
        int8_t tmpVal3;
        int16_t tmpVal;
        uint8_t tmpVal4;
        uint16_t tmpVal5;
        
        switch (thisSchema->getArrSchemaItems().at(j)->getColumnDBType()) {
            case DBDataSchema::DBT_CHAR:
                theString = *(char**)(currRow+byteCount);
                strLen = strlen(theString);
                statement->bind[stride+i].buffer = *(char**)(currRow+byteCount);
                
                if(statement->bind[stride+i].length == NULL) {
                    statement->bind[stride+i].length = (unsigned long*)malloc(sizeof(unsigned long));
                }
                
                *(statement->bind[stride+i].length) = strLen;
                byteCount += sizeof(char*);
                break;
            case DBDataSchema::DBT_BIT:
                //for safety in the cast below
                memcpy(&tmpVal3, (int8_t*)(currRow+byteCount), sizeof(int8_t));
                
                if(statement->bind[stride+i].buffer == NULL) {
                    statement->bind[stride+i].buffer = (signed char*)malloc(sizeof(signed char));
                }
                
                *(signed char*)(statement->bind[stride+i].buffer) = (signed char)tmpVal3;
                byteCount += sizeof(int8_t);
                break;
            case DBDataSchema::DBT_BIGINT:
                statement->bind[stride+i].buffer = (int64_t*)(currRow+byteCount);
                byteCount += sizeof(int64_t);
                break;
            case DBDataSchema::DBT_MEDIUMINT:
                statement->bind[stride+i].buffer = (int32_t*)(currRow+byteCount);
                byteCount += sizeof(int32_t);
                break;
            case DBDataSchema::DBT_INTEGER:
                statement->bind[stride+i].buffer = (int32_t*)(currRow+byteCount);
                byteCount += sizeof(int32_t);
                break;
            case DBDataSchema::DBT_SMALLINT:
                //for safety in the cast below
                memcpy(&tmpVal, (int16_t*)(currRow+byteCount), sizeof(int16_t));
                
                if(statement->bind[stride+i].buffer == NULL) {
                    statement->bind[stride+i].buffer = (short int*)malloc(sizeof(short int));
                }
                
                *(short int*)(statement->bind[stride+i].buffer) = (short int)tmpVal;
                byteCount += sizeof(int16_t);
                break;
            case DBDataSchema::DBT_TINYINT:
                //for safety in the cast below
                memcpy(&tmpVal3, (int8_t*)(currRow+byteCount), sizeof(int8_t));
                
                if(statement->bind[stride+i].buffer == NULL) {
                    statement->bind[stride+i].buffer = (signed char*)malloc(sizeof(signed char));
                }
                
                *(signed char*)(statement->bind[stride+i].buffer) = (signed char)tmpVal3;
                byteCount += sizeof(int8_t);
                break;
            case DBDataSchema::DBT_UBIGINT:
                statement->bind[stride+i].buffer = (uint64_t*)(currRow+byteCount);
                statement->bind[stride+i].is_unsigned = 1;
                byteCount += sizeof(uint64_t);
                break;
            case DBDataSchema::DBT_UMEDIUMINT:
                statement->bind[stride+i].buffer = (uint32_t*)(currRow+byteCount);
                statement->bind[stride+i].is_unsigned = 1;
                byteCount += sizeof(uint32_t);
                break;
            case DBDataSchema::DBT_UINTEGER:
                statement->bind[stride+i].buffer = (uint32_t*)(currRow+byteCount);
                statement->bind[stride+i].is_unsigned = 1;
                byteCount += sizeof(uint32_t);
                break;
            case DBDataSchema::DBT_USMALLINT:
                //for safety in the cast below
                memcpy(&tmpVal4, (uint16_t*)(currRow+byteCount), sizeof(uint16_t));
                
                if(statement->bind[stride+i].buffer == NULL) {
                    statement->bind[stride+i].buffer = (short unsigned int*)malloc(sizeof(short unsigned int));
                }
                
                *(short unsigned int*)(statement->bind[stride+i].buffer) = (short unsigned int)tmpVal;
                statement->bind[stride+i].is_unsigned = 1;
                byteCount += sizeof(uint16_t);
                break;
            case DBDataSchema::DBT_UTINYINT:
                //for safety in the cast below
                memcpy(&tmpVal5, (uint8_t*)(currRow+byteCount), sizeof(uint8_t));
                
                if(statement->bind[stride+i].buffer == NULL) {
                    statement->bind[stride+i].buffer = (unsigned char*)malloc(sizeof(unsigned char));
                }
                
                *(unsigned char*)(statement->bind[stride+i].buffer) = (unsigned char)tmpVal3;
                statement->bind[stride+i].is_unsigned = 1;
                byteCount += sizeof(uint8_t);
                break;
            case DBDataSchema::DBT_FLOAT:
                statement->bind[stride+i].buffer = (float*)(currRow+byteCount);
                byteCount += sizeof(float);
                break;
            case DBDataSchema::DBT_REAL:
                statement->bind[stride+i].buffer = (double*)(currRow+byteCount);
                byteCount += sizeof(double);
                break;
            default:
                DBIngestor_error("castDTypeToDBType: DBType not known, I don't know what to do.");
        }
        
        i++;
    }
    
    return 1;
}

int DBMySQL::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, bool* isNullArray, void* preparedStatement, int nInStmt) {
    
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(isNullArray != NULL);
    assert(preparedStatement != NULL);
    
    //pass command through
    bindOneRowToStmt(thisSchema, thisData, preparedStatement, nInStmt);
    
    //now bind the NULLs
    MYSQL_prepStmt *statement = (MYSQL_prepStmt*) preparedStatement;
    int stride = nInStmt * (int)thisSchema->getNumActiveItems();
    
    //bind NULLs to the prepared statement
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        if(isNullArray[i] == 1) {
            statement->bind[stride+i].is_null = &(statement->isNullTrue);
        } else {
            statement->bind[stride+i].is_null = &(statement->isNullFalse);
        }
        
        i++;
    }
    
    return 1;
}

int DBMySQL::executeStmt(void* preparedStatement) {
    assert(preparedStatement != NULL);
    MYSQL_prepStmt *statement = (MYSQL_prepStmt*) preparedStatement;
    
    if( mysql_stmt_bind_param(statement->stmt, statement->bind) != 0) {
        printf("DBMySQL: Error\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - executeStmt: could not bind statement.\n");
    }
    
    
    if( mysql_stmt_execute(statement->stmt) != 0) {
        printf("DBMySQL: Error\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - executeStmt: could not execute statement.\n");
    }
    
    return 1;
}

int DBMySQL::finalizePreparedStatement(void* preparedStatement) {
    assert(preparedStatement != NULL);
    MYSQL_prepStmt *statement = (MYSQL_prepStmt*) preparedStatement;
    
    //first free any allocated memory, then free bind buffer, then close statement
    for(int i=0; i<statement->lenBind; i++) {
        if(statement->bind[i].length != NULL)
            free(statement->bind[i].length);
        
        if(statement->bind[i].buffer_type == MYSQL_TYPE_TINY &&
           statement->bind[i].buffer != NULL) 
            free(statement->bind[i].buffer);

        if(statement->bind[i].buffer_type == MYSQL_TYPE_SHORT &&
           statement->bind[i].buffer != NULL) 
            free(statement->bind[i].buffer);
    }

    if(mysql_stmt_close(statement->stmt) != 0) {
        printf("DBMySQL: Error\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - finalizePreparedStatement: error in stmt close.\n");
    }
    
    free(statement);
    
    return 1;
}

int DBMySQL::maxRowsPerStmt(DBDataSchema::Schema * thisSchema) {
    char queryString[AING_MYSQL_LENQUERRYBUFFER] = "SHOW VARIABLES LIKE 'max_prepared_stmt_count'";
    
    if(mysql_query(dbHandler, queryString)) {
        printf("Error MySQL:\n");
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - maxRowsPerStmt: cannot determine the maximum prepared statement length");
    }
    
    MYSQL_RES *result = mysql_store_result(dbHandler);
    
    MYSQL_ROW row;

    row = mysql_fetch_row(result);
    
    uint64_t maxRows = atoi(row[1]) / thisSchema->getNumActiveItems();
    
    mysql_free_result(result);
    
    return (int)maxRows;
}

void * DBMySQL::initGetCompleteTable(DBDataSchema::Schema * thisSchema) {
    assert(thisSchema != NULL);
    
    MYSQL_prepStmt * stmtContainer;
    stmtContainer = (MYSQL_prepStmt*)malloc(sizeof(MYSQL_prepStmt));
    if (stmtContainer == NULL) {
        DBIngestor_error("DBMySQL - prepareIngestStatement: could not allocate statement container.");
    }
    
    stmtContainer->prepared = false;
    stmtContainer->isNullTrue = true;
    stmtContainer->isNullFalse = false;

    MYSQL_STMT *statement;
    statement = mysql_stmt_init(dbHandler);

    MYSQL_BIND *bind;
    bind = (MYSQL_BIND*)malloc(thisSchema->getNumActiveItems() * sizeof(MYSQL_BIND));
    if(bind == NULL) {
        DBIngestor_error("DBMySQL - prepareMultiIngestStatement: could not allocate bind container.");
    }
    //zero the bind structure
    memset(bind, 0, thisSchema->getNumActiveItems() * sizeof(MYSQL_BIND));

    string query = "SELECT `";
    
    //insert column names
    int i=0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append("`, `");
        } else {
            query.append("` FROM ");
        }
        
        //fill the bind data with information about this
        bind[i].buffer_type = translateTypeToMYSQL(thisSchema->getArrSchemaItems().at(j)-> getColumnDBType());
        bind[i].is_unsigned = isUnsignedType(thisSchema->getArrSchemaItems().at(j)->getColumnDBType());
        bind[i].is_null = 0;
        bind[i].length = 0;
        
        i++;
    }
    
    query.append(thisSchema->getDbName());
    query.append(".");
    query.append(thisSchema->getTableName());
    
    if (mysql_stmt_prepare(statement, query.c_str(), strlen(query.c_str())) != 0) {
        printf("DBMySQL: Error\n");
        printf("Statement: %s\n", query.c_str());
        printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
        DBIngestor_error("DBMySQL - initGetCompleteTable: an error occured in prepareIngestStatement\n");
    }
    
    stmtContainer->stmt = statement;
    stmtContainer->bind = bind;
    stmtContainer->lenBind = (int)thisSchema->getNumActiveItems();
    
    return (void*)stmtContainer;
}

int DBMySQL::getNextRow(DBDataSchema::Schema * thisSchema, void* thisData, void * preparedStatement) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    MYSQL_prepStmt *statement = (MYSQL_prepStmt*) preparedStatement;
    
    if(statement->prepared == false) {
        if( mysql_stmt_execute(statement->stmt) != 0) {
            printf("DBMySQL: Error\n");
            printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
            DBIngestor_error("DBMySQL - getNextRow: could not execute statement.\n");
        }
        
        bindOneRowToStmt(thisSchema, thisData, preparedStatement, 0);
        
        if( mysql_stmt_bind_result(statement->stmt, statement->bind) != 0) {
            printf("DBMySQL: Error\n");
            printf("ErrNr %u: %s\n", mysql_errno(dbHandler), mysql_error(dbHandler));
            DBIngestor_error("DBMySQL - getNextRow: could not bind statement.\n");
        }
        
        statement->prepared = true;
    }

    if( mysql_stmt_fetch(statement->stmt) == 0) {
        return 1;
    } else {
        return 0;
    }
}

DBDataSchema::DBType DBMySQL::getType(char * thisTypeString) {
    //WARNING! THE SEQUENCE WITH WHICH INTEGERS ARE TESTED IS IMPORTANT!!
    
    string tmpString(thisTypeString);    
    
    boost::to_upper(tmpString);
    
    boost::trim_left(tmpString);
    boost::xpressive::sregex rex;
    boost::xpressive::smatch what;
    
    rex = boost::xpressive::sregex::compile("(.*)CHAR(.*)");
    if( boost::xpressive::regex_match(tmpString, what, rex) ) {
        return DBDataSchema::DBT_CHAR;        
    }
    
    rex = boost::xpressive::sregex::compile("(.*)BOOL(.*)");
    if( boost::xpressive::regex_match(tmpString, what, rex) ) {
        return DBDataSchema::DBT_BIT;        
    }
    
    rex = boost::xpressive::sregex::compile("(.*)UNSIGNED(.*)");
    if( boost::xpressive::regex_match(tmpString, what, rex) ) {

        rex = boost::xpressive::sregex::compile("(.*)TINYINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_UTINYINT;        
        }
        
        rex = boost::xpressive::sregex::compile("(.*)SMALLINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_USMALLINT;        
        }
        
        rex = boost::xpressive::sregex::compile("(.*)MEDIUMINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_UMEDIUMINT;        
        }
        
        rex = boost::xpressive::sregex::compile("(.*)BIGINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_UBIGINT;        
        }
        
        //now that all the ints are checked, the only remaining is INTEGER / INT
        rex = boost::xpressive::sregex::compile("(.*)INT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_UINTEGER;        
        }
    
    } else {
        
        rex = boost::xpressive::sregex::compile("(.*)TINYINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_TINYINT;        
        }

        rex = boost::xpressive::sregex::compile("(.*)SMALLINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_SMALLINT;        
        }
        
        rex = boost::xpressive::sregex::compile("(.*)MEDIUMINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_MEDIUMINT;        
        }
        
        rex = boost::xpressive::sregex::compile("(.*)BIGINT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_BIGINT;        
        }

        //now that all the ints are checked, the only remaining is INTEGER / INT
        rex = boost::xpressive::sregex::compile("(.*)INT(.*)");
        if( boost::xpressive::regex_match(tmpString, what, rex) ) {
            return DBDataSchema::DBT_INTEGER;        
        }
        
    }

    rex = boost::xpressive::sregex::compile("(.*)FLOAT(.*)");
    if( boost::xpressive::regex_match(tmpString, what, rex) ) {
        return DBDataSchema::DBT_FLOAT;        
    }

    rex = boost::xpressive::sregex::compile("(.*)DOUBLE(.*)");
    if( boost::xpressive::regex_match(tmpString, what, rex) ) {
        return DBDataSchema::DBT_REAL;        
    }

    rex = boost::xpressive::sregex::compile("(.*)REAL(.*)");
    if( boost::xpressive::regex_match(tmpString, what, rex) ) {
        return DBDataSchema::DBT_REAL;        
    }

    rex = boost::xpressive::sregex::compile("(.*)DOUBLE(.*)");
    if( boost::xpressive::regex_match(tmpString, what, rex) ) {
        return DBDataSchema::DBT_REAL;        
    }
    
    printf("Error MySQL:\n");
    printf("Err in type: %s\n", thisTypeString);
    DBIngestor_error("DBMySQL: this type used in the table is not yet supported. Please implement support...\n");

    return (DBDataSchema::DBType)0;
}

enum_field_types DBMySQL::translateTypeToMYSQL(DBDataSchema::DBType thisType) {
    switch (thisType) {
        case DBDataSchema::DBT_CHAR:
            return MYSQL_TYPE_STRING;
        case DBDataSchema::DBT_BIT:
            return MYSQL_TYPE_BIT;
        case DBDataSchema::DBT_BIGINT:
            return MYSQL_TYPE_LONGLONG;
        case DBDataSchema::DBT_MEDIUMINT:
            return MYSQL_TYPE_INT24;
        case DBDataSchema::DBT_INTEGER:
            return MYSQL_TYPE_LONG;
        case DBDataSchema::DBT_SMALLINT:
            return MYSQL_TYPE_SHORT;
        case DBDataSchema::DBT_TINYINT:
            return MYSQL_TYPE_TINY;
        case DBDataSchema::DBT_UBIGINT:
            return MYSQL_TYPE_LONGLONG;
        case DBDataSchema::DBT_UMEDIUMINT:
            return MYSQL_TYPE_INT24;
        case DBDataSchema::DBT_UINTEGER:
            return MYSQL_TYPE_LONG;
        case DBDataSchema::DBT_USMALLINT:
            return MYSQL_TYPE_SHORT;
        case DBDataSchema::DBT_UTINYINT:
            return MYSQL_TYPE_TINY;
        case DBDataSchema::DBT_FLOAT:
            return MYSQL_TYPE_FLOAT;
        case DBDataSchema::DBT_REAL:
            return MYSQL_TYPE_DOUBLE;
        case DBDataSchema::DBT_DATE:
            return MYSQL_TYPE_DATE;
        case DBDataSchema::DBT_TIME:
            return MYSQL_TYPE_TIME;
        default:
            DBIngestor_error("DBMySQL: you have specified an unsupported MYSQL data type.\n");
            break;
    }
    
    return (enum_field_types)0;
}

int DBMySQL::isUnsignedType(DBDataSchema::DBType thisType) {
    switch (thisType) {
        case DBDataSchema::DBT_UBIGINT:
            return 1;
        case DBDataSchema::DBT_UMEDIUMINT:
            return 1;
        case DBDataSchema::DBT_UINTEGER:
            return 1;
        case DBDataSchema::DBT_USMALLINT:
            return 1;
        case DBDataSchema::DBT_UTINYINT:
            return 1;
        default:
            return 0;
    }
    
    return 0;
}

