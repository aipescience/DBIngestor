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

#include "DBODBC.h"
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

#define AING_ODBC_LENQUERRYBUFFER 1024

//Private stuff:
typedef struct {
    SQLHSTMT * statement;
    void ** buffer;
    bool * isNullArray;
    DBDataSchema::DBType * type;
    SQLLEN * parLenArray;
    SQLULEN * colSize;
    SQLINTEGER * decDigits;
    int size;
    bool prepared;
    SQLLEN null;
} ODBC_prepStmt;

DBODBC::DBODBC() {
    odbcEnv = SQL_NULL_HENV;
    odbcDbc = SQL_NULL_HDBC;
    myNumElements = -1;
    recCount = 0;
}

DBODBC::~DBODBC() {
    disconnect();
}

int DBODBC::connect(string usr, string pwd, string host, string port, string socket) {
    //ONLY ACCEPT DSNs. Anything else will fail miserably
    SQLCHAR output[1024];
    SQLSMALLINT outputLen;
    
    myusr = usr;
    mypwd = pwd;
    myhost = host;
    myport = port;
    mysocket = socket;

    //Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &odbcEnv);
    SQLSetEnvAttr(odbcEnv, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
    
    //Allocate connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, odbcEnv, &odbcDbc);
    
    //build connection string
    //DSN=Input;UID=userName;PWD=password
    string conString = socket;
    conString.append("SERVER=");
    conString.append(host);
    conString.append(";Port=");
    conString.append(port);
    conString.append(";UID=");
    conString.append(usr);
    conString.append(";PWD=");
    conString.append(pwd);
    conString.append(";");
    
    //connect
    SQLRETURN res = SQLDriverConnect(odbcDbc, NULL, (SQLCHAR*)conString.c_str(), SQL_NTS, 
                                     output, sizeof(output), &outputLen, SQL_DRIVER_COMPLETE);
    
    if(!SQL_SUCCEEDED(res)) {
        printf("Error ODBC:\n");
        printf("%s\n", output);
        printODBCError("SQLDriverConnect", odbcDbc, SQL_HANDLE_DBC);
        printf("DBODBC: could not connect to ODBC database\n");

        return 0;
    }
    
    //read the database system
    SQLCHAR dbmsName[256];
    SQLGetInfo(odbcDbc, SQL_DBMS_NAME, (SQLPOINTER)dbmsName, sizeof(dbmsName), &outputLen);
    dbServerName.assign((char*)dbmsName, outputLen);
    
    isConnected = true;
    
    return 1;
}

int DBODBC::disconnect() {
    SQLDisconnect(odbcDbc);
    
    if(odbcDbc != SQL_NULL_HDBC) {
        SQLFreeHandle(SQL_HANDLE_DBC, odbcDbc);
    }
    
    if(odbcEnv != SQL_NULL_HENV) {
        SQLFreeHandle(SQL_HANDLE_ENV, odbcEnv);
    }
    
    isConnected = false;
    
    return 1;
}

int DBODBC::setSavepoint() {
    SQLHSTMT stmt;
    SQLRETURN result;
    
    if(resumeMode == false) {
        SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt);
        
        result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_AUTOCOMMIT, (SQLUINTEGER*)SQL_AUTOCOMMIT_OFF, sizeof(SQLUINTEGER));
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLSetConnectAttr", odbcDbc, SQL_HANDLE_DBC);
            DBIngestor_error("DBODBC: could not set savepoint.\n");
        }
        
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }

    return 1;
}

int DBODBC::rollback() {
    SQLRETURN result;
    
    if(resumeMode == false) {
        result = SQLEndTran(SQL_HANDLE_DBC, odbcDbc, SQL_ROLLBACK);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLEndTran", odbcDbc, SQL_HANDLE_DBC);
            DBIngestor_error("DBODBC: rollback not successfull.\n");
        }
        
        result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_AUTOCOMMIT, (SQLUINTEGER*)SQL_AUTOCOMMIT_ON, sizeof(SQLUINTEGER));

        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLSetConnectAttr", odbcDbc, SQL_HANDLE_DBC);
            DBIngestor_error("DBODBC: could not reset to autocommit mode.\n");
        }
    }

    return 1;
}

int DBODBC::releaseSavepoint() {
    SQLRETURN result;

    if(resumeMode == false) {
        result = SQLEndTran(SQL_HANDLE_DBC, odbcDbc, SQL_COMMIT);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLEndTran", odbcDbc, SQL_HANDLE_DBC);
            DBIngestor_error("DBODBC: savepoint not realeased successfully.\n");
        }
        
        result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_AUTOCOMMIT, (SQLUINTEGER*)SQL_AUTOCOMMIT_ON, sizeof(SQLUINTEGER));
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLSetConnectAttr", odbcDbc, SQL_HANDLE_DBC);
            DBIngestor_error("DBODBC: could not reset to autocommit mode.\n");
        }
    }

    return 1;
}

int DBODBC::disableKeys(DBDataSchema::Schema * thisSchema) {
    SQLHSTMT stmt;
    SQLHSTMT stmt2;
    SQLRETURN result;
    SQLRETURN result2;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt);
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt2);
    
    if(dbServerName.find("Microsoft") != dbServerName.npos) {
        //query the indexes on this table and construct approperiate sql statement server side
        string query = "select 'alter index '+i.name+' on '+o.name+' disable '+char(13)+char(10)+';' from sys.indexes i inner join sys.objects o on o.object_id=i.object_id where o.is_ms_shipped=0 and i.index_id>=1 and o.name='";
        query.append(thisSchema->getTableName());
        query.append("'");       
        
        result = SQLExecDirect(stmt, (SQLCHAR*)query.c_str(), (SQLINTEGER)strlen(query.c_str()));
        while(SQL_SUCCEEDED(result = SQLFetch(stmt))) {
            SQLLEN ind;
            char buffer[AING_ODBC_LENQUERRYBUFFER];
            result = SQLGetData(stmt, 1, SQL_C_CHAR, buffer, sizeof(buffer), &ind);
            
            if(SQL_SUCCEEDED(result)) {
                result2 = SQLExecDirect(stmt2, (SQLCHAR*)buffer, (SQLINTEGER)strlen(buffer));
                if(!SQL_SUCCEEDED(result2)) {
                    printf("Error ODBC:\n");
                    printODBCError("SQLExecDirect", stmt, SQL_HANDLE_STMT);
                    DBIngestor_error("DBODBC: could not disable keys.\n");
                }
            }
        }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    
    return 1;
}

int DBODBC::enableKeys(DBDataSchema::Schema * thisSchema) {
    SQLHSTMT stmt;
    SQLHSTMT stmt2;
    SQLRETURN result;
    SQLRETURN result2;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt);
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt2);
    
    if(dbServerName.find("Microsoft") != dbServerName.npos) {
        //query the indexes on this table and construct approperiate sql statement server side
        string query = "select 'alter index '+i.name+' on '+o.name+' enable '+char(13)+char(10)+';' from sys.indexes i inner join sys.objects o on o.object_id=i.object_id where o.is_ms_shipped=0 and i.index_id>=1 and o.name='";
        query.append(thisSchema->getTableName());
        query.append("'");       
        
        result = SQLExecDirect(stmt, (SQLCHAR*)query.c_str(), (SQLINTEGER)strlen(query.c_str()));
        while(SQL_SUCCEEDED(result = SQLFetch(stmt))) {
            SQLLEN ind;
            char buffer[AING_ODBC_LENQUERRYBUFFER];
            result = SQLGetData(stmt, 1, SQL_C_CHAR, buffer, sizeof(buffer), &ind);
            
            if(SQL_SUCCEEDED(result)) {
                result2 = SQLExecDirect(stmt2, (SQLCHAR*)buffer, (SQLINTEGER)strlen(buffer));
                if(!SQL_SUCCEEDED(result2)) {
                    printf("Error ODBC:\n");
                    printODBCError("SQLExecDirect", stmt, SQL_HANDLE_STMT);
                    DBIngestor_error("DBODBC: could not reenable keys.\n");
                }
            }
        }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    
    return 1;
}


DBDataSchema::Schema * DBODBC::getSchema(string database, string table) {
    SQLHSTMT stmt;
    SQLHSTMT stmt2;
    SQLRETURN result;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt);
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt2);

    DBDataSchema::Schema * retSchema = new DBDataSchema::Schema;
    
    result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_CURRENT_CATALOG, (SQLCHAR*)database.c_str(), (SQLINTEGER)strlen(database.c_str()));
    
    //check whether the table exists
    SQLCHAR tmpStr[6] = "TABLE";
    SQLTables(stmt, NULL, 0, 
                    NULL, 0, 
                    (SQLCHAR*)table.c_str(), (SQLINTEGER)strlen(table.c_str()), 
                    tmpStr, SQL_NTS);
    
    int count=0;
    while(SQL_SUCCEEDED(result = SQLFetch(stmt))) {
        count++;
    }
    
    if(count == 0) {
        printf("Error ODBC:\n");
        printODBCError("SQLTables", stmt, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBC - getSchema: table not found");
    }
    
    retSchema->setDbName(database);
    retSchema->setTableName(table);
    
    SQLColumns(stmt2, NULL, 0,
                     NULL, 0, 
                     (SQLCHAR*)table.c_str(), (SQLINTEGER)strlen(table.c_str()), 
                     NULL, SQL_NTS);
    
    count=0;
    SQLSMALLINT numCols;
    SQLNumResultCols(stmt2, &numCols);
    while(SQL_SUCCEEDED(result = SQLFetch(stmt2))) {
        char buffer[AING_ODBC_LENQUERRYBUFFER];
        SQLLEN ind;
        SQLSMALLINT typeResult;
        SQLINTEGER columnSize;
        SQLSMALLINT decimalDigits;
        SQLSMALLINT notNull;

        DBDataSchema::SchemaItem * newItem = new DBDataSchema::SchemaItem;
        
        result = SQLGetData(stmt2, 4, SQL_C_CHAR, buffer, sizeof(buffer), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBC - getSchema: could not read column name");
        }            

        result = SQLGetData(stmt2, 5, SQL_SMALLINT, &typeResult, sizeof(typeResult), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBC - getSchema: could not read data type");
        }            

        result = SQLGetData(stmt2, 7, SQL_INTEGER, &columnSize, sizeof(columnSize), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBC - getSchema: could not read column size");
        }            

        result = SQLGetData(stmt2, 9, SQL_SMALLINT, &decimalDigits, sizeof(decimalDigits), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBC - getSchema: could not read data type");
        }            

        result = SQLGetData(stmt2, 11, SQL_SMALLINT, &notNull, sizeof(notNull), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBC - getSchema: could not read data type");
        }

        string colName(buffer);
        
        newItem->setColumnName(colName);
        
        //parse for type
        newItem->setColumnDBType(getType(typeResult));
        
        newItem->setColumnSize(columnSize);
        newItem->setDecimalDigits(decimalDigits);
        
        if(notNull == SQL_NO_NULLS) {
            newItem->setIsNotNull(1);
        } else {
            newItem->setIsNotNull(0);
        }
        
        retSchema->addItemToSchema(newItem);            
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    
    return retSchema;
}

void* DBODBC::prepareIngestStatement(DBDataSchema::Schema * thisSchema) {
    SQLHSTMT * stmt = (SQLHSTMT*)malloc(sizeof(SQLHSTMT));

    myNumElements = -1;
    mySchema = thisSchema;

    if(stmt == NULL) {
        printf("DBODBC: Error\n");
        DBIngestor_error("DBODBC - prepareMultiIngestStatement: error allocating ODBC statement.\n");
    }

    SQLRETURN result;
    ODBC_prepStmt * stmtContainer;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, stmt);
    
    stmtContainer = (ODBC_prepStmt*)allocPrepStmt(thisSchema->getNumActiveItems());
    
    stmtContainer->statement = stmt;
    stmtContainer->size = thisSchema->getNumActiveItems();

    //construct query string
    string query = "INSERT INTO ";
    query.append(thisSchema->getDbName());
    query.append("..");
    query.append(thisSchema->getTableName());
    query.append("(");
    
    //insert column names
    int i=0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append(", ");
        } else {
            query.append(") VALUES (");
        }
     
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
        
        stmtContainer->type[i] = thisSchema->getArrSchemaItems().at(j)->getColumnDBType();
        stmtContainer->colSize[i] = thisSchema->getArrSchemaItems().at(j)->getColumnSize();
        stmtContainer->decDigits[i] = thisSchema->getArrSchemaItems().at(j)->getDecimalDigits();
        
        i++;
    }
    
    result = SQLPrepare(*stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
    
    if(!SQL_SUCCEEDED(result) || stmtContainer->parLenArray == NULL || stmtContainer->buffer == NULL) {
        printf("Error ODBC:\n");
        printf("Statement: %s\n", query.c_str());
        printODBCError("SQLPrepare", *stmt, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBC - prepareIngestStatement: could not prepare statement");
    }            

    myquery = query;
        
    return (void*)stmtContainer;   
}

void* DBODBC::prepareMultiIngestStatement(DBDataSchema::Schema * thisSchema, int numElements) {
    myNumElements = numElements;
    mySchema = thisSchema;

    if(numElements > maxRowsPerStmt(thisSchema)) {
        printf("DBODBC: Error\n");
        printf("max_prepared_stmt_count: %i\n", maxRowsPerStmt(thisSchema));
        DBIngestor_error("DBODBC - prepareMultiIngestStatement: max_prepared_stmt_count has been violated.\n");
    }

    SQLHSTMT * stmt = (SQLHSTMT*)malloc(sizeof(SQLHSTMT));
    
    if(stmt == NULL) {
        printf("DBODBC: Error\n");
        DBIngestor_error("DBODBC - prepareMultiIngestStatement: error allocating ODBC statement.\n");
    }
    
    SQLRETURN result;
    ODBC_prepStmt * stmtContainer;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, stmt);

    stmtContainer = (ODBC_prepStmt*)allocPrepStmt(numElements * thisSchema->getNumActiveItems());
    
    stmtContainer->statement = stmt;
    stmtContainer->size = numElements * thisSchema->getNumActiveItems();
    
    //construct query string
    string query = "INSERT INTO ";
    query.append(thisSchema->getDbName());
    query.append("..");
    query.append(thisSchema->getTableName());
    query.append("(");
    
    //insert column names
    int i=0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append(", ");
        } else {
            query.append(") VALUES ");
        }
        
        i++;
    }
    
    i = 0;
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
            
            unsigned long size = thisSchema->getNumActiveItems();
            stmtContainer->type[j*size+i] = thisSchema->getArrSchemaItems().at(k)->getColumnDBType();
            stmtContainer->colSize[j*size+i] = thisSchema->getArrSchemaItems().at(k)->getColumnSize();
            stmtContainer->decDigits[j*size+i] = thisSchema->getArrSchemaItems().at(k)->getDecimalDigits();
            
            i++;
        }
    }
    
    //printf("%s\n", query.c_str());
    
    result = SQLPrepare(*stmt, (SQLCHAR*)query.c_str(), SQL_NTS);


    if(!SQL_SUCCEEDED(result)) {
        printf("Error ODBC:\n");
        printf("Statement: %s\n", query.c_str());
        printODBCError("SQLPrepare", *stmt, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBC - prepareMultiIngestStatement: could not prepare statement");
    }            
    
    myquery = query;

    return (void*)stmtContainer;    
}

int DBODBC::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData) {
    SQLHSTMT stmt;
    SQLRETURN result;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt);
    
    result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_CURRENT_CATALOG, 
                               (SQLCHAR*)thisSchema->getDbName().c_str(), (SQLINTEGER)strlen(thisSchema->getDbName().c_str()));

    string query;
    
    query = buildOneRowInsertString(thisSchema, thisData, DBTYPE_ODBC_MSSQL);
    
    if(query.size() == 0) {
        DBIngestor_error("DBODBC - insertOneRow: an error occured in insertOneRow in switch while binding\n");
    }
    
    result = SQLExecDirect(stmt, (SQLCHAR*)query.c_str(), (SQLINTEGER)strlen(query.c_str()));
    
    if(!SQL_SUCCEEDED(result)) {
        printf("DBODBC: Error\n");
        printf("Statement: %s\n", query.c_str());
        printODBCError("SQLExecDirect", stmt, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBC - insertOneRow without prepared statement: an error occured in the ingest.\n");
    }
    
    return 1;    
}

int DBODBC::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData, void* preparedStatement) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    SQLHSTMT *statement;
    
    statement = (SQLHSTMT*)preparedStatement;
    
    bindOneRowToStmt(thisSchema, thisData, statement, 0);    
    
    executeStmt((void*)statement);
    
    return 1;
}

int DBODBC::bindStatement(void* preparedStatement) {
    assert(preparedStatement != NULL);
    
    ODBC_prepStmt * prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT *statement = prepStmt->statement;
    
    SQLRETURN result;

    //bind data to the prepared statement
    for(int i=0; i<prepStmt->size; i++) {
        switch (prepStmt->type[i]) {
            case DBDataSchema::DBT_CHAR:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], (SQLLEN)prepStmt->parLenArray[i], prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], (SQLLEN)prepStmt->parLenArray[i], &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_BIT:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(int8_t), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(int8_t), &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_BIGINT:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(int64_t), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(int64_t), &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_MEDIUMINT:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(int32_t), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(int32_t), &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_INTEGER:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(int32_t), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(int32_t), &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_SMALLINT:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_SHORT, SQL_SMALLINT, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(int16_t), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_SHORT, SQL_SMALLINT, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(int16_t), &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_TINYINT:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(int8_t), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(int8_t), &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_FLOAT:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(float), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(float), &(prepStmt->null));
                }
                break;
            case DBDataSchema::DBT_REAL:
                if(prepStmt->isNullArray[i] == false) {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 
                                 prepStmt->colSize[i], 
                                 prepStmt->decDigits[i], 
                                 prepStmt->buffer[i], sizeof(double), prepStmt->parLenArray+i);
                } else {
                    result = SQLBindParameter(*statement, i+1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 
                                              prepStmt->colSize[i], 
                                              prepStmt->decDigits[i], 
                                              prepStmt->buffer[i], sizeof(double), &(prepStmt->null));
                }
                break;
            default:
                DBIngestor_error("DBODBC - bindStatement: an error occured in bindStatement in switch while binding\n");
        }
    
        if(!SQL_SUCCEEDED(result)) {
            printf("DBODBC: Error\n");
            printODBCError("SQLExecute", *statement, SQL_HANDLE_STMT);

            printf("Bind: %llx\n", statement);

            if(recCount == 1)
                DBIngestor_error("DBODBC - executeStmt: could not bind statement.\n");
        }
    }
    
    return 1;
}

int DBODBC::bindColStatement(void* preparedStatement) {
    assert(preparedStatement != NULL);
    
    ODBC_prepStmt * prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT *statement = prepStmt->statement;
    
    SQLRETURN result;
    
    //bind data to the prepared statement
    for(int i=0; i<prepStmt->size; i++) {
        switch (prepStmt->type[i]) {
            case DBDataSchema::DBT_CHAR:
                result = SQLBindCol(*statement, i+1, SQL_C_CHAR, prepStmt->buffer[i], 
                                         (SQLLEN)prepStmt->parLenArray[i], prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_BIT:
                result = SQLBindCol(*statement, i+1, SQL_C_TINYINT,  
                                          prepStmt->buffer[i], sizeof(int8_t), prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_BIGINT:
                result = SQLBindCol(*statement, i+1, SQL_C_SBIGINT, 
                                          prepStmt->buffer[i], sizeof(int64_t), prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_MEDIUMINT:
                result = SQLBindCol(*statement, i+1, SQL_C_LONG, 
                                          prepStmt->buffer[i], sizeof(int32_t), prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_INTEGER:
                result = SQLBindCol(*statement, i+1, SQL_C_LONG, 
                                          prepStmt->buffer[i], sizeof(int32_t), prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_SMALLINT:
                result = SQLBindCol(*statement, i+1, SQL_C_SHORT, 
                                          prepStmt->buffer[i], sizeof(int16_t), prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_TINYINT:
                result = SQLBindCol(*statement, i+1, SQL_C_TINYINT,
                                          prepStmt->buffer[i], sizeof(int8_t), prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_FLOAT:
                result = SQLBindCol(*statement, i+1, SQL_C_FLOAT, 
                                          prepStmt->buffer[i], sizeof(float), prepStmt->parLenArray+i);
                break;
            case DBDataSchema::DBT_REAL:
                result = SQLBindCol(*statement, i+1, SQL_C_DOUBLE, 
                                          prepStmt->buffer[i], sizeof(double), prepStmt->parLenArray+i);
                break;
            default:
                DBIngestor_error("DBODBC - bindStatement: an error occured in bindStatement in switch while binding\n");
        }
        
        if(!SQL_SUCCEEDED(result)) {
            printf("DBODBC: Error\n");
            printODBCError("SQLExecute", *statement, SQL_HANDLE_STMT);

            if(recCount == 1)
                DBIngestor_error("DBODBC - executeStmt: could not bind statement.\n");
        }
    }
    
    return 1;
}

int DBODBC::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, void* preparedStatement, int nInStmt) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    ODBC_prepStmt * prepStmt = (ODBC_prepStmt*) preparedStatement;
    long byteCount = 0;
    int stride = nInStmt * (int)thisSchema->getNumActiveItems();
    char * currRow = (char*)thisData;
    
    //bind data to the prepared statement
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        switch (thisSchema->getArrSchemaItems().at(j)->getColumnDBType()) {
            case DBDataSchema::DBT_CHAR:
                prepStmt->buffer[stride+i] = *(char**)(currRow+byteCount);    
                prepStmt->parLenArray[stride+i] = strlen(*(char**)(currRow+byteCount)); //SQL_NTS;
                byteCount += sizeof(char*);
                break;
            case DBDataSchema::DBT_BIT:
                prepStmt->buffer[stride+i] = (int8_t*)(currRow+byteCount);    
                byteCount += sizeof(int8_t);
                break;
            case DBDataSchema::DBT_BIGINT:
                prepStmt->buffer[stride+i] = (int64_t*)(currRow+byteCount);    
                byteCount += sizeof(int64_t);
                break;
            case DBDataSchema::DBT_MEDIUMINT:
                prepStmt->buffer[stride+i] = (int32_t*)(currRow+byteCount);    
                byteCount += sizeof(int32_t);
                break;
            case DBDataSchema::DBT_INTEGER:
                prepStmt->buffer[stride+i] = (int32_t*)(currRow+byteCount);    
                byteCount += sizeof(int32_t);
                break;
            case DBDataSchema::DBT_SMALLINT:
                prepStmt->buffer[stride+i] = (int16_t*)(currRow+byteCount);    
                byteCount += sizeof(int16_t);
                break;
            case DBDataSchema::DBT_TINYINT:
                prepStmt->buffer[stride+i] = (int8_t*)(currRow+byteCount);    
                byteCount += sizeof(int8_t);
                break;
            case DBDataSchema::DBT_FLOAT:
                prepStmt->buffer[stride+i] = (float*)(currRow+byteCount);    
                byteCount += sizeof(float);
                break;
            case DBDataSchema::DBT_REAL:
                prepStmt->buffer[stride+i] = (double*)(currRow+byteCount);    
                byteCount += sizeof(double);
                break;
            default:
                DBIngestor_error("DBODBC - bindOneRowToStmt: an error occured in bindOneRowToStmt in switch while binding\n");
        }
        
        i++;
    }
    
    return 1;
}

int DBODBC::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, bool* isNullArray, void* preparedStatement, int nInStmt) {
    
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(isNullArray != NULL);
    assert(preparedStatement != NULL);
    
    //pass command through
    bindOneRowToStmt(thisSchema, thisData, preparedStatement, nInStmt);
    
    //now bind the NULLs
    ODBC_prepStmt * statement = (ODBC_prepStmt*) preparedStatement;
    int stride = nInStmt * (int)thisSchema->getNumActiveItems();
    
    //bind NULLs to the prepared statement
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        statement->isNullArray[stride+i] = isNullArray[i];
        
        i++;
    }
    
    return 1;
}

int DBODBC::executeStmt(void* preparedStatement) {
    recCount++;
    
    assert(preparedStatement != NULL);
    SQLRETURN result;
    ODBC_prepStmt *prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT * statement = prepStmt->statement;
    
    bindStatement((void*)prepStmt);
    
    result = SQLExecute(*statement);
    
    if(!SQL_SUCCEEDED(result)) {
        printf("DBODBC: Error\n");
        printODBCError("SQLExecute", *statement, SQL_HANDLE_STMT);

        //get error information
        SQLINTEGER i=0;
        SQLINTEGER native;
        SQLCHAR state[7];
        SQLCHAR text[SQL_MAX_MESSAGE_LENGTH];
        SQLSMALLINT len;
        SQLRETURN result;
    
        //extract information from ODBC
        result = SQLGetDiagRec(SQL_HANDLE_STMT, *statement, ++i, state, &native, text, sizeof(text), &len);

        if(!SQL_SUCCEEDED(result)) {
            DBIngestor_error("DBODBC - executeStmt: could not execute statement.\n");
        }

        if(resumeMode == true && native == 20017 && recCount < 1500) {

            SQLFreeStmt(*statement, SQL_RESET_PARAMS);
            SQLFreeHandle(SQL_HANDLE_STMT, *statement);

            disconnect();

            int count = 0;
            while(connect(myusr, mypwd, myhost, myport, mysocket) == 0 && count < 300) {
                count++;
                disonnect();
                sleep(10);
                printf("\nTrying to reconnect...\n\n");
            }

            printf("Reconnect successfull!\n");

            statement = (SQLHSTMT*)malloc(sizeof(SQLHSTMT));
            SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, statement);
            result = SQLPrepare(*statement, (SQLCHAR*)myquery.c_str(), SQL_NTS);

            prepStmt->statement = statement;

            if(!SQL_SUCCEEDED(result)) {
                printf("Error ODBC:\n");
                printf("Statement: %s\n", myquery.c_str());
                printODBCError("SQLPrepare", *statement, SQL_HANDLE_STMT);
                DBIngestor_error("DBODBC - prepareIngestStatement: could not prepare statement");
            }

            executeStmt((void*)prepStmt);

            recCount--;
            return -2;
        } else {
            DBIngestor_error("DBODBC - executeStmt: could not execute statement.\n");
        }
    }
    
    recCount--;
    return 1;
}

int DBODBC::finalizePreparedStatement(void* preparedStatement) {
    assert(preparedStatement != NULL);
    ODBC_prepStmt *prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT * statement = prepStmt->statement;
    
    SQLFreeStmt(*statement, SQL_RESET_PARAMS);
    SQLFreeHandle(SQL_HANDLE_STMT, *statement);
    
    deallocPrepStmt(prepStmt);
    
    return 1;
}

int DBODBC::maxRowsPerStmt(DBDataSchema::Schema * thisSchema) {
    float numRows;
    
    //2100 magic number for MS SQL Server //apparently it is smaller...
    if(dbServerName.find("Microsoft") != dbServerName.npos) {
        numRows = 1024.0 / thisSchema->getArrSchemaItems().size();
    }
    
    return (int)numRows;
}

void * DBODBC::initGetCompleteTable(DBDataSchema::Schema * thisSchema) {
    assert(thisSchema != NULL);
    
    SQLHSTMT * stmt = (SQLHSTMT*)malloc(sizeof(SQLHSTMT));
    
    if(stmt == NULL) {
        printf("DBODBC: Error\n");
        DBIngestor_error("DBODBC - initGetCompleteTable: error allocating ODBC statement.\n");
    }
    
    SQLRETURN result;
    ODBC_prepStmt * stmtContainer;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, stmt);
    
    stmtContainer = (ODBC_prepStmt*)allocPrepStmt(thisSchema->getNumActiveItems());
    
    stmtContainer->statement = stmt;
    stmtContainer->size = thisSchema->getArrSchemaItems().size();
    
    string query = "SELECT ";
    
    //insert column names
    int i=0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        if (i != thisSchema->getNumActiveItems() - 1) {
            query.append(", ");
        } else {
            query.append(" FROM ");
        }
        
        stmtContainer->type[i] = thisSchema->getArrSchemaItems().at(j)->getColumnDBType();
        stmtContainer->colSize[i] = thisSchema->getArrSchemaItems().at(j)->getColumnSize();
        stmtContainer->decDigits[i] = thisSchema->getArrSchemaItems().at(j)->getDecimalDigits();

        i++;
    }
    
    query.append(thisSchema->getDbName());
    query.append("..");
    query.append(thisSchema->getTableName());

    result = SQLPrepare(*stmt, (SQLCHAR*)query.c_str(), SQL_NTS);

    if(!SQL_SUCCEEDED(result)) {
        printf("DBODBC: Error\n");
        printODBCError("SQLPrepare", *stmtContainer->statement, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBC - initGetCompleteTable: could not prepare statement.\n");
    }

    return (void*)stmtContainer;
}

int DBODBC::getNextRow(DBDataSchema::Schema * thisSchema, void* thisData, void * preparedStatement) {
    assert(preparedStatement != NULL);
    
    SQLRETURN result;
    
    ODBC_prepStmt *prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT * statement = prepStmt->statement;

    if(prepStmt->prepared == false) {
        bindOneRowToStmt(thisSchema, thisData, preparedStatement, 0);

        bindColStatement((void*)prepStmt);
    
        result = SQLExecute(*statement);
    
        if(!SQL_SUCCEEDED(result)) {
            printf("DBODBC: Error\n");
            printODBCError("SQLExecute", *statement, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBC - getNextRow: could not execute statement.\n");
        }
        
        prepStmt->prepared = true;
    }
    
    result = SQLFetch(*statement);

    if(result == SQL_SUCCESS) {
        return 1;
    } else {
        return 0;
    }
}

DBDataSchema::DBType DBODBC::getType(SQLSMALLINT thisTypeID) {
    switch (thisTypeID) {
        case SQL_CHAR:
            return DBDataSchema::DBT_CHAR; 
        case SQL_VARCHAR:
            return DBDataSchema::DBT_CHAR; 
        case SQL_BIT:
            return DBDataSchema::DBT_BIT; 
        case SQL_TINYINT:
            return DBDataSchema::DBT_TINYINT; 
        case SQL_SMALLINT:
            return DBDataSchema::DBT_SMALLINT; 
        case SQL_INTEGER:
            return DBDataSchema::DBT_INTEGER; 
        case SQL_BIGINT:
            return DBDataSchema::DBT_BIGINT; 
        case SQL_FLOAT:
            return DBDataSchema::DBT_REAL;
        case SQL_DOUBLE:
            return DBDataSchema::DBT_REAL; 
        case SQL_REAL:
            return DBDataSchema::DBT_FLOAT;
            
        default:
            printf("Error ODBC:\n");
            printf("Err in ODBC type number: %i\n", thisTypeID);
            DBIngestor_error("DBODBC: this type used in the table is not yet supported. Please implement support...\n");
            return (DBDataSchema::DBType)0;
    }
}

void DBODBC::printODBCError(char *fn, SQLHANDLE thisHandle, SQLSMALLINT type) {
    SQLINTEGER i=0;
    SQLINTEGER native;
    SQLCHAR state[7];
    SQLCHAR text[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT len;
    SQLRETURN result;
    
    //extract information from ODBC
    do {
        result = SQLGetDiagRec(type, thisHandle, ++i, state, &native, text, sizeof(text), &len);
        
        if(SQL_SUCCEEDED(result)) {
            printf("ODBC: %s:%ld:%ld:%s\n", state, i ,native, text);
        }
    } while (result == SQL_SUCCESS);
}

void * DBODBC::allocPrepStmt(int numItems) {
    ODBC_prepStmt * stmtContainer;
    stmtContainer = (ODBC_prepStmt*)malloc(sizeof(ODBC_prepStmt));
    
    stmtContainer->size = numItems;
    
    //allocate buffers in statment container
    stmtContainer->type = (DBDataSchema::DBType*)malloc(numItems * sizeof(DBDataSchema::DBType));
    memset(stmtContainer->type, 0, numItems * sizeof(DBDataSchema::DBType));
    stmtContainer->parLenArray = (SQLLEN*)malloc(numItems * sizeof(SQLLEN));
    memset(stmtContainer->parLenArray, 0, numItems * sizeof(SQLLEN));
    stmtContainer->buffer = (void**)malloc(numItems * sizeof(void*));
    memset(stmtContainer->buffer, NULL, numItems * sizeof(void*));
    stmtContainer->isNullArray = (bool*)malloc(numItems * sizeof(bool));
    memset(stmtContainer->isNullArray, NULL, numItems * sizeof(bool));
    stmtContainer->colSize = (SQLULEN*)malloc(numItems * sizeof(SQLULEN));
    memset(stmtContainer->colSize, 0, numItems * sizeof(SQLULEN));
    stmtContainer->decDigits = (SQLINTEGER*)malloc(numItems * sizeof(SQLINTEGER));
    memset(stmtContainer->decDigits, 0, numItems * sizeof(SQLINTEGER));
    
    stmtContainer->prepared = false;
    
    stmtContainer->null = SQL_NULL_DATA;
    
    if(stmtContainer->type == NULL || stmtContainer->parLenArray == NULL || stmtContainer->buffer == NULL || stmtContainer->isNullArray == NULL) {
        printf("Error ODBC:\n");
        DBIngestor_error("DBODBC - allocPrepStmt: could not allocate statement");
    }       
    
    return (void*)stmtContainer;
}

void DBODBC::deallocPrepStmt(void * thisStatement) {
    assert(thisStatement != NULL);
    ODBC_prepStmt * stmtContainer = (ODBC_prepStmt*) thisStatement;
    
    if(stmtContainer->buffer != NULL) {
        free(stmtContainer->buffer);
    }
    if(stmtContainer->isNullArray != NULL) {
        free(stmtContainer->isNullArray);
    }
    if(stmtContainer->colSize != NULL) {
        free(stmtContainer->colSize);
    }
    if(stmtContainer->decDigits != NULL) {
        free(stmtContainer->decDigits);
    }
    if(stmtContainer->parLenArray != NULL) {
        free(stmtContainer->parLenArray);
    }
    if(stmtContainer->statement != NULL) {
        free(stmtContainer->statement);
    }
    if(stmtContainer->type != NULL) {
        free(stmtContainer->type);
    }
    
    free(stmtContainer);
}

