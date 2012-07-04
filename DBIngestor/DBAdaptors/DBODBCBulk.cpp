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

#include "DBODBCBulk.h"
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
    SQLUSMALLINT * rowStatus;
    int size;
    int numCols;
} ODBC_prepStmt;

DBODBCBulk::DBODBCBulk() {
    odbcEnv = SQL_NULL_HENV;
    odbcDbc = SQL_NULL_HDBC;
}

DBODBCBulk::~DBODBCBulk() {
    disconnect();
}

int DBODBCBulk::connect(string usr, string pwd, string host, string port, string socket) {
    //ONLY ACCEPT DSNs. Anything else will fail miserably
    SQLCHAR output[1024];
    SQLSMALLINT outputLen;
    
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
        DBIngestor_error("DBODBCBulk: could not connect to ODBC database\n");
    }
    
    //check whether this ODBC driver supports bulk operations
    SQLUINTEGER infoResult;
    SQLGetInfo(odbcDbc, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, (SQLPOINTER)&infoResult, sizeof(infoResult), NULL);
    if(!(infoResult & SQL_CA1_BULK_ADD)) {
        printf("Error ODBC:\n");
        DBIngestor_error("DBODBCBulk: bulk operations not supported by this ODBC driver\n");
    }
    
    //read the database system
    SQLCHAR dbmsName[256];
    SQLGetInfo(odbcDbc, SQL_DBMS_NAME, (SQLPOINTER)dbmsName, sizeof(dbmsName), &outputLen);
    dbServerName.assign((char*)dbmsName, outputLen);
    
    isConnected = true;
    
    return 1;
}

int DBODBCBulk::disconnect() {
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

int DBODBCBulk::setSavepoint() {
    SQLHSTMT stmt;
    SQLRETURN result;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt);
    
    /*if(dbServerName.find("Microsoft") != dbServerName.npos) {
     SQLCHAR query[29] = "BEGIN TRAN @dbIngest_ODBC_sp";
     result = SQLExecDirect(stmt, query, 29);
     }*/
    
    result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_AUTOCOMMIT, (SQLUINTEGER*)SQL_AUTOCOMMIT_OFF, sizeof(SQLUINTEGER));
    
    if(!SQL_SUCCEEDED(result)) {
        printf("Error ODBC:\n");
        printODBCError("SQLSetConnectAttr", odbcDbc, SQL_HANDLE_DBC);
        DBIngestor_error("DBODBCBulk: could not set savepoint.\n");
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    
    return 1;
}

int DBODBCBulk::rollback() {
    SQLRETURN result;
    
    result = SQLEndTran(SQL_HANDLE_DBC, odbcDbc, SQL_ROLLBACK);
    
    if(!SQL_SUCCEEDED(result)) {
        printf("Error ODBC:\n");
        printODBCError("SQLEndTran", odbcDbc, SQL_HANDLE_DBC);
        DBIngestor_error("DBODBCBulk: rollback not successfull.\n");
    }
    
    result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_AUTOCOMMIT, (SQLUINTEGER*)SQL_AUTOCOMMIT_ON, sizeof(SQLUINTEGER));
    
    if(!SQL_SUCCEEDED(result)) {
        printf("Error ODBC:\n");
        printODBCError("SQLSetConnectAttr", odbcDbc, SQL_HANDLE_DBC);
        DBIngestor_error("DBODBCBulk: could not reset to autocommit mode.\n");
    }
    
    return 1;
}

int DBODBCBulk::releaseSavepoint() {
    SQLRETURN result;
    
    result = SQLEndTran(SQL_HANDLE_DBC, odbcDbc, SQL_COMMIT);
    
    if(!SQL_SUCCEEDED(result)) {
        printf("Error ODBC:\n");
        printODBCError("SQLEndTran", odbcDbc, SQL_HANDLE_DBC);
        DBIngestor_error("DBODBCBulk: savepoint not realeased successfully.\n");
    }
    
    result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_AUTOCOMMIT, (SQLUINTEGER*)SQL_AUTOCOMMIT_ON, sizeof(SQLUINTEGER));
    
    if(!SQL_SUCCEEDED(result)) {
        printf("Error ODBC:\n");
        printODBCError("SQLSetConnectAttr", odbcDbc, SQL_HANDLE_DBC);
        DBIngestor_error("DBODBCBulk: could not reset to autocommit mode.\n");
    }
    
    return 1;
}

int DBODBCBulk::disableKeys(DBDataSchema::Schema * thisSchema) {
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
                    DBIngestor_error("DBODBCBulk: could not disable keys.\n");
                }
            }
        }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    
    return 1;
}

int DBODBCBulk::enableKeys(DBDataSchema::Schema * thisSchema) {
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
                    DBIngestor_error("DBODBCBulk: could not reenable keys.\n");
                }
            }
        }
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    
    return 1;
}


DBDataSchema::Schema * DBODBCBulk::getSchema(string database, string table) {
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
        DBIngestor_error("DBODBCBulk - getSchema: table not found");
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
        
        DBDataSchema::SchemaItem * newItem = new DBDataSchema::SchemaItem;
        
        result = SQLGetData(stmt2, 4, SQL_C_CHAR, buffer, sizeof(buffer), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBCBulk - getSchema: could not read column name");
        }            
        
        result = SQLGetData(stmt2, 5, SQL_SMALLINT, &typeResult, sizeof(typeResult), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBCBulk - getSchema: could not read data type");
        }            
        
        result = SQLGetData(stmt2, 7, SQL_INTEGER, &columnSize, sizeof(columnSize), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBCBulk - getSchema: could not read column size");
        }            
        
        result = SQLGetData(stmt2, 9, SQL_SMALLINT, &decimalDigits, sizeof(decimalDigits), &ind);
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLGetData", stmt2, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBCBulk - getSchema: could not read data type");
        }            
        
        string colName(buffer);
        
        newItem->setColumnName(colName);
        
        //parse for type
        newItem->setColumnDBType(getType(typeResult));
        
        newItem->setColumnSize(columnSize);
        newItem->setDecimalDigits(decimalDigits);
        
        retSchema->addItemToSchema(newItem);            
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    
    return retSchema;
}

void* DBODBCBulk::prepareIngestStatement(DBDataSchema::Schema * thisSchema) {
    SQLHSTMT * stmt = (SQLHSTMT*)malloc(sizeof(SQLHSTMT));
    
    if(stmt == NULL) {
        printf("DBODBCBulk: Error\n");
        DBIngestor_error("DBODBCBulk - prepareMultiIngestStatement: error allocating ODBC statement.\n");
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
        if (i != thisSchema->getArrSchemaItems().size() - 1) {
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
        DBIngestor_error("DBODBCBulk - prepareIngestStatement: could not prepare statement");
    }            
    
    return (void*)stmtContainer;   
}

void* DBODBCBulk::prepareMultiIngestStatement(DBDataSchema::Schema * thisSchema, int numElements) {
    if(numElements > maxRowsPerStmt(thisSchema)) {
        printf("DBODBCBulk: Error\n");
        printf("max_prepared_stmt_count: %i\n", maxRowsPerStmt(thisSchema));
        DBIngestor_error("DBODBCBulk - prepareMultiIngestStatement: max_prepared_stmt_count has been violated.\n");
    }
    
    SQLHSTMT * stmt = (SQLHSTMT*)malloc(sizeof(SQLHSTMT));
    
    if(stmt == NULL) {
        printf("DBODBCBulk: Error\n");
        DBIngestor_error("DBODBCBulk - prepareMultiIngestStatement: error allocating ODBC statement.\n");
    }
    
    SQLRETURN result;
    ODBC_prepStmt * stmtContainer;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, stmt);

    result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_CURRENT_CATALOG, 
                               (SQLCHAR*)thisSchema->getDbName().c_str(), (SQLINTEGER)strlen(thisSchema->getDbName().c_str()));

    stmtContainer = (ODBC_prepStmt*)allocBulkPrepStmt(numElements, thisSchema);
    
    stmtContainer->statement = stmt;
    
    //construct query string
    //for bulk statements, this has to be one select, that initialises the statement handler properly
    string query = "SELECT ";
    
    int i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }

        if(i == 0) {
            query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        } else {
            query.append(", ");
            query.append(thisSchema->getArrSchemaItems().at(j)->getColumnName());
        }
        
        i++;
    }
    
    query.append(" FROM ");
    query.append(thisSchema->getDbName());
    query.append("..");
    query.append(thisSchema->getTableName());
    
    i = 0;
    for(int j=0; j<thisSchema->getArrSchemaItems().size(); j++) {
        if(thisSchema->getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        stmtContainer->type[i] = thisSchema->getArrSchemaItems().at(j)->getColumnDBType();
        stmtContainer->colSize[i] = thisSchema->getArrSchemaItems().at(j)->getColumnSize();
        stmtContainer->decDigits[i] = thisSchema->getArrSchemaItems().at(j)->getDecimalDigits();
        
        i++;
    }
    
    //setup bulk operation
    result = SQLSetStmtAttr(*stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER) SQL_CURSOR_DYNAMIC, NULL);
    result = SQLSetStmtAttr(*stmt, SQL_ATTR_CONCURRENCY, (SQLPOINTER) SQL_CONCUR_VALUES, NULL);
    result = SQLSetStmtAttr(*stmt, SQL_ATTR_ROW_STATUS_PTR, (SQLPOINTER) stmtContainer->rowStatus, NULL);
    
    //execute select statement to setup statement handler properly
    result = SQLExecDirect(*stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
    
    if(!SQL_SUCCEEDED(result)) {
        printf("Error ODBC:\n");
        printf("Statement: %s\n", query.c_str());
        printODBCError("SQLPrepare", *stmt, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBCBulk - prepareMultiIngestStatement: could not setup bulk statement");
    }
    
    bindStatement(stmtContainer);
    
    return (void*)stmtContainer;    
}

int DBODBCBulk::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData) {
    SQLHSTMT stmt;
    SQLRETURN result;
    
    SQLAllocHandle(SQL_HANDLE_STMT, odbcDbc, &stmt);
    
    result = SQLSetConnectAttr(odbcDbc, SQL_ATTR_CURRENT_CATALOG, 
                               (SQLCHAR*)thisSchema->getDbName().c_str(), (SQLINTEGER)strlen(thisSchema->getDbName().c_str()));
    
    string query;
    
    query = buildOneRowInsertString(thisSchema, thisData, DBTYPE_ODBC_MSSQL);
    
    if(query.size() == 0) {
        DBIngestor_error("DBODBCBulk - insertOneRow: an error occured in insertOneRow in switch while binding\n");
    }
    
    result = SQLExecDirect(stmt, (SQLCHAR*)query.c_str(), (SQLINTEGER)strlen(query.c_str()));
    
    if(!SQL_SUCCEEDED(result)) {
        printf("DBODBCBulk: Error\n");
        printf("Statement: %s\n", query.c_str());
        printODBCError("SQLExecDirect", stmt, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBCBulk - insertOneRow without prepared statement: an error occured in the ingest.\n");
    }
    
    return 1;    
}

int DBODBCBulk::insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData, void* preparedStatement) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    SQLHSTMT *statement;
    
    statement = (SQLHSTMT*)preparedStatement;
    
    bindOneRowToStmt(thisSchema, thisData, statement, 0);    
    
    executeStmt((void*)statement);
    
    return 1;
}

int DBODBCBulk::bindStatement(void* preparedStatement) {
    assert(preparedStatement != NULL);
    
    ODBC_prepStmt * prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT *statement = prepStmt->statement;
    SQLRETURN result;
    
    //bind data to the prepared statement
    for(int i=0; i<prepStmt->numCols; i++) {
        switch (prepStmt->type[i]) {
            case DBDataSchema::DBT_BIT: 
                result = SQLBindCol(*statement, i+1, SQL_C_BIT, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLCHAR), NULL);
                break;
            case DBDataSchema::DBT_BIGINT: 
                result = SQLBindCol(*statement, i+1, SQL_C_SBIGINT, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLBIGINT), NULL);
                break;
            case DBDataSchema::DBT_MEDIUMINT: 
                result = SQLBindCol(*statement, i+1, SQL_C_SLONG, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLINTEGER), NULL);
                break;
            case DBDataSchema::DBT_INTEGER: 
                result = SQLBindCol(*statement, i+1, SQL_C_SLONG, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLINTEGER), NULL);
                break;
            case DBDataSchema::DBT_SMALLINT: 
                result = SQLBindCol(*statement, i+1, SQL_C_SSHORT, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLSMALLINT), NULL);
                break;
            case DBDataSchema::DBT_TINYINT: 
                result = SQLBindCol(*statement, i+1, SQL_C_TINYINT, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLCHAR), NULL);
                break;
            case DBDataSchema::DBT_FLOAT: 
                result = SQLBindCol(*statement, i+1, SQL_C_FLOAT, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLREAL), NULL);
                break;
            case DBDataSchema::DBT_REAL: 
                result = SQLBindCol(*statement, i+1, SQL_C_DOUBLE, 
                                    (SQLPOINTER) prepStmt->buffer[i], 
                                    sizeof(SQLDOUBLE), NULL);
            default:
                printf("Type: %i\n", prepStmt->type[i]);
                DBIngestor_error("DBODBCBulk - bindStatement: an error occured in bindStatement in switch while binding\n");
        }
        
        if(!SQL_SUCCEEDED(result)) {
            printf("Error ODBC:\n");
            printODBCError("SQLBindCol", *statement, SQL_HANDLE_STMT);
            DBIngestor_error("DBODBCBulk - bindStatement: error in bindStatement");
        }
    }
    
    return 1;
}


int DBODBCBulk::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, void* preparedStatement, int nInStmt) {
    assert(thisSchema != NULL);
    assert(thisData != NULL);
    assert(preparedStatement != NULL);
    
    ODBC_prepStmt * prepStmt = (ODBC_prepStmt*) preparedStatement;
    long byteCount = 0;
    char * currRow = (char*)thisData;
    
    //bind data to the prepared statement
    for(int i=0; i<thisSchema->getNumActiveItems(); i++) {
        switch (prepStmt->type[i]) {
            case DBDataSchema::DBT_BIT: {
                SQLCHAR* arr = (SQLCHAR*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLCHAR)(*(int8_t*)(currRow+byteCount));    
                byteCount += sizeof(int8_t);}
                break;
            case DBDataSchema::DBT_BIGINT: {
                SQLBIGINT* arr = (SQLBIGINT*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLBIGINT)(*(int64_t*)(currRow+byteCount));    
                byteCount += sizeof(int64_t);}
                break;
            case DBDataSchema::DBT_MEDIUMINT: {
                SQLINTEGER* arr = (SQLINTEGER*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLINTEGER)(*(int32_t*)(currRow+byteCount));    
                byteCount += sizeof(int32_t);}
                break;
            case DBDataSchema::DBT_INTEGER: {
                SQLINTEGER* arr = (SQLINTEGER*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLINTEGER)(*(int32_t*)(currRow+byteCount));    
                byteCount += sizeof(int32_t);}
                break;
            case DBDataSchema::DBT_SMALLINT: {
                SQLSMALLINT* arr = (SQLSMALLINT*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLSMALLINT)(*(int16_t*)(currRow+byteCount));    
                byteCount += sizeof(int16_t);}
                break;
            case DBDataSchema::DBT_TINYINT: {
                SQLCHAR* arr = (SQLCHAR*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLCHAR)(*(int8_t*)(currRow+byteCount));    
                byteCount += sizeof(int8_t);}
                break;
            case DBDataSchema::DBT_FLOAT: {
                SQLREAL* arr = (SQLREAL*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLREAL)(*(float*)(currRow+byteCount));    
                byteCount += sizeof(float);}
                break;
            case DBDataSchema::DBT_REAL: {
                SQLDOUBLE* arr = (SQLDOUBLE*)prepStmt->buffer[i];
                arr[nInStmt] = (SQLDOUBLE)(*(double*)(currRow+byteCount));    
                byteCount += sizeof(double);}
            default:
                DBIngestor_error("DBODBCBulk - bindOneRowToStmt: an error occured in bindOneRowToStmt in switch while binding\n");
        }
    }
    
    return 1;
}

int DBODBCBulk::bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, bool* isNullArray, void* preparedStatement, int nInStmt) {
    
    bindOneRowToStmt(thisSchema, thisData, preparedStatement, nInStmt);
    
    return 1;
}

int DBODBCBulk::executeStmt(void* preparedStatement) {
    assert(preparedStatement != NULL);
    SQLRETURN result;
    ODBC_prepStmt *prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT * statement = prepStmt->statement;
    
    result = SQLSetStmtAttr(*statement, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) prepStmt->size, NULL);
    result = SQLSetStmtAttr(*statement, SQL_ATTR_CONCURRENCY, (SQLPOINTER) SQL_CONCUR_VALUES, NULL);
    
    result = SQLBulkOperations(*statement, SQL_ADD);
    
    if(!SQL_SUCCEEDED(result)) {
        printf("DBODBCBulk: Error\n");
        printODBCError("SQLExecute", *statement, SQL_HANDLE_STMT);
        DBIngestor_error("DBODBCBulk - executeStmt: could not execute statement.\n");
    }
    
    return 1;
}

int DBODBCBulk::finalizePreparedStatement(void* preparedStatement) {
    assert(preparedStatement != NULL);
    ODBC_prepStmt *prepStmt = (ODBC_prepStmt*) preparedStatement;
    SQLHSTMT * statement = prepStmt->statement;
    
    SQLFreeStmt(*statement, SQL_RESET_PARAMS);
    SQLFreeHandle(SQL_HANDLE_STMT, *statement);
    
    deallocPrepStmt(prepStmt);
    
    return 1;
}

int DBODBCBulk::maxRowsPerStmt(DBDataSchema::Schema * thisSchema) {
    float numRows;
    
    //2100 magic number for MS SQL Server
    if(dbServerName.find("Microsoft") != dbServerName.npos) {
        numRows = 10000; //2100.0 / thisSchema->getArrSchemaItems().size();
    }
    
    return (int)numRows;
}


DBDataSchema::DBType DBODBCBulk::getType(SQLSMALLINT thisTypeID) {
    switch (thisTypeID) {
        case SQL_CHAR:
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
            return DBDataSchema::DBT_FLOAT; 
        case SQL_DOUBLE:
            return DBDataSchema::DBT_REAL; 
        case SQL_REAL:
            return DBDataSchema::DBT_REAL; 
            
        default:
            printf("Error ODBC:\n");
            printf("Err in ODBC type number: %i\n", thisTypeID);
            DBIngestor_error("DBODBCBulk: this type used in the table is not yet supported. Please implement support...\n");
            return (DBDataSchema::DBType)0;
    }
}

void DBODBCBulk::printODBCError(char *fn, SQLHANDLE thisHandle, SQLSMALLINT type) {
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

void * DBODBCBulk::allocPrepStmt(int numItems) {
    ODBC_prepStmt * stmtContainer;
    stmtContainer = (ODBC_prepStmt*)malloc(sizeof(ODBC_prepStmt));
    
    stmtContainer->size = numItems;
    
    //allocate buffers in statment container
    stmtContainer->type = (DBDataSchema::DBType*)malloc(numItems * sizeof(DBDataSchema::DBType));
    memset(stmtContainer->type, 0, numItems * sizeof(DBDataSchema::DType));
    stmtContainer->parLenArray = (SQLLEN*)malloc(numItems * sizeof(SQLLEN));
    memset(stmtContainer->parLenArray, 0, numItems * sizeof(SQLLEN));
    stmtContainer->buffer = (void**)malloc(numItems * sizeof(void*));
    memset(stmtContainer->buffer, NULL, numItems * sizeof(void*));
    stmtContainer->colSize = (SQLULEN*)malloc(numItems * sizeof(SQLULEN));
    memset(stmtContainer->colSize, 0, numItems * sizeof(SQLULEN));
    stmtContainer->decDigits = (SQLINTEGER*)malloc(numItems * sizeof(SQLINTEGER));
    memset(stmtContainer->decDigits, 0, numItems * sizeof(SQLINTEGER));
    stmtContainer->rowStatus = NULL;
    
    if(stmtContainer->type == NULL || stmtContainer->parLenArray == NULL || stmtContainer->buffer == NULL) {
        printf("Error ODBC:\n");
        DBIngestor_error("DBODBCBulk - allocPrepStmt: could not allocate statement");
    }       
    
    return (void*)stmtContainer;
}

void DBODBCBulk::deallocPrepStmt(void * thisStatement) {
    assert(thisStatement != NULL);
    ODBC_prepStmt * stmtContainer = (ODBC_prepStmt*) thisStatement;

    if(stmtContainer->buffer != NULL) {
        free(stmtContainer->buffer);
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

void * DBODBCBulk::allocBulkPrepStmt(int numItems, DBDataSchema::Schema * thisSchema) {
    ODBC_prepStmt * stmtContainer;
    stmtContainer = (ODBC_prepStmt*)malloc(sizeof(ODBC_prepStmt));
    
    size_t numCols = thisSchema->getNumActiveItems();
    stmtContainer->size = numItems;
    stmtContainer->numCols = numCols;
    
    //allocate buffers in statment container
    stmtContainer->type = (DBDataSchema::DBType*)malloc(numCols * sizeof(DBDataSchema::DBType));
    memset(stmtContainer->type, 0, numCols * sizeof(DBDataSchema::DType));
    stmtContainer->parLenArray = (SQLLEN*)malloc(numCols * sizeof(SQLLEN));
    memset(stmtContainer->parLenArray, 0, numCols * sizeof(SQLLEN));
    stmtContainer->buffer = (void**)malloc(numCols * sizeof(void*));
    memset(stmtContainer->buffer, NULL, numCols * sizeof(void*));
    stmtContainer->colSize = (SQLULEN*)malloc(numCols * sizeof(SQLULEN));
    memset(stmtContainer->colSize, 0, numCols * sizeof(SQLULEN));
    stmtContainer->decDigits = (SQLINTEGER*)malloc(numCols * sizeof(SQLINTEGER));
    memset(stmtContainer->decDigits, 0, numCols * sizeof(SQLINTEGER));
    stmtContainer->rowStatus = (SQLUSMALLINT*)malloc(numItems * sizeof(SQLUSMALLINT));
    memset(stmtContainer->rowStatus, 0, numItems * sizeof(SQLUSMALLINT));
    
    if(stmtContainer->type == NULL || stmtContainer->parLenArray == NULL || 
       stmtContainer->buffer == NULL || stmtContainer->rowStatus == NULL) {
        printf("Error ODBC:\n");
        DBIngestor_error("DBODBCBulk - allocPrepStmtBulk: could not allocate statement");
    }       
    
    //allocate numItems memory to hold data
    for(int i=0; i<numCols; i++) {
        DBDataSchema::SchemaItem * thisItem = thisSchema->getArrSchemaItems().at(i);

        switch (thisItem->getColumnDBType()) {
            case DBDataSchema::DBT_BIT:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLCHAR));
                break;
            case DBDataSchema::DBT_BIGINT:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLBIGINT));
                break;
            case DBDataSchema::DBT_MEDIUMINT:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLINTEGER));
                break;
            case DBDataSchema::DBT_INTEGER:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLINTEGER));
                break;
            case DBDataSchema::DBT_SMALLINT:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLSMALLINT));
                break;
            case DBDataSchema::DBT_TINYINT:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLSCHAR));
                break;
            case DBDataSchema::DBT_FLOAT:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLREAL));
                break;
            case DBDataSchema::DBT_REAL:
                stmtContainer->buffer[i] = (void*)malloc(numItems * sizeof(SQLDOUBLE));
                break;
            default:
                printf("Error ODBC:\n");
                DBIngestor_error("DBODBCBulk - allocPrepStmt: CHAR, ANY, DATE and TIME are not supported by the ODBCBulkIngestor");
        }
        
        if(stmtContainer->buffer[i] == NULL) {
            printf("Error ODBC:\n");
            DBIngestor_error("DBODBCBulk - allocPrepStmtBulk: could not allocate bulk buffer");
        }
    }
    
    return (void*)stmtContainer;
}

void DBODBCBulk::deallocBulkPrepStmt(void * thisStatement) {
    assert(thisStatement != NULL);
    
    ODBC_prepStmt * stmtContainer = (ODBC_prepStmt*) thisStatement;
    
    if(stmtContainer->buffer != NULL) {
        for(int i=0; i<stmtContainer->size; i++) {
            if(stmtContainer->buffer[i] != NULL) {
                free(stmtContainer->buffer[i]);
            }
        }
        
        free(stmtContainer->buffer);
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

