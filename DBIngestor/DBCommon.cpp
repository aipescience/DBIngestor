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

#include "DBCommon.h"
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

using namespace DBServer;
using namespace std;

string DBServer::buildOneRowInsertString(DBDataSchema::Schema * thisSchema, void** thisData, DBType_enum dbType) {
    //construct query string
    string query = "INSERT INTO ";
    
    if(dbType == DBTYPE_MYSQL) {
        query.append(thisSchema->getDbName());
        query.append(".");
    } else if (dbType == DBTYPE_ODBC_MSSQL) {
        query.append(thisSchema->getDbName());
        query.append("..");
    }
    
    query.append(thisSchema->getTableName());
    query.append("(");
    
    //insert column names
    for(int i=0; i<thisSchema->getArrSchemaItems().size(); i++) {
        query.append(thisSchema->getArrSchemaItems().at(i)->getColumnName());
        if (i != thisSchema->getArrSchemaItems().size() - 1) {
            query.append(", ");
        } else {
            query.append(") VALUES (");
        }
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
                return query;
        }
        
        if (i != thisSchema->getArrSchemaItems().size() - 1) {
            query.append(", ");
        } else {
            query.append(")");
        }
    }
    
    return query;
}
