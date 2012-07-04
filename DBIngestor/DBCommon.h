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

/*! \file DBCommon.h
 \brief Common functions used by database wrapers
 
 This provides common functions used by the database wrapers
 */

#include <string>
#include "Schema.h"

#ifndef DBIngestor_DBCOMMON_h
#define DBIngestor_DBCOMMON_h

enum DBType_enum {
    DBTYPE_SQLITE = 1,
    DBTYPE_MYSQL = 2,
    DBTYPE_ODBC_MSSQL = 3
};

namespace DBServer {
    
    /*! \brief builds the insert into string used for inserting one row. 
     \param DBDataSchema::Schema * thisSchema: current schema
     \param void** thisData: pointer to the array of data
     \param DBType_enum dbType: type of database server
     
     \return returns the insert string
     
     This function build the insert into SQL statement that can then be passed on
     to the server from the data and the schema.*/
    std::string buildOneRowInsertString(DBDataSchema::Schema * thisSchema, void** thisData, DBType_enum dbType);
        
}
#endif
