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

#include <stdlib.h>
#include <stdio.h>
#include "dbingestor_error.h"
#include <assert.h>
#include "DBAdaptorsFactory.h"

////////////////////////////////////////////////////
// your custom Converter goes here:
////////////////////////////////////////////////////
#ifdef DB_SQLITE3
#include "../../DBIngestor/DBIngestor/DBAdaptors/DBSqlite3.h"
#endif

#ifdef DB_MYSQL
#include "../../DBIngestor/DBIngestor/DBAdaptors/DBMySQL.h"
#endif

#ifdef DB_ODBC
#include "../../DBIngestor/DBIngestor/DBAdaptors/DBODBC.h"
#include "../../DBIngestor/DBIngestor/DBAdaptors/DBODBCBulk.h"
#endif

using namespace DBServer;
using namespace std;

DBAdaptorsFactory::DBAdaptorsFactory() {

}

DBAdaptorsFactory::~DBAdaptorsFactory() {
    
}

DBAbstractor * DBAdaptorsFactory::getDBAdaptors(string name) {
    DBServer::DBAbstractor * dbServer = NULL;
    int found = 0;
    
#ifdef DB_MYSQL
    if(name.compare("mysql") == 0) {
        found = 1;
        dbServer = new DBServer::DBMySQL();
    } 
#endif
    
#ifdef DB_SQLITE3
    if (name.compare("sqlite3") == 0) {
        found = 1;
        dbServer = new DBServer::DBSqlite3();
    }
#endif
    
#ifdef DB_ODBC
    if (name.compare("cust_odbc") == 0) {
        found = 1;
        dbServer = new DBServer::DBODBC();
    } 

    if (name.compare("unix_sqlsrv_odbc") == 0) {
        found = 1;
        dbServer = new DBServer::DBODBC();
    } 
    
    if (name.compare("sqlsrv_odbc") == 0) {
        found = 1;
        dbServer = new DBServer::DBODBC();
    } 
    
    if (name.compare("sqlsrv_odbc_bulk") == 0) {
       //TESTS ON SQL SERVER SHOWED THIS IS VERY SLOW. BUT NO CLUE WHY, DID NOT BOTHER TO LOOK AT PROFILER YET
       found = 1;
       dbServer = new DBServer::DBODBCBulk();
    }

    if (name.compare("cust_odbc_bulk") == 0) {
        //TESTS ON SQL SERVER SHOWED THIS IS VERY SLOW. BUT NO CLUE WHY, DID NOT BOTHER TO LOOK AT PROFILER YET
        found = 1;
        dbServer = new DBServer::DBODBCBulk();
    }
#endif    

    if (found == 0 || dbServer == NULL) {
        printf("Error: Sorry the database %s is not yet supported. To add support, implement the DBAbstractor class accordingly\n", name.c_str());
        DBIngestor_error("DBAdaptorsFactors: DB not yet supported.\n");
    }

    return dbServer;
}
