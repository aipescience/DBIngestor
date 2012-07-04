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

//Example that writes "Hello world" and 42 into a table with 
//a column "name" (char) and "const" (integer)

#include <iostream>
#include "HelloWorld_Reader.h"
#include "HelloWorld_SchemaMapper.h"
#include "../../../../DBIngestor/DBIngestor/Schema.h"
#include "../../../../DBIngestor/DBIngestor/DBIngestor.h"
#include "../../../../DBIngestor/DBIngestor/DBAdaptorsFactory.h"
#include "../../../../DBIngestor/DBIngestor/AsserterFactory.h"
#include "../../../../DBIngestor/DBIngestor/ConverterFactory.h"

using namespace HelloWorld;

int main (int argc, const char * argv[])
{
    DBServer::DBAbstractor * dbServer;
    DBIngest::DBIngestor * asciiIngestor;
    DBServer::DBAdaptorsFactory adaptorFac;

    DBAsserter::AsserterFactory * assertFac = new DBAsserter::AsserterFactory;
    DBConverter::ConverterFactory * convFac = new DBConverter::ConverterFactory;
    HelloWorldSchemaMapper * thisSchemaMapper = new HelloWorldSchemaMapper(assertFac, convFac);     //registering the converter and asserter factories
    //HelloWorldSchemaMapper * thisSchemaMapper = new HelloWorldSchemaMapper();
    
    DBDataSchema::Schema * thisSchema;
    //thisSchema = thisSchemaMapper->generateSchema("test", "helloWorld");
    thisSchema = thisSchemaMapper->generateSchema("spider", "helloWorld");
    
    HelloWorldReader * thisReader = new HelloWorldReader();

    dbServer = adaptorFac.getDBAdaptors("mysql");
    //dbServer = adaptorFac.getDBAdaptors("unix_sqlsrv_odbc");
    asciiIngestor = new DBIngest::DBIngestor(thisSchema, thisReader, dbServer);
    asciiIngestor->setUsrName("root");
    asciiIngestor->setPasswd("");

    //settings for MS SQL Server through FreeTDS ODBC
    //asciiIngestor->setSocket("DRIVER=FreeTDS;TDS_Version=7.0;");
    //asciiIngestor->setPort("1433");
    
    //settings for mysql - if this should work with other DBs, addapt here
    asciiIngestor->setSocket("");
    asciiIngestor->setPort("3306");
    asciiIngestor->setHost("localhost");
   
    //now ingest data after setup
    asciiIngestor->setPerformanceMeter(2);
    asciiIngestor->ingestData(5);    
    
    delete thisSchemaMapper;
    delete thisSchema;
    //delete assertFac;
    //delete convFac;

    return 0;
}

