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

#include "DBIngestor.h"
#include "DBIngestBuffer.h"
#include "dbingestor_error.h"
#include <assert.h>
#include <stdio.h>
#include <string>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "DType.h"

using namespace DBIngest;
using namespace std;

#define DBING_RESULT_BUFFER_SIZE 128

DBIngestor::DBIngestor() {
    disableKeys = 0;
    enableKeys = 0;
    askUserToValidateRead = 1;
    myDBAbstractor = NULL;
    myDBSchema = NULL;
    myReader = NULL;
}

DBIngestor::~DBIngestor() {
    
}

DBIngestor::DBIngestor(DBDataSchema::Schema * newSchema, DBReader::Reader * newReader, DBServer::DBAbstractor * newAbstractor) {
    assert(newSchema != NULL);
    assert(newReader != NULL);
    assert(newAbstractor != NULL);
    
    disableKeys = 0;
    enableKeys = 0;
    myDBAbstractor = NULL;
    myDBSchema = NULL;
    myReader = NULL;
    askUserToValidateRead = 1;
    
    setSchema(newSchema);
    setReader(newReader);
    setDBAbstractor(newAbstractor);
    setSocket("");
}

int DBIngestor::validateSchema() {
    //TODO: Make this check more extensive! Check for isnull and stuff... i.e. search for missing data
    assert(myDBAbstractor != NULL);
    assert(myDBSchema != NULL);
    
    //get schema from server
    DBDataSchema::Schema * srvSchema = myDBAbstractor->getSchema(myDBSchema->getDbName(), myDBSchema->getTableName());
    
    //check if input schema has less elements than server side. this would mean, something is incomplete
    if(myDBSchema->getArrSchemaItems().size() < srvSchema->getArrSchemaItems().size()) {
        printf("DBIngestor ERROR:\n");
        
        printf("\nA list of all columns in the schema file:\n");
        myDBSchema->printSchema();
        
        printf("\nA list of all columns on the server:\n");
        srvSchema->printSchema();
        
        printf("DBIngestor: Warning in matching the schemas. The schema you defined has less entries than the one on the server side.\n");
        
        if(getAskUserToValidateRead() == 1) {
            string answer;
            printf("\nPlease tell me if I should continue or not:\n");
            printf("Please enter (Y/n): ");
            getline(cin, answer);
            
            if(answer.length() != 0 && answer.at(0) != 'Y' && answer.at(0) != 'y') {
                printf("\nDBIngestor: You don't want to continue.\n");
                return 0;
            } 
        }
    }
    
    //compare the two schemas and return 0 if they are not compatible
    for(int i=0; i<myDBSchema->getArrSchemaItems().size(); i++) {
        DBDataSchema::SchemaItem * currLocalItem = myDBSchema->getArrSchemaItems().at(i);
        
        //skip any schema item that we donot want to add to the database
        if(currLocalItem->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        //search for this schema in the server's answer
        int success = 0;
        for(int j=0; j<srvSchema->getArrSchemaItems().size(); j++) {
            DBDataSchema::SchemaItem * currSrvItem = srvSchema->getArrSchemaItems().at(j);
            
            if(currSrvItem->getColumnName().compare(currLocalItem->getColumnName()) == 0) {
                //we have found a column with the same name, good...
                success = 1;
                
                //now the types need to match as well
                if(currSrvItem->getColumnDBType() == currLocalItem->getColumnDBType() ||
                   currSrvItem->getColumnDBType() == DBDataSchema::DBT_ANY) {
                    success = 2;
                    
                    //copy additional information from schema over
                    currLocalItem->setColumnSize(currSrvItem->getColumnSize());
                    currLocalItem->setDecimalDigits(currSrvItem->getDecimalDigits());
                    currLocalItem->setIsNotNull(currSrvItem->getIsNotNull());
                    
                    break;
                }
            }
        }
        
        if(success != 2) {
            printf("DBIngestor ERROR:\n");
            printf("Success: %i\n", success); 
            printf("Requested column: %s\n", currLocalItem->getColumnName().c_str());
            
            printf("\nA list of all columns in the schema file:\n");
            myDBSchema->printSchema();

            printf("\nA list of all columns on the server:\n");
            srvSchema->printSchema();
            
            printf("DBIngestor: Error in matching the schemas. Could not find this schema column.\n");
            return 0;
        }
    }
    
	return 1;
}

int DBIngestor::ingestData(int lenBuffer) {
    assert(myDBAbstractor != NULL);
    assert(myDBSchema != NULL);
    assert(myReader != NULL);
    int err;
    
    //open connection to the database
    if(myDBAbstractor->getIsConnected() == false) {
        err = myDBAbstractor->connect(getUsrName(), getPasswd(), getHost(), getPort(), getSocket());
    }
        
    //first validate schema
    err = validateSchema();
    if(err != 1) {
        DBIngestor_error("DBIngestor: Error in matching the schemas. Check the errors above for information\n");
    }
    
    if(isDryRun != true) {
        printf("Setting savepoint...\n");
        err = myDBAbstractor->setSavepoint();
        printf("Setting savepoint DONE\n");
    }
    
    if(disableKeys != 0 && isDryRun != true) {
        printf("Disabling keys...\n");
        err = myDBAbstractor->disableKeys(myDBSchema);
        printf("Disabling keys DONE\n");
    }
    
    DBIngest::DBIngestBuffer * ingestBuff = new DBIngestBuffer(myDBSchema, myDBAbstractor);
    ingestBuff->setBufferSize(lenBuffer);
        
    ingestBuff->setIsDryRun(isDryRun);
    
    //loop through the data and ingest
    myReader->rewind();
    myReader->skipHeader();
    
    //this is a buffer for the results of various size (double or long long is the maximum?)
    char result[DBING_RESULT_BUFFER_SIZE];
    bool isNull;
    
    //performance output stuff
    int64_t counter = 0;
    boost::posix_time::ptime startTime;
    boost::posix_time::ptime endTime;
    startTime = boost::posix_time::microsec_clock::universal_time();
    
    printf("Starting ingest...\n");
    
    while(myReader->getNextRow()) {
        ingestBuff->newRow();
        
        for(int i=0; i<myDBSchema->getArrSchemaItems().size(); i++) {
            //skip any schema item that we donot want to add to the database
            if(myDBSchema->getArrSchemaItems().at(i)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
                continue;
            }

            isNull = myReader->getItemInRow(myDBSchema->getArrSchemaItems().at(i)->getDataDesc(), 1, 1, &result);
            
            err = ingestBuff->addToRow(result, isNull, myDBSchema->getArrSchemaItems().at(i));
            
            if(err != 1) {
                printf("Error in reading line %i\n", counter);
                printf("Problem with schema item number: %i\n", i);
                DBIngestor_error("DBIngestor: Ingesting NULL in column that is set IS NOT NULL!\n");
            }
            
            //if this was a string, free it on the reader side... it was already copied somewhere else...
            if(myDBSchema->getArrSchemaItems().at(i)->getDataDesc()->getDataObjDType() == DBDataSchema::DT_STRING) {
                if(myDBSchema->getArrSchemaItems().at(i)->getDataDesc()->getIsConstItem() != true)
                    free(*(char**)result);
            }
        }
        
        counter++;
        
        if(performanceMeter != -1 && counter % performanceMeter == 0) {
            endTime = boost::posix_time::microsec_clock::universal_time();
            printf("Time took to ingest %lld (current %lld) rows: %lld ms\n", performanceMeter, counter, (endTime-startTime).total_milliseconds());
            startTime = boost::posix_time::microsec_clock::universal_time();
        }
    }

    if(isDryRun != true) {
        ingestBuff->commit();
    }

    if(performanceMeter != -1) {
        endTime = boost::posix_time::microsec_clock::universal_time();
        printf("Time took to ingest %lld (current %lld) rows: %lld ms\n", performanceMeter, counter, (endTime-startTime).total_milliseconds());
        startTime = boost::posix_time::microsec_clock::universal_time();
    }

    delete ingestBuff;
    
    printf("Ingest DONE\n");

    if(enableKeys != 0 && isDryRun != true) {
        printf("Re-enabling keys...\n");
        err = myDBAbstractor->enableKeys(myDBSchema);
        printf("Re-enabling key DONE\n");
    }

    if(isDryRun != true) {
        printf("Releasing savepoint...\n");
        myDBAbstractor->releaseSavepoint();
        printf("Releasing savepoint DONE\n");
    }
    
    return 1;
}

string DBIngestor::getUsrName() {
	return usrName;
}

void DBIngestor::setUsrName(string newUsrName) {
	usrName = newUsrName;
}

string DBIngestor::getPasswd() {
	return passwd;
}

void DBIngestor::setPasswd(string newPasswd) {
	passwd = newPasswd;
}

string DBIngestor::getHost() {
	return host;
}

void DBIngestor::setHost(string newHost) {
	host = newHost;
}

string DBIngestor::getPort() {
	return port;
}

void DBIngestor::setPort(string newPort) {
	port = newPort;
}

string DBIngestor::getSocket() {
	return socket;
}

void DBIngestor::setSocket(string newSocket) {
	socket = newSocket;
}

uint32_t DBIngestor::getDisableKeys() {
    return disableKeys;
}

void DBIngestor::setDisableKeys(uint32_t newDisableKeys) {
    assert(newDisableKeys == 0 || newDisableKeys == 1);
    
    disableKeys = newDisableKeys;
}

uint32_t DBIngestor::getEnableKeys() {
    return enableKeys;
}

void DBIngestor::setEnableKeys(uint32_t newEnableKeys) {
    assert(newEnableKeys == 0 || newEnableKeys == 1);
    
    enableKeys = newEnableKeys;
}

int64_t DBIngestor::getPerformanceMeter() {
    return performanceMeter;
}

void DBIngestor::setPerformanceMeter(int64_t newPerformanceMeter) {
    assert(newPerformanceMeter >= -1);
    
    performanceMeter = newPerformanceMeter;
}

bool DBIngestor::getIsDryRun() {
    return isDryRun;
}

void DBIngestor::setIsDryRun(bool newIsDryRun) {
    isDryRun = newIsDryRun;
}

DBDataSchema::Schema * DBIngestor::getSchema() {
	return myDBSchema;
}

void DBIngestor::setSchema(DBDataSchema::Schema * newDBSchema) {
	assert(newDBSchema != NULL);
    
    myDBSchema = newDBSchema;
    
    if(myReader != NULL) {
        myReader->setSchema(myDBSchema);
    }
}

DBReader::Reader * DBIngestor::getReader() {
	return myReader;
}

void DBIngestor::setReader(DBReader::Reader * newReader) {
	assert(newReader != NULL);
    
    myReader = newReader;
    
    if(myDBSchema != NULL) {
        myReader->setSchema(myDBSchema);
    }
}

DBServer::DBAbstractor * DBIngestor::getDBAbstractor() {
    return myDBAbstractor;
}

void DBIngestor::setDBAbstractor(DBServer::DBAbstractor * newDBAbstractor) {
    assert(newDBAbstractor != NULL);
    
    myDBAbstractor = newDBAbstractor;    
}

bool DBIngestor::getAskUserToValidateRead() {
    return askUserToValidateRead;
}

void DBIngestor::setAskUserToValidateRead(bool val) {
    assert(val == 0 || val == 1);
    
    askUserToValidateRead = val;
}
