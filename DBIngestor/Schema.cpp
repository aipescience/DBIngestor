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
#include "Schema.h"
#include <assert.h>
#include <iostream>
#include <algorithm>

using namespace DBDataSchema;
using namespace std;

Schema::Schema() {
    numActiveItems = -1;
}

Schema::~Schema() {
    //go through the schema item vector and deallocate
    //everything there
    
    if(arrSchemaItems.size() > 0) {
        for (int i=0; i<arrSchemaItems.size(); i++) {
            delete arrSchemaItems.at(i);
        }
    }
}

string Schema::getDbName() {
	return dbName;
}

void Schema::setDbName(string newDbName) {
    dbName = newDbName;
}

string Schema::getTableName() {
    return tableName;
}

void Schema::setTableName(string newTableName) {
    tableName = newTableName;
}

vector<SchemaItem*> & Schema::getArrSchemaItems() {
	return arrSchemaItems;
}

int32_t Schema::getNumActiveItems() {
    if(numActiveItems < 0) {
        numActiveItems = 0;
        
        for (int i=0; i<arrSchemaItems.size(); i++) {
            if(arrSchemaItems.at(i)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) != 0) {
                numActiveItems++;
            }
        }
    }
    
    return numActiveItems;
}

void Schema::addItemToSchema(SchemaItem * thisItem) {
    arrSchemaItems.push_back(thisItem);
}

void Schema::sortSchema() {
    std::sort(arrSchemaItems.begin(), arrSchemaItems.end(), & compSchemaItem);
}

bool DBDataSchema::compSchemaItem (SchemaItem * i, SchemaItem * j) { 
    return (i->getDataDesc()->getOffsetId() < j->getDataDesc()->getOffsetId());
}

int64_t Schema::getRowSizeInBytes() {
    int64_t byteCount = 0;
    
    for(int j=0; j<getArrSchemaItems().size(); j++) {
        if(getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        DBDataSchema::SchemaItem * currObj = getArrSchemaItems().at(j);        
        
        if(currObj->getColumnDBType() == DBDataSchema::DBT_CHAR) {
            byteCount += sizeof(char*);
        } else {
            byteCount += DBDataSchema::getByteLenOfDBType(currObj->getColumnDBType());
        }
    }
    
    return byteCount;
}

void Schema::printSchema() {
    for(int j=0; j<getArrSchemaItems().size(); j++) {
        if(getArrSchemaItems().at(j)->getColumnName().compare(EMPTY_SCHEMAITEM_NAME) == 0) {
            continue;
        }
        
        string dbTypeStr = strDBType(getArrSchemaItems().at(j)->getColumnDBType());        
        printf("%i: '%s' - Type: %s\n", j, getArrSchemaItems().at(j)->getColumnName().c_str(), dbTypeStr.c_str());
    }
}



