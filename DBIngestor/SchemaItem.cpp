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


#include "SchemaItem.h"
#include <assert.h>

using namespace DBDataSchema;
using namespace std;

SchemaItem::SchemaItem() {
    columnName = EMPTY_SCHEMAITEM_NAME;
    isNotNull = false;
    dataDesc = NULL;
}

SchemaItem::~SchemaItem() {
    if(dataDesc != NULL)
        delete dataDesc;    
}

string SchemaItem::getColumnName() {
	return columnName;
}

void SchemaItem::setColumnName(string newColumnName) {
	columnName = newColumnName;
}

DataObjDesc * SchemaItem::getDataDesc() {
	return dataDesc;
}

void SchemaItem::setDataDesc(DataObjDesc * newDataDesc) {    
	dataDesc = newDataDesc;
}

DBType SchemaItem::getColumnDBType() {
	return columnDBType;
}

void SchemaItem::setColumnDBType(DBType newColumnDBType) {
	assert(newColumnDBType > 0 && newColumnDBType <= DBT_MAXTYPE);
    
    columnDBType = newColumnDBType;
}

bool SchemaItem::getIsNotNull() {
    return isNotNull;
}

void SchemaItem::setIsNotNull(bool newIsNotNull) {
    isNotNull = newIsNotNull;
}

int32_t SchemaItem::getColumnSize() {
    return columnSize;
}

void SchemaItem::setColumnSize(int32_t newColumnSize) {
    columnSize = newColumnSize;
}

int32_t SchemaItem::getDecimalDigits() {
    return decimalDigits;
}

void SchemaItem::setDecimalDigits(int32_t newDecimalDigits) {
    decimalDigits = newDecimalDigits;
}