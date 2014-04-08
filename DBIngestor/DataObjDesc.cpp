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

#include "DataObjDesc.h"
#include "dbingestor_error.h"
#include <assert.h>
#include <stdlib.h>
#include <cstring>

using namespace DBDataSchema;
using namespace std;

DataObjDesc::DataObjDesc() {
    isConstData = 0;
    isHeaderItem = 0;
    constData = NULL;
    conversionEvaluated = false;
    assertionsEvaluated = false;
}

DataObjDesc::~DataObjDesc() {
    if(constData != NULL) 
        free(constData);

}

int DataObjDesc::getOffsetId() {
	return offsetId;
}

void DataObjDesc::setOffsetId(int newOffsetId) {
    assert(newOffsetId >= 0);
    
    offsetId = newOffsetId;
}

string DataObjDesc::getDataObjName() {
	return dataObjName;
}

void DataObjDesc::setDataObjName(string newDataObjName) {
    dataObjName = newDataObjName;
}

bool DataObjDesc::getIsHeaderItem() {
	return isHeaderItem;
}

void DataObjDesc::setIsHeaderItem(bool newIsHeaderItem) {
    assert(newIsHeaderItem == 0 || newIsHeaderItem == 1);
    
    isHeaderItem = newIsHeaderItem;
}

bool DataObjDesc::getIsConstItem() {
    return isConstData;
}

void DataObjDesc::setIsConstItem(bool newIsConstItem,  bool newIsStorage) {
    assert(newIsConstItem == 0 || newIsConstItem == 1);
    assert(newIsStorage == 0 || newIsStorage == 1);
    
    isConstData = newIsConstItem;
    isStoreageConstData = newIsStorage;
}

bool DataObjDesc::getIsStorageItem() {
    return isStoreageConstData;
}

DBAsserter::Asserter * DataObjDesc::getAssertion(unsigned long index) {
    assert(index < assertions.size());
    
    return assertions.at(index);
}

void DataObjDesc::addAssertion(DBAsserter::Asserter * newAssertion) {
    assert(newAssertion != NULL);
    
    assertions.push_back(newAssertion);
}

unsigned long DataObjDesc::getNumAssertions() {
    return assertions.size();
}

DBConverter::Converter * DataObjDesc::getConversion(unsigned long index) {
    assert(index < conversions.size());
    
    return conversions.at(index);
}

void DataObjDesc::addConverter(DBConverter::Converter * newConverter) {
    assert(newConverter != NULL);
    
    conversions.push_back(newConverter);
}

unsigned long DataObjDesc::getNumConverters() {
    return conversions.size();
}


void * DataObjDesc::getConstData() {
    return constData;
}

void DataObjDesc::setConstData(void * newConstData) {
    assert(newConstData != NULL);
    
    constData = newConstData;
}

void DataObjDesc::updateConstData(void * newConstData) {
    assert(constData != NULL);
    
    memcpy(constData, newConstData, DBDataSchema::getByteLenOfDType(getDataObjDType()));
}


DType DataObjDesc::getDataObjDType() {
    return dataObjDType;
}

void DataObjDesc::setDataObjDType(DType newDataObjDType) {
    assert(newDataObjDType > 0 && newDataObjDType <= DT_MAXTYPE);
    
    dataObjDType = newDataObjDType;
}

bool DataObjDesc::getConversionEvaluated() {
    return conversionEvaluated;
}

bool DataObjDesc::setConversionEvaluated(bool value) {
    conversionEvaluated = value;
}

bool DataObjDesc::getAssertionEvaluated() {
    return assertionsEvaluated;
}

bool DataObjDesc::setAssertionEvaluated(bool value) {
    assertionsEvaluated = value;
}

bool DataObjDesc::resetForNextRow() {
    conversionEvaluated = false;
    assertionsEvaluated = false;
}


