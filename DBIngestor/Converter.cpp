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

#include <stdlib.h>
#include <stdio.h>
#include "dbingestor_error.h"
#include <assert.h>
#include "Converter.h"

using namespace DBConverter;
using namespace std;

#define CONV_RESULT_BUFFER_SIZE 128

Converter::Converter() {
    result = NULL;
}

Converter::~Converter() {
    if(result != NULL) {
        free(result);
    }

    //deallocate function parameter array
    for(int i=0; i<functionParamArray.size(); i++) {
        free(functionParamArray.at(i).validTypeArray);
    }
}

int Converter::checkDType(DBDataSchema::DType thisDType) {
    for(int i=0; i < DTypeArray.size(); i++) {
        if(thisDType == DTypeArray.at(i)) {
            return 1;
        }
    }
    
    return 0;
}

string Converter::getName() {
	return name;
}

void Converter::setName(string newString) {
    name = newString;
}

void Converter::setDTypeArray(DBDataSchema::DType *newDTypeArray, int size) {
    assert(newDTypeArray != NULL);
    assert(size >= 0);
    
    for(int i=0; i<size; i++) {
        DTypeArray.push_back(newDTypeArray[i]);
    }
}

void Converter::setFunctionParameters(convFunctionParam * paramArray, int size) {
    for(int i=0; i<size; i++) {
        convFunctionParam currParam;
        
        //allocate permanent memory for dtype array
        DBDataSchema::DType * currDTypeArr = (DBDataSchema::DType *)malloc(paramArray[i].numTypes * sizeof(DBDataSchema::DType));
        if(currDTypeArr == NULL) {
            printf("Error:\n");
            printf("Converter: %s\n", name.c_str());
            DBIngestor_error("Converter setFunctionParameters: No memory for DType array!\n", NULL);
        }
        
        for(int j=0; j<paramArray[i].numTypes; j++) {
            currDTypeArr[j] = paramArray[i].validTypeArray[j];
        }
        
        currParam.validTypeArray = currDTypeArr;
        currParam.numTypes = paramArray[i].numTypes;
        currParam.varNum = paramArray[i].varNum;
        
        functionParamArray.push_back(currParam);
    }
    
    //deallocate already allocated memory if needed
    if(functionValues.size() > 0) {
        for(int i=0; i<functionValues.size(); i++) {
            free(functionValues.at(i));
        }
    }
    
    //allocate memory for holding the function values
    currFuncInstanceDTypes.resize(size);
    functionValues.resize(size);
    dataObjArray.resize(size);
    
    for(int i=0; i<size; i++) {
        functionValues.at(i) = (void*)malloc(sizeof(char)*CONV_RESULT_BUFFER_SIZE);
        if(functionValues.at(i) == NULL) {
            DBIngestor_error("Allocation of converter variable buffer failed!\n");
        }
    }
}

int Converter::registerInternalParameter(int parNum, DBDataSchema::DType parType) {
    assert(parNum < functionParamArray.size());
    
    //check whether this type is accepted for this converter
    for(int i=0; i<functionParamArray.at(parNum).numTypes; i++) {
        if(parType == functionParamArray.at(parNum).validTypeArray[i]) {
            currFuncInstanceDTypes.at(parNum) = parType;
            
            return 1;
        }
    }
 
    printf("Error:\n");
    printf("Converter: %s\n", name.c_str());
    printf("Parameter num: %i\n", parNum);
    DBIngestor_error("Converter registerParameter: The DType you specified for this parameter is not supported by this converter!\n", NULL);
    
    return 0;    
}

int Converter::registerParameter(int parNum, DBDataSchema::DataObjDesc * dataObj) {
    assert(parNum < functionParamArray.size());
    assert(dataObj != NULL);
    
    dataObjArray.at(parNum) = dataObj;
    registerInternalParameter(parNum, dataObj->getDataObjDType());
    
    return 0;    
}

DBDataSchema::DataObjDesc * Converter::getParameterDatObj(int parNum) {
    assert(parNum < dataObjArray.size());
    
    return dataObjArray.at(parNum);
}

unsigned long Converter::getNumParameters() {
    return functionParamArray.size();
}

int Converter::setParameter(int parNum, void* value) {
	assert(parNum < functionValues.size());
    assert(value != NULL);
    
    functionValues.at(parNum) = value;
    
    return 1;
}

void* Converter::getParameter(int parNum) {
    assert(parNum < functionValues.size());
    
    return functionValues.at(parNum);
}

int Converter::setResult(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);

    if(result != NULL) {
        free(result);
    }

    switch (thisDType) {
        case DBDataSchema::DT_STRING:
            result = malloc(strlen(*(char**)value) + 1);
            strcpy(*(char**)result, *(char**)value);
            break;
        case DBDataSchema::DT_INT1:
            result = malloc(sizeof(int8_t));
            *(int8_t*)result = *(int8_t*)value;
            break;
        case DBDataSchema::DT_INT2:
            result = malloc(sizeof(int16_t));
            *(int16_t*)result = *(int16_t*)value;
            break;
        case DBDataSchema::DT_INT4:
            result = malloc(sizeof(int32_t));
            *(int32_t*)result = *(int32_t*)value;
            break;
        case DBDataSchema::DT_INT8:
            result = malloc(sizeof(int64_t));
            *(int64_t*)result = *(int64_t*)value;
            break;
        case DBDataSchema::DT_UINT1:
            result = malloc(sizeof(int8_t));
            *(int8_t*)result = *(int8_t*)value;
            break;
        case DBDataSchema::DT_UINT2:
            result = malloc(sizeof(int16_t));
            *(int16_t*)result = *(int16_t*)value;
            break;
        case DBDataSchema::DT_UINT4:
            result = malloc(sizeof(int32_t));
            *(int32_t*)result = *(int32_t*)value;
            break;
        case DBDataSchema::DT_UINT8:
            result = malloc(sizeof(int64_t));
            *(int64_t*)result = *(int64_t*)value;
            break;
        case DBDataSchema::DT_REAL4:
            result = malloc(sizeof(float));
            *(float*)result = *(float*)value;
            break;
        case DBDataSchema::DT_REAL8:
            result = malloc(sizeof(double));
            *(double*)result = *(double*)value;
            break;
        default:
            DBIngestor_error("Converter Error: Cannot save the result due to unknown format type\n", NULL);
            break;
    }    
    
    return 1;
}

int Converter::getResult(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);

    if(result == NULL) {
        return 0;
    }

    switch (thisDType) {
        case DBDataSchema::DT_STRING:
            value = malloc(strlen(*(char**)result) + 1);
            strcpy(*(char**)value, *(char**)result);
            break;
        case DBDataSchema::DT_INT1:
            *(int8_t*)value = *(int8_t*)result;
            break;
        case DBDataSchema::DT_INT2:
            *(int16_t*)value = *(int16_t*)result;
            break;
        case DBDataSchema::DT_INT4:
            *(int32_t*)value = *(int32_t*)result;
            break;
        case DBDataSchema::DT_INT8:
            *(int64_t*)value = *(int64_t*)result;
            break;
        case DBDataSchema::DT_UINT1:
            *(int8_t*)value = *(int8_t*)result;
            break;
        case DBDataSchema::DT_UINT2:
            *(int16_t*)value = *(int16_t*)result;
            break;
        case DBDataSchema::DT_UINT4:
            *(int32_t*)value = *(int32_t*)result;
            break;
        case DBDataSchema::DT_UINT8:
            *(int64_t*)value = *(int64_t*)result;
            break;
        case DBDataSchema::DT_REAL4:
            *(float*)value = *(float*)result;
            break;
        case DBDataSchema::DT_REAL8:
            *(double*)value = *(double*)result;
            break;
        default:
            DBIngestor_error("Converter Error: Cannot return the result due to unknown format type\n", NULL);
            break;
    }    

    return 1;
}

bool Converter::execute(DBDataSchema::DType thisDType, void* value) {
	throw "Not yet implemented";
}
