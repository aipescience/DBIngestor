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
#include "Converter.h"

using namespace DBConverter;
using namespace std;

#define CONV_RESULT_BUFFER_SIZE 128

Converter::Converter() {

}

Converter::~Converter() {
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

bool Converter::execute(DBDataSchema::DType thisDType, void* value) {
	throw "Not yet implemented";
}
