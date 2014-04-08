/*  
 *  Copyright (c) 2012 - 2014, Adrian M. Partl <apartl@aip.de>, 
 *			            Kristin Riebe <kriebe@aip.de>
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

#include "dbingestor_error.h"
#include <assert.h>
#include "convert_set.h"
#include <math.h>
#include <stdio.h> // sprintf
#include <stdlib.h> // malloc

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_set::convert_set() {
    string converterName = "CONV_SET";
    int sizeTypeArray = 11;
    DType typeArray[11] = {DT_STRING, DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    int numParameters = 1;
    DType funcParTypes[11] = {DT_STRING, DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    convFunctionParam parameters[1] = { { 0, 11, funcParTypes } };
   
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
    setFunctionParameters(parameters, numParameters);
}

convert_set::~convert_set() {
}

Converter * convert_set::clone() {
    return new convert_set;
}

bool convert_set::execute(DBDataSchema::DType thisDType, void* value) {

    assert(value != NULL);
    
    //apply set to the function value
    char * buffer1 = NULL;
    char * returnBuffer = NULL;
    switch (thisDType) {
        case DBDataSchema::DT_STRING:
            buffer1 = castToString(currFuncInstanceDTypes[0], functionValues[0]);
            returnBuffer = (char*) malloc(sizeof(buffer1) + 1);
            if(returnBuffer == NULL)
                DBIngestor_error("Converter Error: Could not allocate memory in CONV_SET\n", NULL);

            strcpy((char*)returnBuffer, buffer1);
            *(char**)value = returnBuffer;
            free(buffer1);
            return 1;
            break;
        case DBDataSchema::DT_INT1:
            *(int8_t*)value = castToInt1(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT2:
            *(int16_t*)value = castToInt2(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT4:
            *(int32_t*)value = castToInt4(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT8:
            *(int64_t*)value = castToInt8(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT1:
            *(int8_t*)value = castToUInt1(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT2:
            *(int16_t*)value = castToUInt2(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT4:
            *(int32_t*)value = castToUInt4(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT8:
            *(int64_t*)value = castToUInt8(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL4:
            *(float*)value = castToFloat(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            *(double*)value = castToDouble(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;            
        default:
            DBIngestor_error("Converter Error: CONV_SET does not handle this datatype - ONLY STRING, INTS, FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
