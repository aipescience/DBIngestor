/*  
 *  Copyright (c) 2012, Adrian M. Partl <apartl@aip.de>, 
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
#include "convert_postset.h"
#include <math.h>
#include <stdio.h> // sprintf
#include <stdlib.h> // malloc

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_postset::convert_postset() {
    string converterName = "CONV_POSTSET";
    int sizeTypeArray = 11;
    DType typeArray[11] = {DT_STRING, DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    int numParameters = 1;
    DType funcParTypes[11] = {DT_STRING, DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    convFunctionParam parameters[1] = { { 0, 11, funcParTypes } };
   
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
    setFunctionParameters(parameters, numParameters);

    valueCopy = NULL;
}

convert_postset::~convert_postset() {
    if(valueCopy != NULL) {
        free(valueCopy);
    }
}

Converter * convert_postset::clone() {
    return new convert_postset;
}

bool convert_postset::execute(DBDataSchema::DType thisDType, void* value) {

    assert(value != NULL);
    
    switch (thisDType) {
        case DBDataSchema::DT_STRING:
            char * buffer1 = castToString(currFuncInstanceDTypes[0], functionValues[0]);

            if(valueCopy != NULL) {
                *(char**)value = *(char**)valueCopy;
            } else {
                *(char**)value = buffer1;
            }
            
            if(*(char**)valueCopy != NULL) {
                free(*(char**)valueCopy);
            }
            
            *(char**)valueCopy = buffer1;
            return 1;
            break;
        case DBDataSchema::DT_INT1:
            if(valueCopy != NULL) {
                *(int8_t*)value = *(int8_t*)valueCopy;
            } else {
                *(int8_t*)value = castToInt1(currFuncInstanceDTypes[0], functionValues[0]);
                *(int8_t*)valueCopy = (int8_t*)malloc(sizeof(int8_t));
            }
            
            *(int8_t*)valueCopy = castToInt1(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT2:
            if(valueCopy != NULL) {
                *(int16_t*)value = *(int16_t*)valueCopy;
            } else {
                *(int16_t*)value = castToInt2(currFuncInstanceDTypes[0], functionValues[0]);
                *(int16_t*)valueCopy = (int16_t*)malloc(sizeof(int16_t));
            }
            
            *(int16_t*)valueCopy = castToInt2(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT4:
            if(valueCopy != NULL) {
                *(int32_t*)value = *(int32_t*)valueCopy;
            } else {
                *(int32_t*)value = castToInt4(currFuncInstanceDTypes[0], functionValues[0]);
                *(int32_t*)valueCopy = (int32_t*)malloc(sizeof(int32_t));
            }
            
            *(int32_t*)valueCopy = castToInt4(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT8:
            if(valueCopy != NULL) {
                *(int64_t*)value = *(int64_t*)valueCopy;
            } else {
                *(int64_t*)value = castToInt8(currFuncInstanceDTypes[0], functionValues[0]);
                *(int64_t*)valueCopy = (int64_t*)malloc(sizeof(int64_t));
            }
            
            *(int64_t*)valueCopy = castToInt8(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT1:
            if(valueCopy != NULL) {
                *(int8_t*)value = *(int8_t*)valueCopy;
            } else {
                *(int8_t*)value = castToUInt1(currFuncInstanceDTypes[0], functionValues[0]);
                *(int8_t*)valueCopy = (int8_t*)malloc(sizeof(int8_t));
            }
            
            *(int8_t*)valueCopy = castToUInt1(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT2:
            if(valueCopy != NULL) {
                *(int16_t*)value = *(int16_t*)valueCopy;
            } else {
                *(int16_t*)value = castToUInt2(currFuncInstanceDTypes[0], functionValues[0]);
                *(int16_t*)valueCopy = (int16_t*)malloc(sizeof(int16_t));
            }
            
            *(int16_t*)valueCopy = castToUInt2(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT4:
            if(valueCopy != NULL) {
                *(int32_t*)value = *(int32_t*)valueCopy;
            } else {
                *(int32_t*)value = castToUInt4(currFuncInstanceDTypes[0], functionValues[0]);
                *(int32_t*)valueCopy = (int32_t*)malloc(sizeof(int32_t));
            }
            
            *(int32_t*)valueCopy = castToUInt4(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT8:
            if(valueCopy != NULL) {
                *(int64_t*)value = *(int64_t*)valueCopy;
            } else {
                *(int64_t*)value = castToUInt8(currFuncInstanceDTypes[0], functionValues[0]);
                *(int64_t*)valueCopy = (int64_t*)malloc(sizeof(int64_t));
            }
            
            *(int64_t*)valueCopy = castToUInt8(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL4:
            if(valueCopy != NULL) {
                *(float*)value = *(float*)valueCopy;
            } else {
                *(float*)value = castToFloat(currFuncInstanceDTypes[0], functionValues[0]);
                *(float*)valueCopy = (float*)malloc(sizeof(float));
            }
            
            *(float*)valueCopy = castToFloat(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            if(valueCopy != NULL) {
                *(double*)value = *(double*)valueCopy;
            } else {
                *(double*)value = castToDouble(currFuncInstanceDTypes[0], functionValues[0]);
                *(double*)valueCopy = (double*)malloc(sizeof(double));
            }
            
            *(double*)valueCopy = castToDouble(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;            
        default:
            DBIngestor_error("Converter Error: CONV_POSTSET does not handle this datatype - ONLY STRING, INTS, FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
