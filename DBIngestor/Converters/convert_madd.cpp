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

#include "dbingestor_error.h"
#include <assert.h>
#include "convert_madd.h"
#include <math.h>

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_madd::convert_madd() {
    string converterName = "CONV_MADD";
    int sizeTypeArray = 10;
    DType typeArray[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    DType funcParTypes[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    int numParameters = 2;
    convFunctionParam parameters[2] = { { 0, 10, funcParTypes }, { 1, 10, funcParTypes } };
    
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
    setFunctionParameters(parameters, numParameters);
}

convert_madd::~convert_madd() {
    
}

Converter * convert_madd::clone() {
    return new convert_madd;
}

bool convert_madd::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);
    
	//apply add to the value
    switch (thisDType) {
        case DBDataSchema::DT_INT1:
            *(int8_t*)value = *(int8_t*)value * castToInt1(currFuncInstanceDTypes[0], functionValues[0]) + castToInt1(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT2:
            *(int16_t*)value = *(int16_t*)value * castToInt2(currFuncInstanceDTypes[0], functionValues[0]) + castToInt2(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT4:
            *(int32_t*)value = *(int32_t*)value * castToInt4(currFuncInstanceDTypes[0], functionValues[0]) + castToInt4(currFuncInstanceDTypes[1], functionValues[1]);            return 1;
            break;
            
        case DBDataSchema::DT_INT8:
            *(int64_t*)value = *(int64_t*)value * castToInt8(currFuncInstanceDTypes[0], functionValues[0]) + castToInt8(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;

        case DBDataSchema::DT_UINT1:
            *(int8_t*)value = *(uint8_t*)value * castToUInt1(currFuncInstanceDTypes[0], functionValues[0]) + castToUInt1(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT2:
            *(int16_t*)value = *(uint16_t*)value * castToUInt2(currFuncInstanceDTypes[0], functionValues[0]) + castToUInt2(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT4:
            *(int32_t*)value = *(uint32_t*)value * castToUInt4(currFuncInstanceDTypes[0], functionValues[0]) + castToUInt4(currFuncInstanceDTypes[1], functionValues[1]);            return 1;
            break;
            
        case DBDataSchema::DT_UINT8:
            *(int64_t*)value = *(uint64_t*)value * castToUInt8(currFuncInstanceDTypes[0], functionValues[0]) + castToUInt8(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;

        case DBDataSchema::DT_REAL4:
            *(float*)value = *(float*)value * castToFloat(currFuncInstanceDTypes[0], functionValues[0]) + castToFloat(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            *(double*)value = *(double*)value * castToDouble(currFuncInstanceDTypes[0], functionValues[0]) + castToDouble(currFuncInstanceDTypes[1], functionValues[1]);
            return 1;
            break;

        default:
            DBIngestor_error("Converter Error: CONV_MADD does not handle the datatype - ONLY INTS, FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
