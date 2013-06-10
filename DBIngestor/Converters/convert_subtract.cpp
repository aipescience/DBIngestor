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

#include "dbingestor_error.h"
#include <assert.h>
#include "convert_subtract.h"
#include <math.h>

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_subtract::convert_subtract() {
    string converterName = "CONV_SUBTRACT";
    int sizeTypeArray = 10;
    DType typeArray[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    int numParameters = 1;
    DType funcParTypes[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    convFunctionParam parameters[1] = { { 0, 10, funcParTypes } };
    
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
    setFunctionParameters(parameters, numParameters);
}

convert_subtract::~convert_subtract() {
    
}

Converter * convert_subtract::clone() {
    return new convert_subtract;
}

bool convert_subtract::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);
    
	//apply subtract to the value
    switch (thisDType) {
        case DBDataSchema::DT_INT1:
            *(int8_t*)value -= castToInt1(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT2:
            *(int16_t*)value -= castToInt2(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT4:
            *(int32_t*)value -= castToInt4(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_INT8:
            *(int64_t*)value -= castToInt8(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL4:
            *(float*)value -= castToFloat(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            *(double*)value -= castToDouble(currFuncInstanceDTypes[0], functionValues[0]);
            return 1;
            break;
            
        default:
            DBIngestor_error("Converter Error: CONV_SUBTRACT does not handle the datatype - ONLY INTS, FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
