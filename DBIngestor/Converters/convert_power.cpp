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
#include "convert_power.h"
#include <math.h>
#include <stdio.h> // sprintf
#include <stdlib.h> // malloc


using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_power::convert_power() {
    string converterName = "CONV_POWER";
    int sizeTypeArray = 10;
    DType typeArray[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    int numParameters = 1;
    DType funcParTypes[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    convFunctionParam parameters[1] = { { 0, 10, funcParTypes } };
    
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
    setFunctionParameters(parameters, numParameters);
}

convert_power::~convert_power() {
    
}

Converter * convert_power::clone() {
    return new convert_power;
}

bool convert_power::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);
    
    double exp = castToDouble(currFuncInstanceDTypes[0], functionValues[0]);   
    double val = castToDouble(thisDType,value);
    
    //fractional power of negative values is not allowed; division by 0 is not allowed (0^(-exp))
    if ( ( exp > -1.0 && 
           exp < 1.0 && 
           exp != 0.0 && 
           val < 0.0 )
       || ( exp < 0 && 
           val == 0.0 )
       )
          {
        char * str;
        str = (char*) malloc(1000 * sizeof(char));
        sprintf(str,"ERROR: Negative values not allowed in CONV_POWER when power is between -1 and 1! Offending value is %f^%lf\n", 
            val, exp);
        DBIngestor_error(str, NULL);
        return 0;
    }
    
    //apply power-function
    switch (thisDType) {
        case DBDataSchema::DT_INT1:
            *(int8_t*)value = pow(*(int8_t*)value, exp);
            return 1;
            break;
            
        case DBDataSchema::DT_INT2:
            *(int16_t*)value = pow(*(int16_t*)value, exp);
            return 1;
            break;
            
        case DBDataSchema::DT_INT4:
            *(int32_t*)value = pow(*(int32_t*)value, exp);
            return 1;
            break;
            
        case DBDataSchema::DT_INT8:
            *(int64_t*)value = pow(*(int64_t*)value, exp);
            return 1;
            break;

        case DBDataSchema::DT_UINT1:
            *(uint8_t*)value = pow(*(uint8_t*)value, exp);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT2:
            *(uint16_t*)value = pow(*(uint16_t*)value, exp);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT4:
            *(uint32_t*)value = pow(*(uint32_t*)value, exp);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT8:
            *(uint64_t*)value = pow(*(uint64_t*)value, exp);
            return 1;
            break;

        case DBDataSchema::DT_REAL4:
            *(float*)value = pow(*(float*)value, exp);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            *(double*)value = pow(*(double*)value, exp);
            return 1;
            break;
            
        default:
            DBIngestor_error("Converter Error: CONV_POWER does not handle this datatype - ONLY INTS, FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
