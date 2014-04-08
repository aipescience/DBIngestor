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
#include "convert_sqrt.h"
#include <math.h>
#include <stdio.h> // sprintf
#include <stdlib.h> // malloc

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_sqrt::convert_sqrt() {
    string converterName = "CONV_SQRT";
    int sizeTypeArray = 10;
    DType typeArray[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
}

convert_sqrt::~convert_sqrt() {
}

Converter * convert_sqrt::clone() {
    return new convert_sqrt;
}

//TODO: maybe return error/warning, if an argument to CONV_SQRT is given?? (at the moment, it is just ignored)
bool convert_sqrt::execute(DBDataSchema::DType thisDType, void* value) {

    assert(value != NULL);
    
    double val = castToDouble(thisDType,value);
    if (val < 0.0) {
        char * str;
        str = (char*) malloc(1000 * sizeof(char));
        sprintf(str,"ERROR: Negative values not allowed in CONV_SQRT! Offending value is %f\n", 
            val);
        DBIngestor_error(str, NULL);
        return 0;
    }
    
    //apply sqrt to the function value
    switch (thisDType) {
        case DBDataSchema::DT_INT1:
            *(int8_t*)value = sqrt(*(int8_t*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_INT2:
            *(int16_t*)value = sqrt(*(int16_t*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_INT4:
            *(int32_t*)value = sqrt(*(int32_t*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_INT8:
            *(int64_t*)value = sqrt(*(int64_t*)value);
            return 1;
            break;

        case DBDataSchema::DT_UINT1:
            *(uint8_t*)value = sqrt(*(uint8_t*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT2:
            *(uint16_t*)value = sqrt(*(uint16_t*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT4:
            *(uint32_t*)value = sqrt(*(uint32_t*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_UINT8:
            *(uint64_t*)value = sqrt(*(uint64_t*)value);
            return 1;
            break;

        case DBDataSchema::DT_REAL4:
            *(float*)value = sqrt(*(float*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            *(double*)value = sqrt(*(double*)value);
            return 1;
            break;
            
        default:
            DBIngestor_error("Converter Error: CONV_SQRT does not handle this datatype - ONLY INTS, FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
