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
#include "convert_periodicBound.h"
#include <math.h>

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

#define PERIODICLEFT(X,BOX) ( (X) < 0 ? ( (X) + ( -1 * (int64_t)( (X) / (BOX) ) + 1 ) * (BOX) ) : (X) )
#define PERIODICRIGHT(X,BOX) ( (X) > (BOX) ? ( (X) - ( (int64_t)( (X) / (BOX) ) ) * (BOX) ) : (X) )

convert_periodicbound::convert_periodicbound() {
    string converterName = "CONV_PERIODICBOUND";
    int sizeTypeArray = 10;
    DType typeArray[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    int numParameters = 1;
    DType funcParTypes[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    convFunctionParam parameters[1] = { { 0, 10, funcParTypes } };
    
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
    setFunctionParameters(parameters, numParameters);
}

convert_periodicbound::~convert_periodicbound() {
    
}

Converter * convert_periodicbound::clone() {
    return new convert_periodicbound;
}

bool convert_periodicbound::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);
    
    // use box as double value
    double box = castToDouble(currFuncInstanceDTypes[0], functionValues[0]);

    //apply periodic boundary conditions
    switch (thisDType) {
        case DBDataSchema::DT_INT1:
            *(int8_t*)value = PERIODICLEFT( (*(int8_t*)value), box );
            *(int8_t*)value = PERIODICRIGHT( (*(int8_t*)value), box );
            return 1;
            break;
            
        case DBDataSchema::DT_INT2:
            *(int16_t*)value = PERIODICLEFT( (*(int16_t*)value), box );
            *(int16_t*)value = PERIODICRIGHT( (*(int16_t*)value), box );
            return 1;
            break;
            
        case DBDataSchema::DT_INT4:
            *(int32_t*)value = PERIODICLEFT( (*(int32_t*)value), box );
            *(int32_t*)value = PERIODICRIGHT( (*(int32_t*)value), box );
            return 1;
            break;
            
        case DBDataSchema::DT_INT8:
            *(int64_t*)value = PERIODICLEFT( (*(int64_t*)value), box );
            *(int64_t*)value = PERIODICRIGHT( (*(int64_t*)value), box );
            return 1;
            break;

        case DBDataSchema::DT_UINT1:
            *(uint8_t*)value = PERIODICLEFT( (*(uint8_t*)value), box );
            *(uint8_t*)value = PERIODICRIGHT( (*(uint8_t*)value), box );
            return 1;
            break;
            
        case DBDataSchema::DT_UINT2:
            *(uint16_t*)value = PERIODICLEFT( (*(uint16_t*)value), box );
            *(uint16_t*)value = PERIODICRIGHT( (*(uint16_t*)value), box );
            return 1;
            break;
            
        case DBDataSchema::DT_UINT4:
            *(uint32_t*)value = PERIODICLEFT( (*(uint32_t*)value), box );
            *(uint32_t*)value = PERIODICRIGHT( (*(uint32_t*)value), box );
            return 1;
            break;
            
        case DBDataSchema::DT_UINT8:
            *(uint64_t*)value = PERIODICLEFT( (*(uint64_t*)value), box );
            *(uint64_t*)value = PERIODICRIGHT( (*(uint64_t*)value), box );
            return 1;
            break;

        case DBDataSchema::DT_REAL4:
            *(float*)value = PERIODICLEFT( (*(float*)value), box );
            *(float*)value = PERIODICRIGHT( (*(float*)value), box );
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            *(double*)value = PERIODICLEFT( (*(double*)value), box );
            *(double*)value = PERIODICRIGHT( (*(double*)value), box );
            return 1;
            break;
            
        default:
            DBIngestor_error("Converter Error: CONV_PERIODICBOUND does not handle this datatype - ONLY INTS, FLOAT AND DOUBLE SUPPORTED\n", NULL);
	    break;
    }
    
    return 0;
}
