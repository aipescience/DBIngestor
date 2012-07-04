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
#include "assert_ispositive.h"
#include <math.h>

using namespace DBAsserter;
using namespace DBDataSchema;
using namespace std;

assert_ispositive::assert_ispositive() {
    string asserterName = "ASRT_ISPOSITIVE";
    int sizeTypeArray = 10;
    DType typeArray[10] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8};
    
    setName(asserterName);
    setDTypeArray(typeArray, sizeTypeArray);
}

assert_ispositive::~assert_ispositive() {
    
}

int assert_ispositive::setParameter(int parNum, void* value) {
	return 1;
}

void* assert_ispositive::getParameter(int parNum) {
	return 0;
}

bool assert_ispositive::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);
    
	//check if this is positive
    switch (thisDType) {
        case DBDataSchema::DT_INT1:
            if((*(int8_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;
            
        case DBDataSchema::DT_INT2:
            if((*(int16_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;
            
        case DBDataSchema::DT_INT4:
            if((*(int32_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;
            
        case DBDataSchema::DT_INT8:
            if((*(int64_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;

        case DBDataSchema::DT_UINT1:
            if((*(uint8_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;
            
        case DBDataSchema::DT_UINT2:
            if((*(uint16_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;
            
        case DBDataSchema::DT_UINT4:
            if((*(uint32_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;
            
        case DBDataSchema::DT_UINT8:
            if((*(uint64_t*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;

        case DBDataSchema::DT_REAL4:
            if((*(float*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;
            
        case DBDataSchema::DT_REAL8:
            if((*(double*)value) >= 0) {
                return 1;
            } else {
                return 0;
            }
            break;

        default:
            DBIngestor_error("Asserter Error: ASRT_ISPOSITIVE does not handle the datatype - ONLY INT1,2,4,8, FLOAT AND DOUBLE SUPPORTED\n");
            break;
    }
    
    return 0;
}
