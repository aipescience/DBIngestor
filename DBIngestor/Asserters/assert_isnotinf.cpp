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
#include "assert_isnotinf.h"
#include <boost/math/special_functions/fpclassify.hpp>

using namespace DBAsserter;
using namespace DBDataSchema;
using namespace std;

assert_isnotinf::assert_isnotinf() {
    string asserterName = "ASRT_ISNOTINF";
    int sizeTypeArray = 2;
    DType typeArray[2] = {DT_REAL4, DT_REAL8};
    
    setName(asserterName);
    setDTypeArray(typeArray, sizeTypeArray);
}

assert_isnotinf::~assert_isnotinf() {
    
}

int assert_isnotinf::setParameter(int parNum, void* value) {
	return 1;
}

void* assert_isnotinf::getParameter(int parNum) {
	return 0;
}

bool assert_isnotinf::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);
    
	//check if this is inf
    switch (thisDType) {
        case DBDataSchema::DT_REAL4:
            if((boost::math::isinf)(*(float*)value) == 0) {
                return 1;
            } else {
                return 0;
            }
            break;

        case DBDataSchema::DT_REAL8:
            if((boost::math::isinf)(*(double*)value) == 0) {
                return 1;
            } else {
                return 0;
            }
            break;

        default:
            DBIngestor_error("Asserter Error: ASRT_ISNOTINF does not handle the datatype - ONLY FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
