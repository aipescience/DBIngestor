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
#include "convert_floor.h"
#include <math.h>

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_floor::convert_floor() {
    string converterName = "CONV_FLOOR";
    int sizeTypeArray = 2;
    DType typeArray[2] = {DT_REAL4, DT_REAL8};
    
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
}

convert_floor::~convert_floor() {
    
}

Converter * convert_floor::clone() {
    return new convert_floor;
}

bool convert_floor::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);
    
	//apply floor to the value
    switch (thisDType) {
        case DBDataSchema::DT_REAL4:
            *(float*)value = floorf(*(float*)value);
            return 1;
            break;
            
        case DBDataSchema::DT_REAL8:
            *(double*)value = floor(*(double*)value);
            return 1;
            break;

        default:
            DBIngestor_error("Converter Error: CONV_FLOOR does not handle the datatype - ONLY FLOAT AND DOUBLE SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
