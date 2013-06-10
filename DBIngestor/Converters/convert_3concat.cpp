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
#include "convert_3concat.h"
#include <math.h>
#include <string.h>
#include <stdlib.h>
 #include <stdio.h>

using namespace DBConverter;
using namespace DBDataSchema;
using namespace std;

convert_3concat::convert_3concat() {
    string converterName = "CONV_3CONCAT";
    int sizeTypeArray = 11;
    DType typeArray[1] = {DT_STRING};
    DType funcParTypes[11] = {DT_INT1, DT_INT2, DT_INT4, DT_INT8, DT_UINT1, DT_UINT2, DT_UINT4, DT_UINT8, DT_REAL4, DT_REAL8, DT_STRING};
    int numParameters = 3;
    convFunctionParam parameters[3] = { { 0, 11, funcParTypes }, { 1, 11, funcParTypes }, { 2, 11, funcParTypes } };
    
    setName(converterName);
    setDTypeArray(typeArray, sizeTypeArray);
    setFunctionParameters(parameters, numParameters);
}

convert_3concat::~convert_3concat() {
    
}

Converter * convert_3concat::clone() {
    return new convert_3concat;
}

bool convert_3concat::execute(DBDataSchema::DType thisDType, void* value) {
    assert(value != NULL);

	//apply add to the value
    switch (thisDType) {
        case DBDataSchema::DT_STRING: {
            //allocate memory to hold result string
            char * buffer1 = castToString(currFuncInstanceDTypes[0], functionValues[0]);
            char * buffer2 = castToString(currFuncInstanceDTypes[1], functionValues[1]);
            char * buffer3 = castToString(currFuncInstanceDTypes[2], functionValues[2]);
            char * returnBuffer = (char*) malloc(strlen(buffer1) + strlen(buffer2) + strlen(buffer3) + 1);
            if(returnBuffer == NULL)
                DBIngestor_error("Converter Error: Could not allocate memory in CONV_3CONCAT\n", NULL);

            strncpy((char*)returnBuffer, buffer1, strlen(buffer1));
            strncpy((char*)(returnBuffer + strlen(buffer1)), buffer2, strlen(buffer2));
            strncpy((char*)(returnBuffer + strlen(buffer1) + strlen(buffer2)), buffer3, strlen(buffer3));
            returnBuffer[strlen(buffer1) + strlen(buffer2) + strlen(buffer3)] = '\0';
            *(char**)value = returnBuffer;

            free(buffer1);
            free(buffer2);
            free(buffer3);
            return 1;}
            break;
        default:
            DBIngestor_error("Converter Error: CONV_3CONCAT does not handle the datatype - ONLY STRINGS SUPPORTED\n", NULL);
            break;
    }
    
    return 0;
}
