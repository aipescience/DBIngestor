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

#include "DType.h"
#include "dbingestor_error.h"
#include <stdio.h>
#include <stdexcept>
#include <boost/algorithm/string.hpp>
#ifndef _WIN32
#include <math.h>
#include <stdint.h>
#else
#include <math.h>
#include <float.h>
#define isnan(x) _isnan(x)
#define isinf(x) (!_finite(x))
#include "stdint_win.h"
#endif

using namespace DBDataSchema;
using namespace std;

///////////////////////////////////////////////
////// PRIVATE FUNCTION FORWARD DECLARATION ///
///////////////////////////////////////////////

int convToInt1(std::string & thisString, void* result);
int convToInt2(std::string & thisString, void* result);
int convToInt4(std::string & thisString, void* result);
int convToInt8(std::string & thisString, void* result);
int convToUInt1(std::string & thisString, void* result);
int convToUInt2(std::string & thisString, void* result);
int convToUInt4(std::string & thisString, void* result);
int convToUInt8(std::string & thisString, void* result);
int convToReal4(std::string & thisString, void* result);
int convToReal8(std::string & thisString, void* result);

///////////////////////////////////////////////


int DBDataSchema::testDType(string thisDType) {
    boost::to_upper(thisDType);
    
    if(thisDType == "CHAR") {
            return 1;
    }
        
    if(thisDType == "INT1") {
        return 1;
    }
    
    if(thisDType == "INT2") {
        return 1;
    }

    if(thisDType == "INT4") {
        return 1;
    }

    if(thisDType == "INT8") {
        return 1;
    }

    if(thisDType == "UINT1") {
        return 1;
    }
    
    if(thisDType == "UINT2") {
        return 1;
    }
    
    if(thisDType == "UINT4") {
        return 1;
    }
    
    if(thisDType == "UINT8") {
        return 1;
    }

    if(thisDType == "REAL4") {
        return 1;
    }

    if(thisDType == "REAL8") {
        return 1;
    }

    return 0;    
}

DType DBDataSchema::convDType(std::string thisDType) {
    boost::to_upper(thisDType);
    
    if(thisDType == "CHAR") {
        return DT_STRING;
    }
    
    if(thisDType == "INT1") {
        return DT_INT1;
    }
    
    if(thisDType == "INT2") {
        return DT_INT2;
    }
    
    if(thisDType == "INT4") {
        return DT_INT4;
    }
    
    if(thisDType == "INT8") {
        return DT_INT8;
    }

    if(thisDType == "UINT1") {
        return DT_UINT1;
    }
    
    if(thisDType == "UINT2") {
        return DT_UINT2;
    }
    
    if(thisDType == "UINT4") {
        return DT_UINT4;
    }
    
    if(thisDType == "UINT8") {
        return DT_UINT8;
    }

    if(thisDType == "REAL4") {
        return DT_REAL4;
    }
    
    if(thisDType == "REAL8") {
        return DT_REAL8;
    }
    
    DBIngestor_error("DType convDType: Type not known...");
    return (DType)0;    
}

int DBDataSchema::castStringToDType(std::string & thisString, DType thisType, void* result) {
    int isNull = 0;
    
    switch (thisType) {
        case DT_STRING: {
            //since we are only working with pointers to strings internaly, allocate memory, copy string and hope this
            //gets freed in the IngestBuffer....
            char * tmpStr = (char*)malloc(thisString.size() * sizeof(char));
            strcpy(tmpStr, thisString.c_str());
            *(char**)result = tmpStr;}
            break;
        case DT_INT1:
            isNull = convToInt1(thisString, result);
            break;
        case DT_INT2:
            isNull = convToInt2(thisString, result);
            break;
        case DT_INT4:
            isNull = convToInt4(thisString, result);
            break;
        case DT_INT8:
            isNull = convToInt8(thisString, result);
            break;
        case DT_UINT1:
            isNull = convToUInt1(thisString, result);
            break;
        case DT_UINT2:
            isNull = convToUInt2(thisString, result);
            break;
        case DT_UINT4:
            isNull = convToUInt4(thisString, result);
            break;
        case DT_UINT8:
            isNull = convToUInt8(thisString, result);
            break;
        case DT_REAL4:
            isNull = convToReal4(thisString, result);
            break;
        case DT_REAL8:
            isNull = convToReal8(thisString, result);
            break;
        default:
            DBIngestor_error("castStringToDType: DType not known, I don't know what to do.");
            break;
    }
    
    return isNull;
}

void DBDataSchema::printThisDType(void* var, DType thisType) {
    printf("DType - printThisDType: ");
    
    switch (thisType) {
        case DT_STRING:
            printf("%s\n", (char*)var);
            break;
        case DT_INT1:
            printf("%d\n", *(int8_t*)var);
            break;
        case DT_INT2:
            printf("%hd\n", *(int16_t*)var);
            break;
        case DT_INT4:
            printf("%d\n", *(int32_t*)var);
            break;
        case DT_INT8:
            printf("%lld\n", *(int64_t*)var);
            break;
        case DT_UINT1:
            printf("%u\n", *(uint8_t*)var);
            break;
        case DT_UINT2:
            printf("%hu\n", *(uint16_t*)var);
            break;
        case DT_UINT4:
            printf("%u\n", *(uint32_t*)var);
            break;
        case DT_UINT8:
            printf("%llu\n", *(uint64_t*)var);
            break;
        case DT_REAL4:
            printf("%f\n", *(float*)var);
            break;
        case DT_REAL8:
            printf("%lf\n", *(double*)var);
            break;
        default:
            printf("Unknown type\n");
            break;
    }
}

int DBDataSchema::getByteLenOfDType(DType thisType) {
    switch (thisType) {
        case DT_STRING:
            return sizeof(char);
        case DT_INT1:
            return sizeof(int8_t);
        case DT_INT2:
            return sizeof(int16_t);
        case DT_INT4:
            return sizeof(int32_t);
        case DT_INT8:
            return sizeof(int64_t);
        case DT_UINT1:
            return sizeof(uint8_t);
        case DT_UINT2:
            return sizeof(uint16_t);
        case DT_UINT4:
            return sizeof(uint32_t);
        case DT_UINT8:
            return sizeof(uint64_t);
        case DT_REAL4:
            return sizeof(float);
        case DT_REAL8:
            return sizeof(double);
        default:
            return 0;
    }
}

int8_t DBDataSchema::castToInt1(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (int8_t)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (int8_t)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (int8_t)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (int8_t)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (int8_t)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (int8_t)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (int8_t)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (int8_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (int8_t)tmp;}
        default:
            return 0;
    }
}

int16_t DBDataSchema::castToInt2(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (int16_t)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (int16_t)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (int16_t)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (int16_t)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (int16_t)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (int16_t)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (int16_t)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (int16_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (int16_t)tmp;}
        default:
            return 0;
    }
}


int32_t DBDataSchema::castToInt4(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (int32_t)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (int32_t)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (int32_t)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (int32_t)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (int32_t)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (int32_t)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (int32_t)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (int32_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (int32_t)tmp;}
        default:
            return 0;
    }
}


int64_t DBDataSchema::castToInt8(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (int64_t)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (int64_t)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (int64_t)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (int64_t)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (int64_t)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (int64_t)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (int64_t)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (int64_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (int64_t)tmp;}
        default:
            return 0;
    }
}

int8_t DBDataSchema::castToUInt1(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (uint8_t)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (uint8_t)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (uint8_t)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (uint8_t)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (uint8_t)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (uint8_t)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (uint8_t)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (uint8_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (uint8_t)tmp;}
        default:
            return 0;
    }
}

int16_t DBDataSchema::castToUInt2(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (uint16_t)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (uint16_t)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (uint16_t)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (uint16_t)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (uint16_t)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (uint16_t)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (uint16_t)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (uint16_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (uint16_t)tmp;}
        default:
            return 0;
    }
}


int32_t DBDataSchema::castToUInt4(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (uint32_t)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (uint32_t)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (uint32_t)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (uint32_t)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (uint32_t)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (uint32_t)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (uint32_t)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (uint32_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (uint32_t)tmp;}
        default:
            return 0;
    }
}


int64_t DBDataSchema::castToUInt8(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (uint64_t)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (uint64_t)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (uint64_t)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (uint64_t)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (uint64_t)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (uint64_t)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (uint64_t)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (uint64_t)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (uint64_t)tmp;}
        default:
            return 0;
    }
}

float DBDataSchema::castToFloat(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (float)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (float)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (float)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (float)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (float)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (float)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (float)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (float)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return (float)tmp;}
        default:
            return 0;
    }
}

double DBDataSchema::castToDouble(DType thisType, void* value) {
    switch (thisType) {
        case DT_INT1: {
            int8_t tmp = *(int8_t*) value;
            return (double)tmp;}
        case DT_INT2: {
            int16_t tmp = *(int16_t*) value;
            return (double)tmp;}
        case DT_INT4: {
            int32_t tmp = *(int32_t*) value;
            return (double)tmp;}
        case DT_INT8: {
            int64_t tmp = *(int64_t*) value;
            return (double)tmp;}
        case DT_UINT1: {
            uint8_t tmp = *(uint8_t*) value;
            return (double)tmp;}
        case DT_UINT2: {
            uint16_t tmp = *(uint16_t*) value;
            return (double)tmp;}
        case DT_UINT4: {
            uint32_t tmp = *(uint32_t*) value;
            return (double)tmp;}
        case DT_UINT8: {
            uint64_t tmp = *(uint64_t*) value;
            return (double)tmp;}
        case DT_REAL4: {
            float tmp = *(float*) value;
            return (double)tmp;}
        case DT_REAL8: {
            double tmp = *(double*) value;
            return tmp;}
        default:
            return 0;
    }
}

int convToInt1(std::string & thisString, void* result) {
    int32_t tmp;
    tmp = strtol(thisString.c_str(), NULL, 0);
    
    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(tmp == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    *(int8_t*)result = (int8_t)tmp;
    
    return 0;
}

int convToInt2(std::string & thisString, void* result) {
    int32_t tmp;
    tmp = strtol(thisString.c_str(), NULL, 0);
    
    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(tmp == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    *(int16_t*)result = (int16_t)tmp;
    
    return 0;
}

int convToInt4(std::string & thisString, void* result) {
    *(int32_t*)result = strtol(thisString.c_str(), NULL, 0);

    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(*(int32_t*)result == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    return 0;
}

int convToInt8(std::string & thisString, void* result) {
#ifdef _WIN32
    *(int64_t*)result = _strtoi64(thisString.c_str(), NULL, 0);
#else
    *(int64_t*)result = strtoll(thisString.c_str(), NULL, 0);
#endif

    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(*(int64_t*)result == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    return 0;
}

int convToUInt1(std::string & thisString, void* result) {
    int32_t tmp;
    tmp = strtol(thisString.c_str(), NULL, 0);

    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(tmp == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    *(uint8_t*)result = (uint8_t)tmp;

    return 0;
}

int convToUInt2(std::string & thisString, void* result) {
    int32_t tmp;
    tmp = strtol(thisString.c_str(), NULL, 0);
    
    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(tmp == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    *(uint16_t*)result = (uint16_t)tmp;
    
    return 0;
}

int convToUInt4(std::string & thisString, void* result) {
    *(uint32_t*)result = strtoul(thisString.c_str(), NULL, 0);

    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(*(uint32_t*)result == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    return 0;
}

int convToUInt8(std::string & thisString, void* result) {
#ifdef _WIN32
    *(uint64_t*)result = _strtoui64(thisString.c_str(), NULL, 0);
#else
    *(uint64_t*)result = strtoull(thisString.c_str(), NULL, 0);
#endif

    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(*(uint64_t*)result == 0) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    return 0;
}

int convToReal4(std::string & thisString, void* result) {
#ifdef _WIN32
    double tmp = strtod(thisString.c_str(), NULL);
    *(float*)result = (float)tmp;
#else
    *(float*)result = strtof(thisString.c_str(), NULL);
#endif

    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(*(float*)result == 0.0 || isnan(*(float*)result) || isinf(*(float*)result)) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    return 0;
}

int convToReal8(std::string & thisString, void* result) {
    *(double*)result = strtod(thisString.c_str(), NULL);

    //check if this could be a candiate for a NULL
    //strol returns 0 if there was a character in there 
    if(*(double*)result == 0.0 || isnan(*(double*)result) || isinf(*(double*)result)) {
        if(thisString.find_first_not_of("0123456789.xXeE+-") != string::npos) {
            return 1; 
        }
    }
    
    return 0;
}

