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

#include "DBType.h"
#include "dbingestor_error.h"
#include <stdio.h>
#include <stdexcept>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#ifndef _WIN32
#include <stdint.h>
#else
#include "stdint_win.h"
#endif

using namespace DBDataSchema;
using namespace std;

///////////////////////////////////////////////
////// PRIVATE FUNCTION FORWARD DECLARATION ///
///////////////////////////////////////////////

void convToChar(void* value, DType thisDType, void* result);
void convToBit(void* value, DType thisDType, void* result);
void convToBigint(void* value, DType thisDType, void* result);
void convToInt(void* value, DType thisDType, void* result);
void convToSmallint(void* value, DType thisDType, void* result);
void convToTinyint(void* value, DType thisDType, void* result);
void convToUBigint(void* value, DType thisDType, void* result);
void convToUInt(void* value, DType thisDType, void* result);
void convToUSmallint(void* value, DType thisDType, void* result);
void convToUTinyint(void* value, DType thisDType, void* result);
void convToFloat(void* value, DType thisDType, void* result);
void convToReal(void* value, DType thisDType, void* result);
void convToDate(void* value, DType thisDType, void* result);
void convToTime(void* value, DType thisDType, void* result);
void convToAny(void* value, DType thisDType, void* result);

///////////////////////////////////////////////

int DBDataSchema::getByteLenOfDBType(DBType thisType) {
    switch (thisType) {
        case DBT_CHAR:
            return sizeof(char*);
        case DBT_BIT:
            return sizeof(char);
        case DBT_BIGINT:
            return sizeof(int64_t);
        case DBT_MEDIUMINT:
            return sizeof(int32_t);
        case DBT_INTEGER:
            return sizeof(int32_t);
        case DBT_SMALLINT:
            return sizeof(int16_t);
        case DBT_TINYINT:
            return sizeof(int8_t);
        case DBT_FLOAT:
            return sizeof(float);
        case DBT_REAL:
            return sizeof(double);
        case DBT_DATE:
            return sizeof(void*);
        case DBT_TIME:
            return sizeof(void*);
        case DBT_ANY:
            return sizeof(void*);
        case DBT_UBIGINT:
            return sizeof(uint64_t);
        case DBT_UMEDIUMINT:
            return sizeof(uint32_t);
        case DBT_UINTEGER:
            return sizeof(uint32_t);
        case DBT_USMALLINT:
            return sizeof(uint16_t);
        case DBT_UTINYINT:
            return sizeof(uint8_t);
        case DBT_UFLOAT:
            return sizeof(float);
        case DBT_UREAL:
            return sizeof(double);
        default:
            return 0;
    }
}

DBType DBDataSchema::convDTypeToDBType(DType thisDType) {
    switch (thisDType) {
        case DT_STRING:
            return DBT_CHAR;
            break;
        case DT_INT1:
            return DBT_TINYINT;
            break;
        case DT_INT2:
            return DBT_SMALLINT;
            break;
        case DT_INT4:
            return DBT_INTEGER;
            break;
        case DT_INT8:
            return DBT_BIGINT;
            break;
        case DT_UINT1:
            return DBT_UTINYINT;
            break;
        case DT_UINT2:
            return DBT_USMALLINT;
            break;
        case DT_UINT4:
            return DBT_UINTEGER;
            break;
        case DT_UINT8:
            return DBT_UBIGINT;
            break;
        case DT_REAL4:
            return DBT_FLOAT;
            break;
        case DT_REAL8:
            return DBT_REAL;
            break;
        default:
            DBIngestor_error("castStringToDType: DType not known, I don't know what to do.", NULL);
    }
    
}

void DBDataSchema::castDTypeToDBType(void * value, DType fromThisType, DBType toThisType, void* result) {
    switch (toThisType) {
        case DBT_CHAR:
            convToChar(value, fromThisType, result);
            break;
        case DBT_BIT:
            convToBit(value, fromThisType, result);
            break;
        case DBT_BIGINT:
            convToBigint(value, fromThisType, result);
            break;
        case DBT_MEDIUMINT:
            convToInt(value, fromThisType, result);
            break;
        case DBT_INTEGER:
            convToInt(value, fromThisType, result);
            break;
        case DBT_SMALLINT:
            convToSmallint(value, fromThisType, result);
            break;
        case DBT_TINYINT:
            convToTinyint(value, fromThisType, result);
            break;
        case DBT_FLOAT:
            convToFloat(value, fromThisType, result);
            break;
        case DBT_REAL:
            convToReal(value, fromThisType, result);
            break;
        case DBT_DATE:
            convToDate(value, fromThisType, result);
            break;
        case DBT_TIME:
            convToTime(value, fromThisType, result);
            break;
        case DBT_ANY:
            convToAny(value, fromThisType, result);
            break;
        case DBT_UBIGINT:
            convToUBigint(value, fromThisType, result);
            break;
        case DBT_UMEDIUMINT:
            convToUInt(value, fromThisType, result);
            break;
        case DBT_UINTEGER:
            convToUInt(value, fromThisType, result);
            break;
        case DBT_USMALLINT:
            convToUSmallint(value, fromThisType, result);
            break;
        case DBT_UTINYINT:
            convToUTinyint(value, fromThisType, result);
            break;
        case DBT_UFLOAT:
            convToFloat(value, fromThisType, result);
            break;
        case DBT_UREAL:
            convToReal(value, fromThisType, result);
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DBType not known, I don't know what to do.", NULL);
    }
}

void convToChar(void* value, DType thisDType, void* result) {
    string tmpStr;
    char * outputStr;
    
    switch (thisDType) {
        case DT_STRING: 
            outputStr = (char*)malloc((strlen(*(char**)value)+1)*sizeof(char));
            strcpy(outputStr, *(char**)value);
            *(char**) result = outputStr;
            return;
        case DT_INT1: 
            tmpStr = boost::lexical_cast<std::string>(static_cast<int>(*(int8_t*)value));
            break;
        case DT_INT2: 
            tmpStr = boost::lexical_cast<std::string>((int16_t*)value);
            break;
        case DT_INT4:
            tmpStr = boost::lexical_cast<std::string>((int32_t*)value);
            break;
        case DT_INT8:
            tmpStr = boost::lexical_cast<std::string>((int64_t*)value);
            break;
        case DT_UINT1: 
            tmpStr = boost::lexical_cast<std::string>(static_cast<int>(*(uint8_t*)value));
            break;
        case DT_UINT2: 
            tmpStr = boost::lexical_cast<std::string>((uint16_t*)value);
            break;
        case DT_UINT4:
            tmpStr = boost::lexical_cast<std::string>((uint32_t*)value);
            break;
        case DT_UINT8:
            tmpStr = boost::lexical_cast<std::string>((uint64_t*)value);
            break;
        case DT_REAL4:
            tmpStr = boost::lexical_cast<std::string>((float*)value);
            break;
        case DT_REAL8:
            tmpStr = boost::lexical_cast<std::string>((double*)value);
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }

    outputStr = (char*)malloc((tmpStr.size()+1)*sizeof(char));
    strcpy(outputStr, tmpStr.c_str());
    *(char**) result = outputStr;
}

void convToBit(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: {
            int16_t tmp;
            sscanf((char*) value, "%hd", tmp);
            *(int8_t*) result = (int8_t)tmp;}
            break;
        case DT_INT1: 
            *(int8_t*) result = *(int8_t*)value;
            break;
        case DT_INT2: 
            *(int8_t*) result = (int8_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(int8_t*) result = (int8_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(int8_t*) result = (int8_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(int8_t*) result = (int8_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(int8_t*) result = (int8_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(int8_t*) result = (int8_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(int8_t*) result = (int8_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(int8_t*) result = (int8_t)*(float*)value;
            break;
        case DT_REAL8:
            *(int8_t*) result = (int8_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
    
    if(*(int8_t*) result != 0) {
        *(int8_t*) result = 1;
    }
}

void convToTinyint(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: {
            int16_t tmp;
            sscanf((char*) value, "%hd", tmp);
            *(int8_t*) result = (int8_t)tmp;}
            break;
        case DT_INT1: 
            *(int8_t*) result = *(int8_t*)value;
            break;
        case DT_INT2: 
            *(int8_t*) result = (int8_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(int8_t*) result = (int8_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(int8_t*) result = (int8_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(int8_t*) result = (int8_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(int8_t*) result = (int8_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(int8_t*) result = (int8_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(int8_t*) result = (int8_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(int8_t*) result = (int8_t)*(float*)value;
            break;
        case DT_REAL8:
            *(int8_t*) result = (int8_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToBigint(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: {
#ifdef _WIN32
            *(int64_t*)result = _strtoi64((char*)value, NULL, 10);
#else
            *(int64_t*)result = strtoll((char*)value, NULL, 10);
#endif
            break;}
        case DT_INT1: 
            *(int64_t*) result = (int64_t)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(int64_t*) result = (int64_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(int64_t*) result = (int64_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(int64_t*) result = *(int64_t*)value;
            break;
        case DT_UINT1: 
            *(int64_t*) result = (int64_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(int64_t*) result = (int64_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(int64_t*) result = (int64_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(int64_t*) result = (int64_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(int64_t*) result = (int64_t)*(float*)value;
            break;
        case DT_REAL8:
            *(int64_t*) result = (int64_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToInt(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: 
            *(int32_t*)result = strtol((char*) value, NULL, 10);
            break;
        case DT_INT1: 
            *(int32_t*) result = (int32_t)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(int32_t*) result = (int32_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(int32_t*) result = *(int32_t*)value;
            break;
        case DT_INT8:
            *(int32_t*) result = (int32_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(int32_t*) result = (int32_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(int32_t*) result = (int32_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(int32_t*) result = (int32_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(int32_t*) result = (int32_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(int32_t*) result = (int32_t)*(float*)value;
            break;
        case DT_REAL8:
            *(int32_t*) result = (int32_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}
        
void convToSmallint(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: 
            sscanf((char*)value, "%hd", result);
            break;
        case DT_INT1: 
            *(int16_t*) result = (int16_t)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(int16_t*) result = *(int16_t*)value;
            break;
        case DT_INT4:
            *(int16_t*) result = (int16_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(int16_t*) result = (int16_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(int16_t*) result = (int16_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(int16_t*) result = (int16_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(int16_t*) result = (int16_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(int16_t*) result = (int16_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(int16_t*) result = (int16_t)*(float*)value;
            break;
        case DT_REAL8:
            *(int16_t*) result = (int16_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToUTinyint(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: {
            int16_t tmp;
            sscanf((char*) value, "%hd", tmp);
            *(uint8_t*) result = (uint8_t)tmp;}
            break;
        case DT_INT1: 
            *(uint8_t*) result = (uint8_t)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(uint8_t*) result = (uint8_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(uint8_t*) result = (uint8_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(uint8_t*) result = (uint8_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(uint8_t*) result = *(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(uint8_t*) result = (uint8_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(uint8_t*) result = (uint8_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(uint8_t*) result = (uint8_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(uint8_t*) result = (uint8_t)*(float*)value;
            break;
        case DT_REAL8:
            *(uint8_t*) result = (uint8_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToUBigint(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING:
#ifdef _WIN32
            *(uint64_t*)result = _strtoui64((char*)value, NULL, 10);
#else
            *(uint64_t*)result = strtoull((char*)value, NULL, 10);
#endif
            break;
        case DT_INT1: 
            *(uint64_t*) result = (uint64_t)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(uint64_t*) result = (uint64_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(uint64_t*) result = (uint64_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(uint64_t*) result = (uint64_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(uint64_t*) result = (uint64_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(uint64_t*) result = (uint64_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(uint64_t*) result = (uint64_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(uint64_t*) result = *(uint64_t*)value;
            break;
        case DT_REAL4:
            *(uint64_t*) result = (uint64_t)*(float*)value;
            break;
        case DT_REAL8:
            *(uint64_t*) result = (uint64_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToUInt(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: 
            *(uint32_t*)result = strtoul((char*) value, NULL, 10);
            break;
        case DT_INT1: 
            *(uint32_t*) result = (uint32_t)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(uint32_t*) result = (uint32_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(uint32_t*) result = (uint32_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(uint32_t*) result = (uint32_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(uint32_t*) result = (uint32_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(uint32_t*) result = (uint32_t)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(uint32_t*) result = *(uint32_t*)value;
            break;
        case DT_UINT8:
            *(uint32_t*) result = (uint32_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(uint32_t*) result = (uint32_t)*(float*)value;
            break;
        case DT_REAL8:
            *(uint32_t*) result = (uint32_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToUSmallint(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: 
            sscanf((char*)value, "%hu", result);
            break;
        case DT_INT1: 
            *(uint16_t*) result = (uint16_t)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(uint16_t*) result = (uint16_t)*(int16_t*)value;
            break;
        case DT_INT4:
            *(uint16_t*) result = (uint16_t)*(int32_t*)value;
            break;
        case DT_INT8:
            *(uint16_t*) result = (uint16_t)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(uint16_t*) result = (uint16_t)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(uint16_t*) result = *(uint16_t*)value;
            break;
        case DT_UINT4:
            *(uint16_t*) result = (uint16_t)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(uint16_t*) result = (uint16_t)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(uint16_t*) result = (uint16_t)*(float*)value;
            break;
        case DT_REAL8:
            *(uint16_t*) result = (uint16_t)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToFloat(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: {
#ifdef _WIN32
            double tmp = strtod((char*)value, NULL);
            *(float*)result = (float)tmp;
#else
            *(float*)result = strtof((char*)value, NULL);
#endif
            }
            break;
        case DT_INT1: 
            *(float*) result = (float)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(float*) result = (float)*(int16_t*)value;
            break;
        case DT_INT4:
            *(float*) result = (float)*(int32_t*)value;
            break;
        case DT_INT8:
            *(float*) result = (float)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(float*) result = (float)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(float*) result = (float)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(float*) result = (float)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(float*) result = (float)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(float*) result = *(float*)value;
            break;
        case DT_REAL8:
            *(float*) result = (float)*(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToReal(void* value, DType thisDType, void* result) {
    switch (thisDType) {
        case DT_STRING: 
            *(double*)result = strtod((char*)value, NULL);
            break;
        case DT_INT1: 
            *(double*) result = (double)*(int8_t*)value;
            break;
        case DT_INT2: 
            *(double*) result = (double)*(int16_t*)value;
            break;
        case DT_INT4:
            *(double*) result = (double)*(int32_t*)value;
            break;
        case DT_INT8:
            *(double*) result = (double)*(int64_t*)value;
            break;
        case DT_UINT1: 
            *(double*) result = (double)*(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(double*) result = (double)*(uint16_t*)value;
            break;
        case DT_UINT4:
            *(double*) result = (double)*(uint32_t*)value;
            break;
        case DT_UINT8:
            *(double*) result = (double)*(uint64_t*)value;
            break;
        case DT_REAL4:
            *(double*) result = (double)*(float*)value;
            break;
        case DT_REAL8:
            *(double*) result = *(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

void convToDate(void* value, DType thisDType, void* result) {
    DBIngestor_error("castDTypeToDBType: Date types not yet supported.", NULL);
}

void convToTime(void* value, DType thisDType, void* result) {
    DBIngestor_error("castDTypeToDBType: Time types not yet supported.", NULL);
}

void convToAny(void* value, DType thisDType, void* result) {
    string tmpStr;
    char * outputStr;

    switch (thisDType) {
        case DT_STRING: 
            outputStr = (char*)malloc(strlen((char*)value)*sizeof(char));
            strcpy(outputStr, (char*)value);
            *(char**) result = outputStr;
            break;
        case DT_INT1: 
            *(int8_t*) result = *(int8_t*)value;
            break;
        case DT_INT2: 
            *(int16_t*) result = *(int16_t*)value;
            break;
        case DT_INT4:
            *(int32_t*) result = *(int32_t*)value;
            break;
        case DT_INT8:
            *(int64_t*) result = *(int64_t*)value;
            break;
        case DT_UINT1: 
            *(uint8_t*) result = *(uint8_t*)value;
            break;
        case DT_UINT2: 
            *(uint16_t*) result = *(uint16_t*)value;
            break;
        case DT_UINT4:
            *(uint32_t*) result = *(uint32_t*)value;
            break;
        case DT_UINT8:
            *(uint64_t*) result = *(uint64_t*)value;
            break;
        case DT_REAL4:
            *(float*) result = *(float*)value;
            break;
        case DT_REAL8:
            *(double*) result = *(double*)value;
            break;
        default:
            DBIngestor_error("castDTypeToDBType: DType not known, I don't know what to do.", NULL);
    }
}

std::string DBDataSchema::strDBType(DBType thisType) {
    switch (thisType) {
        case DBT_CHAR:
            return "DBT_CHAR";
        case DBT_BIT:
            return "DBT_BIT";
        case DBT_BIGINT:
            return "DBT_BIGINT";
        case DBT_MEDIUMINT:
            return "DBT_MEDIUMINT";
        case DBT_INTEGER:
            return "DBT_INTEGER";
        case DBT_SMALLINT:
            return "DBT_SMALLINT";
        case DBT_TINYINT:
            return "DBT_TINYINT";
        case DBT_FLOAT:
            return "DBT_FLOAT";
        case DBT_REAL:
            return "DBT_REAL";
        case DBT_DATE:
            return "DBT_DATE";
        case DBT_TIME:
            return "DBT_TIME";
        case DBT_ANY:
            return "DBT_ANY";
        case DBT_UBIGINT:
            return "DBT_UBIGINT";
        case DBT_UMEDIUMINT:
            return "DBT_UMEDIUMINT";
        case DBT_UINTEGER:
            return "DBT_UINTEGER";
        case DBT_USMALLINT:
            return "DBT_USMALLINT";
        case DBT_UTINYINT:
            return "DBT_UTINYINT";
        case DBT_UFLOAT:
            return "DBT_UFLOAT";
        case DBT_UREAL:
            return "DBT_UREAL";
        default:
            return "DBType not known...";
    }
}
