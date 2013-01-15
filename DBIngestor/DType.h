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

/*! \file DType.h
 \brief Data types for the data file
 
 Common data types for describing the data in the data file
 */

#include <string>
#ifndef _WIN32
#include <stdint.h>
#else
#include "stdint_win.h"
#endif

#ifndef DBIngestor_DType_h
#define DBIngestor_DType_h

namespace DBDataSchema {
    
#define DT_MAXTYPE 11
    
    enum DType {
        DT_STRING = 1,
        DT_INT1 = 2,
        DT_INT2 = 3,
        DT_INT4 = 4,
        DT_INT8 = 5,
        DT_REAL4 = 6,
        DT_REAL8 = 7,
        DT_UINT1 = 8,
        DT_UINT2 = 9,
        DT_UINT4 = 10,
        DT_UINT8 = 11
    };

    /*! \brief tests whether this string version of DType is valid
     \param string thisDType: string holding the string DType representation
     \return 1 if valid, 0 is not
     
     This function checks the validity of a DType string representation and returns 1 if valid.*/
    int testDType(std::string thisDType); 
    
    DType convDType(std::string thisDType);
        
    int castStringToDType(std::string & thisString, DType thisType, void* result);
    
    void printThisDType(void* var, DType thisType);
    
    int getByteLenOfDType(DType thisType);

    int8_t castToInt1(DType thisType, void* value);

    int16_t castToInt2(DType thisType, void* value);

    int32_t castToInt4(DType thisType, void* value);

    int64_t castToInt8(DType thisType, void* value);

    int8_t castToUInt1(DType thisType, void* value);
    
    int16_t castToUInt2(DType thisType, void* value);
    
    int32_t castToUInt4(DType thisType, void* value);
    
    int64_t castToUInt8(DType thisType, void* value);

    float castToFloat(DType thisType, void* value);

    double castToDouble(DType thisType, void* value);

    char * castToString(DType thisType, void* value);

}

#endif


