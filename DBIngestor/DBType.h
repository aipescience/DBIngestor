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

/*! \file DBType.h
 \brief SQL data types for the database schema
 
 SQL data types for describing data in the database. DBT_ANY is there for compatibility
 with strange DBs such as SQLite3, which donot have strict typing
 */

#include "DType.h"
#include <string.h>

#ifndef DBIngestor_DBType_h
#define DBIngestor_DBType_h

namespace DBDataSchema {
    
#define DBT_MAXTYPE 19
    
    enum DBType {
        DBT_CHAR = 1,
        DBT_BIT = 2,
        DBT_BIGINT = 3,
        DBT_MEDIUMINT = 4,
        DBT_INTEGER = 5,
        DBT_SMALLINT = 6,
        DBT_TINYINT = 7,
        DBT_FLOAT = 8,
        DBT_REAL = 9,
        DBT_DATE = 10,
        DBT_TIME = 11,
        DBT_ANY = 12,
        DBT_UBIGINT = 13,
        DBT_UMEDIUMINT = 14,
        DBT_UINTEGER = 15,
        DBT_USMALLINT = 16,
        DBT_UTINYINT = 17,
        DBT_UFLOAT = 18,
        DBT_UREAL = 19
    };
    
    
    int getByteLenOfDBType(DBType thisType);
    
    //string casts are allocated in the function! as is a cast from string to string. memcpy involved!
    //freeing of the input string is responsibility of programmer. I would recomend use of std::string
    void castDTypeToDBType(void * value, DType fromThisType, DBType toThisType, void* result);

    DBType convDTypeToDBType(DType thisDType);
    
    std::string strDBType(DBType thisType);
}

#endif