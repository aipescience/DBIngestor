/*  
 *  Copyright (c) 2012 - 2014, Adrian M. Partl <apartl@aip.de>, 
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


/*! \file SchemaItem.h
 \brief Schema object descriptor (column in database table)
 
 Class for describing a column in a database schema (columns
 in a database table)
 */

#include <string>
#ifndef _WIN32
#include <stdint.h>
#else
#include "stdint_win.h"
#endif
#include "DataObjDesc.h"
#include "DBType.h"

#ifndef DBIngestor_SchemaItem_h
#define DBIngestor_SchemaItem_h

#define EMPTY_SCHEMAITEM_NAME "EMPTY_COLUMN_NAME"

namespace DBDataSchema {
    
    /*! \class SchemaItem
     \brief SchemaItem class
     
     Class for abstracting database table columns
     */
	class SchemaItem {

	private:
        /*! \var string columnName
         name of the column in the database table
         */
        std::string columnName;

        /*! \var DBType columnDType
         data type of the column
         */
		DBType columnDBType;
        
        /*! \var bool isNotNull
         is not null flag
         */
        bool isNotNull;
        
        /*! \var int32_t columnSize
         column size of the data (SEE ODBC column sizes)
         */
        int32_t columnSize;
        
        /*! \var int32_t decimalDigits
         decimal digits of the data (SEE ODBC decimal digits)
         */
        int32_t decimalDigits;

        /*! \var DataObjDesc dataDesc
         descriptor to the data object in the file to be ingested
         */
        DataObjDesc * dataDesc;

	public:
        SchemaItem();
        
        virtual ~SchemaItem();
        
        std::string getColumnName();
	
		void setColumnName(std::string newColumnName);
	
		DataObjDesc * getDataDesc();
	
		void setDataDesc(DataObjDesc * newDataDesc);
	
		DBType getColumnDBType();
	
		void setColumnDBType(DBType newColumnDBType);
        
        bool getIsNotNull();
        
        void setIsNotNull(bool newIsNotNull);
        
        int32_t getColumnSize();
        
        void setColumnSize(int32_t newColumnSize);
        
        int32_t getDecimalDigits();
        
        void setDecimalDigits(int32_t newDecimalDigits);
	};
}

#endif
