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

/*! \file Schema.h
 \brief Schema  descriptor (database table abstraction)
 
 Abstraction class for existing tables in a database
 */

#include <string>
#include <vector>
#include "SchemaItem.h"

#ifndef DBIngestor_Schema_h
#define DBIngestor_Schema_h

namespace DBDataSchema {

    bool compSchemaItem (SchemaItem * i, SchemaItem * j);

    /*! \class Schema
     \brief Schema class
     
     Class for abstracting database tables
     */
    class Schema {

	private:
        /*! \var string dbName
         name of the database
         */
        std::string dbName;

        /*! \var string tableName
         name of the database table
         */
        std::string tableName;
		
        /*! \var vector<SchemaItem> arrSchemaItems
         array of columns (SchemaItems) in the database table
         */
        std::vector<SchemaItem*> arrSchemaItems;

        /*! \var int32_t numActiveItems
         number of items that are actually transfered to the DB
         */
        int32_t numActiveItems;

	public:
        Schema();
        
        virtual ~Schema();
        
        std::string getDbName();
	
		void setDbName(std::string newDbName);
        
        std::string getTableName();
        
        void setTableName(std::string newTableName);
        
        int32_t getNumActiveItems();

        std::vector<SchemaItem*> & getArrSchemaItems();
	
		void addItemToSchema(SchemaItem * thisItem);
        
        void sortSchema();
        
        int64_t getRowSizeInBytes();
        
        void printSchema();

        void prepareSchemaForNextRow();
	};
}

#endif
