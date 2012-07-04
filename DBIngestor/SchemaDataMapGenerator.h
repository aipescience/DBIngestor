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


/*! \file SchemaDataMapGenerator.h
 \brief Interface for defining a schema to data mapper
 
 Interface class which has to be implemented by the developer
 to generate a schema to data relation
 */

#include <string>
#include "Schema.h"

#ifndef DBIngestor_SchemaDataMapGenerator_h
#define DBIngestor_SchemaDataMapGenerator_h

namespace DBDataSchema {
    /*! \class SchemaDataMapGenerator
     \brief SchemaDataMapGenerator class
     
     Interface class which generates a map between a schema and 
     the input files. Depending on the implementation of the data
     reader, this will work differently and has to be implemented
     by the developer.
     */
	class SchemaDataMapGenerator {


	public:
        SchemaDataMapGenerator();
        
        virtual ~SchemaDataMapGenerator();
        
        /*! \brief generates a schema class, with the approperiate data mappings.
         \param NONE
         \return DBDataSchema::Schema
         
         This method will initialize and properly link the data files, with the schema as defined
         by the developer. It will return an instance of the Schema class, which can then be used
         by the data reader to properly read the data file and pass its fields to the abstract
         database ingestor.*/
		virtual DBDataSchema::Schema * generateSchema();
	};
}

#endif
