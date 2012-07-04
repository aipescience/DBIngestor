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


/*! \file DBAdaptorsFactory.h
 \brief Factory class for DB adaptors
 
 Factory class for database adaptor objects. Add your own adaptor here,
 including the proper header file and adding the corresponding 
 line to the factory method.
 */

#include <string>
#include "DBAbstractor.h"

#ifndef DBIngestor_DBAdaptorsFactory_h
#define DBIngestor_DBAdaptorsFactory_h

namespace DBServer {
    /*! \class ConverterFactory
     \brief ConverterFactory factory class
     
     Returns a new initialised Converter object. 
     */
	class DBAdaptorsFactory {
        
	public:
        DBAdaptorsFactory();
        
        ~DBAdaptorsFactory();

        /*! \brief returns a db abstractor object with the specified name
         \param string name: name of the DB adaptor object
         \return an initialised DB adaptor object
         
         Returns the database adaptor object that is requested. Will look through all the possible
         Adaptors and returns the one that is needed.*/
		DBAbstractor * getDBAdaptors(std::string name);
	};
}

#endif
