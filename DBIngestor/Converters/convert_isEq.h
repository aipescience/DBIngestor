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


/*! \file Converter.h
 \brief Interface class for Converter functions
 
 Interface for Converter functions that can be used to validate
 the data
 */

#include <string>
#include <vector>
#include "Converter.h"
#include "DType.h"

#ifndef DBIngestor_convert_iseq_h
#define DBIngestor_convert_iseq_h

namespace DBConverter {
    /*! \class convert_ispositive
     \brief Converter class implementing ispositive check
     
     Implementation of ispositive check
     */
	class convert_iseq : public Converter {
        
	private:

	public:
        convert_iseq();
        
        ~convert_iseq();
        
        Converter * clone();
        
        /*! \brief executes the convertion for a specific data object and value
         \param DBDataSchema::DType thisDType: dtype to execute this convertion for
         \param void* value: pointer to the value that needs to be converted
         \return 1 if convertion is passed, 0 if convertion is not passed successfully
         
         Runs the convertion on the specified value. Returns 1 if the convertion is passen, 0 if not. This needs to be
         implemented by the convertion implementer.*/
		bool execute(DBDataSchema::DType thisDType, void* value);
    };
}

#endif
