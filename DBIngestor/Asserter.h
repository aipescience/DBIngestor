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


/*! \file Asserter.h
 \brief Interface class for asserter functions
 
 Interface for asserter functions that can be used to validate
 the data
 */

#include <string>
#include <vector>
#include "DType.h"

#ifndef DBIngestor_Asserter_h
#define DBIngestor_Asserter_h

namespace DBAsserter {
    /*! \class Asserter
     \brief Asserter class interface
     
     Implement this class, if you want to build your own asserter object. 
     */
	class Asserter {

	private:
        /*! \var string name
         identifier string for the asserter
         */
		std::string name;

        /*! \var vector<DBDataSchema::DType> DTypeArray
         supported DTypes that the asserter can handle
         */
        std::vector<DBDataSchema::DType> DTypeArray;
    
    protected:
        void setName(std::string newString);

        void setDTypeArray(DBDataSchema::DType * newDTypeArray, int size);

	public:
        Asserter();
        
        virtual ~Asserter();
        
        /*! \brief check whether DType is supporter by this asserter
         \param DBDataSchema::DType thisDType: dtype to test whether this assertion works or not
         \return 1 if ok, 0 if not
         
         Checks whether the DType of this data object can be handled by this asserter. If not, throw an
         error.*/
		int checkDType(DBDataSchema::DType thisDType);
	
        /*! \brief if the specific implementation supports parameters, this function sets the specified parameter
         \param int parNum: the number of the parameter to set
         \param void* value: value to set
         \return 1 if ok, 0 if not
         
         Sets the parNum-th parameter as defined by the implementor, with a specified value. This needs to be
         implemented by the assertion implementer.*/
		virtual int setParameter(int parNum, void* value) = 0;
	
        /*! \brief if the specific implementation supports parameters, this function returns the specified parameter
         \param int parNum: the number of the parameter to set
         \return a void pointer to the parameter variable.
         
         Sets the parNum-th parameter as defined by the implementor, with a specified value. This needs to be
         implemented by the assertion implementer.*/
		virtual void* getParameter(int parNum) = 0;
	
        /*! \brief executes the assertion for a specific data object and value
         \param DBDataSchema::DType thisDType: dtype to execute this assertion for
         \param void* value: pointer to the value that needs to be asserted
         \return 1 if assertion is passed, 0 if assertion is not passed successfully
         
         Runs the assertion on the specified value. Returns 1 if the assertion is passen, 0 if not. This needs to be
         implemented by the assertion implementer.*/
		virtual bool execute(DBDataSchema::DType thisDType, void* value) = 0;

        virtual std::string getName();
    };
}

#endif
