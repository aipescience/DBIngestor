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


/*! \file AsserterFactory.h
 \brief Factory class for possible asserter functions
 
 Factory class for asserter objects. Add your own asserter here,
 including the proper header file and adding the corresponding 
 line to the factory method.
 */

#include <string>
#include <vector>
#include "Asserter.h"
#include "DataObjDesc.h"
#include "DType.h"

#ifndef DBIngestor_AsserterFactory_h
#define DBIngestor_AsserterFactory_h

namespace DBAsserter {
    /*! \class AsserterFactory
     \brief AsserterFactory factory class
     
     Returns a new initialised asserter object. 
     */
	class AsserterFactory {
        
    private:
        /*! \var vector<string> nameArray
         holds a list of all registered asserters for faster lookup. this is fed by registerAssert
         */
        std::vector<std::string> nameArray;
        
        /*! \var vector<Asserter*> asserterArray
         holds a list of all registered asserter objects, which are initialised in registerAssert. this is fed by registerAssert
         */
        std::vector<Asserter*> asserterArray;
        
        /*! \brief registers a asserter class with the factory
         \param class T: an implementation of the asserter class
         \param Asserter * thisAssert = new T: a pointer to an assertion class, if it is already allocated
         
         This function registers an implementation of the asserter class with the factory. DONOT give a asserter
         pointer to this class, it is not needed. The default value assigned here, will allocate and initialise
         a new assertion object of the derived type you implemented for you. If you still need to pass a pointer,
         feel free to do so. This function will register the asserter class pointer and the name of the asserter
         as defined in the implementation with the approperiate arrays.*/
        template <class T> void registerAssert(Asserter * thisAssert = new T);

	public:
        AsserterFactory();
        
        ~AsserterFactory();
        
        /*! \brief returns a initialised assertion object with the specified name
         \param string name: name of the asserter object
         \return an initialised asserter object
         
         Returns the asserter object that is requested. Will look through all the possible
         asserters and returns the one that is needed.*/
		Asserter * getAsserter(std::string name);
	};
}

#endif
