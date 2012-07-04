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


/*! \file ConverterFactory.h
 \brief Factory class for possible Converter functions
 
 Factory class for Converter objects. Add your own Converter here,
 including the proper header file and adding the corresponding 
 line to the factory method.
 */

#include <string>
#include <vector>
#include "Converter.h"
#include "DataObjDesc.h"
#include "DType.h"

#ifndef DBIngestor_ConverterFactory_h
#define DBIngestor_ConverterFactory_h

namespace DBConverter {
    /*! \class ConverterFactory
     \brief ConverterFactory factory class
     
     Returns a new initialised Converter object. 
     */
	class ConverterFactory {
        
    private:
        /*! \var vector<string> nameArray
         holds a list of all registered Converters for faster lookup. this is fed by registerConvert
         */
        std::vector<std::string> nameArray;
        
        /*! \var vector<Converter*> ConverterArray
         holds a list of all registered Converter objects, which are initialised in registerConvert. this is fed by registerConvert
         */
        std::vector<Converter*> ConverterArray;
        
        /*! \brief registers a Converter class with the factory
         \param class T: an implementation of the Converter class
         \param Converter * thisConvert = new T: a pointer to an convertion class, if it is already allocated
         
         This function registers an implementation of the Converter class with the factory. DONOT give a Converter
         pointer to this class, it is not needed. The default value assigned here, will allocate and initialise
         a new convertion object of the derived type you implemented for you. If you still need to pass a pointer,
         feel free to do so. This function will register the Converter class pointer and the name of the Converter
         as defined in the implementation with the approperiate arrays.*/
        template <class T> void registerConvert(Converter * thisConvertt = new T);

	public:
        ConverterFactory();
        
        ~ConverterFactory();

        /*! \brief returns a newly instanciated convertion object with the specified name
         \param string name: name of the Converter object
         \return an initialised Converter object
         
         Returns the Converter object that is requested. It will instanciate a new converter.*/
		Converter * newParamConverter(std::string name);

        /*! \brief returns a initialised convertion object with the specified name
         \param string name: name of the Converter object
         \return an initialised Converter object
         
         Returns the Converter object that is requested. Will look through all the possible
         Converters and returns the one that is needed.*/
		Converter * getConverter(std::string name);
	};
}

#endif
