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


/*! \file HeaderReader.h
 \brief Header Reader interface
 
 Interface for logic which can obtain information from the file
 header. This needs to be implemented by the developer and only
 a generic set of function is provided as such.
 */

#include <string>
#include <stdio.h>
#include "DataObjDesc.h"

#ifndef DBIngestor_HeaderReader_h
#define DBIngestor_HeaderReader_h

namespace DBReader {
    /*! \class HeaderReader
     \brief HeaderReader class interface
     
     Interface class which needs to be implemented by the
     developer for reading information from the data file 
     header. Only getItemBySearch is implemented in a generic
     fashion, but might need overloading for specific purposes.
     */
	class HeaderReader {

	private:
        /*! \var string headerContent
         a complete copy of the header string in memory
         */
        std::string headerContent;
        
        /*! \var bool askUserToValidateRead
         when this is set, the ingestor asks the user to validate the read
         */
		bool askUserToValidateRead;

	public:
        HeaderReader();
        
        virtual ~HeaderReader();
        
        /*! \brief reads the header from file
         \param FILE * filePointer: open filePointer to data file
         \return NONE
         
         This method extracts the header from the data file and saves its
         content in the class as a string. This needs to be implemented by the
         developer. This method needs to assure, that the filePointer is valid!
         If the header is in binary format, the developer can convert its content
         into a string from and save it in the class for further use.*/
		virtual void parseHeader(FILE * filePointer);
	
        /*! \brief reads the specified header
                   item from the header.
         \param DBDataSchema::DataObjDesc * thisItem: a header data object
         \return void*: a pointer to the data
         
         This developer implemented method looks for the specified data object item in the header
         and returns its value. This needs to be implented by the developer accordingly.*/
		virtual void* getItem(DBDataSchema::DataObjDesc * thisItem) = 0;
	
        /*! \brief searches for the string
                   field name specified in the data object. Generic function, working with any
                   string header.
         \param DBDataSchema::DataObjDesc * thisItem: a header data object with a specified field name
         \param string searchString: the string to look for the item
         \return void*: a pointer to the data
         
         This method is generically provided and searches for the name defined in the data object
         in the header string. If the string is found, the value is tried to be read by this method
         according to the data type defined in the data object. This interface class provides a more
         general implementation of this method and if more functionality is needed by the developer,
         or the general implementation does not work, the developer can implement this class through
         overloading.*/
		void* getItemBySearch(DBDataSchema::DataObjDesc * thisItem, std::string searchString);
	
        std::string getHeaderContent();
	
		void setHeaderContent(std::string newHeaderContent);
        
        bool getAskUserToValidateRead();
        
        void setAskUserToValidateRead(bool val);
	};
}

#endif