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


/*! \file Reader.h
 \brief Data File Reader interface
 
 Interface for logic which can obtain information from the data file. 
 This needs to be implemented by the developer.
 */

#include <string>
#include <stdio.h>
#include "DataObjDesc.h"
#include "HeaderReader.h"
#include "Schema.h"

#ifndef DBIngestor_Reader_h
#define DBIngestor_Reader_h

namespace DBReader {
    /*! \class Reader
     \brief Reader class interface
     
     Interface class which needs to be implemented by the
     developer for reading information from the data file. 
     */
	class Reader {

	private:
        /*! \var DBReader::HeaderReader header
         header class of the data file
         */
		DBReader::HeaderReader * header;

        /*! \var DBDataSchema::Schema * schem
         a pointer to the data schema that is connected to this reader
         */
		DBDataSchema::Schema * schema;
        
    protected:
        void checkAssertions(DBDataSchema::DataObjDesc * thisItem, void* result);
        bool applyConversions(DBDataSchema::DataObjDesc * thisItem, void* result);

        unsigned long long readCount;

	public:
        Reader();
        
        virtual ~Reader();
        
        /*! \brief opens a data file for reading
         \param string newFileName: path and name of the file to open
         \return NONE
         
         Opens a data file for reading, performs basic checks on the data if it
         is available and sound.*/
		virtual void openFile(std::string newFileName);
	
        /*! \brief closes the file
         \param NONE
         \return NONE
         
         Closes the data file.*/
		virtual void closeFile();

        /*! \brief rewind the file
         \param NONE
         \return NONE
         
         Seeks to the begining of the file to start again.*/
		virtual void rewind();

        /*! \brief skips the header and moves to where the data starts
         \param NONE
         \return NONE
         
         Seeks to the begining of the file, where the bulk data starts, skipping the header.*/
		virtual void skipHeader();

        /*! \brief reads a next row from the file (buffered read) and
                   if necessary fills the read buffer with new data
         \param NONE
         \return int: error code
         
         Reads a new row from the data file, stores it in the memory or if the developer
         implemented a buffered read, fills the buffer with new data if needed. The data in
         the row can then be accessed through getItemInRow.*/
		virtual int getNextRow() = 0;
	
        /*! \brief reads a data item from
                   the current already read data row.
         \param DBDataSchema::DataObjDesc thisItem: the data object describing what needs to be
                read
         \param bool appyAsserters: appy the assertion functions
         \param bool appyConverters: apply the convertion functions
         \param void* result: writes the data item in the row to this address space (buffer that is previously allocated!)
         
         \retrun 1 if element is NULL, 0 if not
         
         Reads/parses the current row using the prescription defined in a data object. If the data
         has been successfully read, a void pointer to the data is returned. This function returns a 1 if the
         value is a NULL and 0 if the value is not NULL.*/
		virtual bool getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result) = 0;

        /*! \brief retrieves a constant item
         \param DBDataSchema::DataObjDesc thisItem: the data object describing what needs to be
         read
         \param void* result: writes the data item in the row to this address space (buffer that is previously allocated!)
         
         Reads/parses the current constant item. If the constant item has not yet been parsed and saved in the constant
         data field of the DataObjDesc object, this function does that. If it finds an already converted item, this returns
         the pointer to the data*/
        virtual void getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result) = 0;

        DBReader::HeaderReader * getHeader();
	
        void setHeader(DBReader::HeaderReader * newHeader);

        DBDataSchema::Schema * getSchema();
        
        void setSchema(DBDataSchema::Schema * newSchema);

        unsigned long long getReadCount();
    };
}

#endif
