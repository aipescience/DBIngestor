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


/*! \file DBIngestBuffer.h
 \brief Data Ingest Buffer Interface Class
 
 This class provides all the functionality to ingest data from a given
 file and a given Schema into a database using buffered ingest. It might need
 to be reimplemented for certain database systems to make things more efficient!
 */

#include "Schema.h"
#include "DBAbstractor.h"
#ifndef _WIN32
#include <stdint.h>
#else
#include "stdint_win.h"
#endif

#ifndef DBIngestor_DBIngestBuffer_h
#define DBIngestor_DBIngestBuffer_h

namespace DBIngest  {
    /*! \class DBIngestBuffer
     \brief DBIngestBuffer Interface class
     
     This class provides all the functionality to ingest data from a given
     file and a given Schema into a database using buffered ingest. It might need
     to be reimplemented for certain database systems to make things more efficient!
     */
	class DBIngestBuffer {

	protected:
        /*! \var int bufferSize
         the maximum size of the buffer before data is commited and/or a transaction is closed
         */
		int bufferSize;

        /*! \var int currSize
         current number of rows in the buffer
         */
        int currSize;

        /*! \var void ** bufferArray
         the bufferArray holding buffers: buffer holds columns in row subsequently. for strings, a pointer to a string
         is saved (i.e. char**) and the buffer is set up according to the Schema object in that ordering.
         */
        void ** bufferArray;

        /*! \var bool ** isNullArray
         array storing information if given field is NULL
         */
        bool ** isNullArray;

        /*! \var int32_t * lenArray
         array holding the length of the row arrays in the buffer array. 
         */
        int32_t * lenArray;

        /*! \var DBDataSchema::Schema * myDBSchema
         pointer to the associated Schema
         */
        DBDataSchema::Schema * myDBSchema;
        
        /*! \var int64_t basicSizeRow
         hold the minimum size of a row as determined from the Schema
         */
        int64_t basicSizeRow;

        /*! \var int64_t * colLookupArray
         array holding an offset lookup table for fast access to the various elements in a row
         */
        int64_t * colLookupArray;
        
        /*! \var int currRowItemByte
         the current byte into which a value is added
         */
        int currRowItemByte;

        /*! \var int currRowItemId
         the current index of the value to add
         */
        int currRowItemId;

        /*! \var DBServer::DBAbstractor * myDBAbstractor
         pointer to the DBAbstractor object
         */
        DBServer::DBAbstractor * myDBAbstractor;
        
        /*! \var void* preparedStmt
         prepared statement to use for the ingest
         */
        void* preparedStmt;

        /*! \var int lenPreparedStmt
         length of the prepared statement, i.e. the numbers of rows covered
         */
        int lenPreparedStmt;

        /*! \var void* preparedStmtRemain
         prepared statement to use for the remainding buffer items per ingest
         */
        void* preparedStmtRemain;
        
        /*! \var int lenPreparedStmtRemain
         length of the remainder prepared statement, i.e. the numbers of rows covered
         */
        int lenPreparedStmtRemain;

        /*! \var bool isDryRun
         if this is set to true, a dry run is carried out. This means, that newRow will not issue the
         commit command while ingesting.
         */
        bool isDryRun;


	public:
        DBIngestBuffer();
        
        ~DBIngestBuffer();
        
        /*! \brief constructor of a DBIngestBuffer 
                    object to a given Schema and DBAbstractor. 
         \param DBDataSchema::Schema * newSchema: the Schema used for reading and storing the data in the database
         \param DBServer::DBAbstractor * newDBAbstractor: the Schema used for reading and storing the data in the database
         
         Initialises a DBIngestBuffer with a Schema and a DBAbstractor.*/
		DBIngestBuffer(DBDataSchema::Schema * newSchema, DBServer::DBAbstractor * newDBAbstractor);
	
        /*! \brief interface method for adding a new row to the buffer. 

         \return returns 1 if successfull or 0 if not
         
         INTERFACE METHOD: developer needs to implement this. This method adds a new row to the buffer. If the buffer is full, the buffer
         will be commited to the database and the buffer is flushed. A new row is then added again.*/
		virtual int newRow() ;
	
        /*! \brief interface method for adding a data field
                                    to a row. 

         \param void* value: void pointer to the data field
         \param bool isNull: decodes whether current value is NULL or not
         \param DBDataSchema::SchemaItem * thisSchemaItem: pointer to the SchemaItem

         \return returns 1 if successfull or 0 if not
         
         INTERFACE METHOD: developer needs to implement this. This method adds a data field to the currently active row according to the
         SchemaItem. HOWEVER: To improve performance, this method does not put value at the right place in bufferArray. This would require
         a lookup in the Schema for every item added. Therefore it is THE DEVELOPERS responsability to add the data in the RIGHT ORDER ACCORDING
         TO THE SCHEMA! Data has to be in the DBT_Type format, i.e. in the format as is on the database side.*/
		virtual int addToRow(void* value, bool isNull, DBDataSchema::SchemaItem * thisSchemaItem);
	
        /*! \brief clears the buffer
         
         
         \return returns 1 if successfull or 0 if not
         
         INTERFACE METHOD: developer needs to implement this. This method clears the buffer, sets its size to 0, freeing all memory for new
         additions of rows. Additionally it starts a new transaction.*/
		virtual int clear();
        
        /*! \brief commits the buffer to the database
         
         
         \return returns 1 if successfull or 0 if not
         
         INTERFACE METHOD: developer needs to implement this. This method commits the buffer to the database, closing the active transaction.*/
        virtual int commit();
        
        void setIsDryRun(bool newIsDryRun);
        
        int getCurrSize();
        
        int getBufferSize();
        
        virtual void setBufferSize(int newBufferSize);
        
        DBDataSchema::Schema * getDBSchema();
        
        void setDBSchema(DBDataSchema::Schema * newSchema);        

        DBServer::DBAbstractor * getDBAbstractor();
        
        void setDBAbstractor(DBServer::DBAbstractor * newDBAbstractor);       
        
        int initPreparedStmt(int numRows);
    };
}

#endif
