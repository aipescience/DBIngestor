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


/*! \file DBIngestor.h
 \brief Data Ingestor Class
 
 This class provides all the functionality to ingest data from a given
 file and a given Schema into a database using buffered ingest.
 */

#include <string>
#include "Schema.h"
#include "Reader.h"
#include "DBAbstractor.h"
#include "AsserterFactory.h"
#ifndef _WIN32
#include <stdint.h>
#else
#include "stdint_win.h"
#endif

#ifndef DBIngestor_DBIngestor_h
#define DBIngestor_DBIngestor_h

namespace DBIngest {

    /*! \class DBIngestor
     \brief DBIngestor class
     
     Class providing the functionality to ingest data from a data
     source using a data schema into a database which is given through
     a DBAbstractor class.
     */
    class DBIngestor {

	private:
        /*! \var string usrName
         user name to connect to the database with
         */
		std::string usrName;

        /*! \var string passwd
         password to connect to the database with
         */
        std::string passwd;
		
        /*! \var string host
         host name or ip address of the database server
         */
        std::string host;

        /*! \var string port
         port with which to connect to the database server
         */
		std::string port;

        /*! \var string socket
         socket through which to connect to the database server
         */
		std::string socket;
        
        /*! \var int64_t performanceMeter
         number of rows that need to be ingested until performance data is shown. if this is -1, donot measure performance
         */
		int64_t performanceMeter;

        /*! \var uint32_t disableKeys
         if this variable is set, all keys/indexes will be disabled before ingest. Do remember to enable them, either manually or by
         setting enableKeys. If you use this, you should know what you are doing!
         */
		uint32_t disableKeys;

        /*! \var uint32_t enableKey
         if this variable is set, all keys/indexes will be reenabled AFTER the ingest is finished. WARNING: On large tables this can
         take a long time and should be better done manually! If you use this, you should know what you are doing!
         */
		uint32_t enableKeys;

        /*! \var bool isDryRun
         if this variable is set to true, the ingestor will run in dry run mode. This means, that everything except binding and commiting
         the data to the database is followed.
         */
		bool isDryRun;

        /*! \var bool resumeMode
         if set to true, the ingest will be done in resume mode. Resume mode will try to reconnect to the server if the
         connection fails and will continue the ingest at the point the connection failed. Resume mode however needs to 
         disable any global transactions on the ingest, i.e. will not run the setSavepoint command 
         (otherwise we cannot resume the ingest).
         */
        bool resumeMode;

        /*! \var DBDataSchema::Schema * myDBSchema
         pointer to the Schema class, describing the data to be read
         */
        DBDataSchema::Schema * myDBSchema;
		
        /*! \var DBReader::Reader * myReader
         pointer to the data reader for reading the data file
         */
        DBReader::Reader * myReader;

        /*! \var DBServer::DBAbstractor * myDBAbstractor
         pointer to the data base abstractor
         */
        DBServer::DBAbstractor * myDBAbstractor;

        /*! \var bool askUserToValidateRead
         when this is set, the ingestor asks the user to validate the read
         */
		bool askUserToValidateRead;

	public:
        DBIngestor();
        
        ~DBIngestor();

        /*! \brief constructor of 
                    a DBIngestor object to a given Schema, Data Reader, and DBAbstractor. 
         \param DBDataSchema::Schema * newSchema: the Schema used for reading and storing the data in the database
         \param DBReader::Reader * newReader: the data Reader used for reading the data from the input file
         \param DBServer::DBAbstractor * newAbstractora: a database abstractor object that handles the storage into a database
         
         \return returns a DBIngestor object
         
         Reads/parses the current row using the prescription defined in a data object. If the data
         has been successfully read, a void pointer to the data is returned.*/
        DBIngestor(DBDataSchema::Schema * newSchema, DBReader::Reader * newReader, DBServer::DBAbstractor * newAbstractor);
	
        /*! \brief validates the Schema with the database table given in the Schema. 
         
         \return 1 if successfull, 0 if the Schema does not map to the database table
         
         Retrieves the table schema from the database table defined in the Schema and matches it with the information
         given in the Schema object. If the table names and types match, a 1 is returned. This is also the case if there are
         columns missing in the Schema which can be NULL. If there is any difference, return 0.*/
		int validateSchema();
	
        /*! \brief ingests all the data in the Reader object into the database. 
         
         \param int lenBuffer: length of the ingest buffer to be used

         \return 1 if successfull, 0 if the Schema does not map to the database table
         
         Retrieves the table schema from the database table defined in the Schema and matches it with the information
         given in the Schema object. If the table names and types match, a 1 is returned. This is also the case if there are
         columns missing in the Schema which can be NULL. If there is any difference, return 0.*/
		int ingestData(int lenBuffer);
	
		std::string getUsrName();
	
		void setUsrName(std::string newUsrName);
	
		std::string getPasswd();
	
		void setPasswd(std::string newPasswd);
	
		std::string getHost();
	
		void setHost(std::string newHost);
	
		std::string getPort();
	
		void setPort(std::string newPort);

        std::string getSocket();
        
		void setSocket(std::string newSocket);

        int64_t getPerformanceMeter();
        
		void setPerformanceMeter(int64_t newPerformanceMeter);

        uint32_t getDisableKeys();
        
		void setDisableKeys(uint32_t newDisableKeys);

        uint32_t getEnableKeys();
        
		void setEnableKeys(uint32_t newEnableKeys);

        bool getIsDryRun();
        
		void setIsDryRun(bool newIsDryRun);

        bool getResumeMode();
        
        void setResumeMode(bool newResumeMode);

		DBDataSchema::Schema * getSchema();
	
		void setSchema(DBDataSchema::Schema * newDBSchema);
	
		DBReader::Reader * getReader();
	
		void setReader(DBReader::Reader * newReader);
        
        DBServer::DBAbstractor * getDBAbstractor();
        
        void setDBAbstractor(DBServer::DBAbstractor * newDBAbstractor);
        
        bool getAskUserToValidateRead();
        
        void setAskUserToValidateRead(bool val);
	};
}

#endif
