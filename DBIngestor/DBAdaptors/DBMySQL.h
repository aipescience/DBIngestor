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


/*! \file DBMySQL.h
 \brief Implementation of DBAbstractor for MySQL
 
 This provides an implementation of DBAbstractor for MySQL.
 */

#include "DBAbstractor.h"

#ifdef _WIN32
#include <winsock.h>
#endif

#include <mysql.h>

#ifndef DBIngestor_DBMySQL_h
#define DBIngestor_DBMySQL_h

namespace DBServer {
    
    /*! \class DBMySQL
     \brief DBMySQL communication class
     
     This class implements all the DBAbstractor methods needed for communicating
     with an MySQL database.
     */
    class DBMySQL : public DBAbstractor {
    private:
        /*! \var MYSQL * dbHandler
         a pointer to the MYSQL db handler
         */
        MYSQL * dbHandler;
        
        /*! \brief translates type on the server into DBType. 
         
         \param char * thisTypeString: a string returned by MySQL describing the column type
         \return returns according DBType
         
         Parses the column type returned by the MySQL server and translates this into a
         corresponding DBType. THE ORDER IN WHICH THINGS ARE CHECKED IN THIS FUNCTION IS
         IMPORTANT! THINK BEFORE YOU CHANGE IT!*/
        DBDataSchema::DBType getType(char * thisTypeString);
        
        /*! \brief translates DBType into type that the MySQL API understands. 
         
         \param DBDataSchema::DBType thisType: DBType to translate into MySQL API lingo
         \return returns MySQL API type
         
         Converts a DBType into a type that MySQL API understands.*/
        enum_field_types translateTypeToMYSQL(DBDataSchema::DBType thisType);

        /*! \brief checks whether DBType is an unsigned type or not
         
         \param DBDataSchema::DBType thisType: DBType to translate into MySQL API lingo
         \return returns 1 if unsigned, 0 if signed
         
         Checks whether a DBType is signed or unsigend. If it is signed, return 0, if unsigned, return 1.*/
        int isUnsignedType(DBDataSchema::DBType thisType);
        
    public:
        DBMySQL();
        
        ~DBMySQL();        
        
        /*! \brief connects to a database server. 
         \param string usr: username with which to connect to the DB server
         \param string pwd: password for the given user
         \param string host: host or ip address to the database server
         \param string port: port of the database server
         
         \return returns 1 if successfull or 0 if not
         
         Opens a connection to a database server at the given host and port, using the given username
         and password. If the connection was sucessfully established, this shall return 1, otherwise 0.*/
		virtual int connect(std::string usr, std::string pwd, std::string host, std::string port, std::string socket);
        
        /*! \brief disconnects from the database server. 
         
         \return returns 1 if successfull or 0 if not
         
         Disconnects from the database server. If the disconnect was successfull, this shall return 1, otherwise 0.*/
		virtual int disconnect();
        
        /*! \brief sets a new savepoint if supported by the DB engine. 
         
         \return returns 1 if successfull or 0 if not
         
         Sets a savepoint or opens a new transaction depending on the database capabilities. For databases that donot support transactions
         and/or savepoints, this function will still pretend to function properly. However no acction is carried out. If the savepoint
         was successfully set, this shall return 1, otherwise 0.*/
        virtual int setSavepoint();
        
        /*! \brief starts a rollback if supported. 
         
         \return returns 1 if successfull or 0 if not
         
         Starts the rollback process of all the data ingested in the current transaction. For databases that donot support transactions
         and/or savepoints, this function will still pretend to function properly. However no acction is carried out. If the rollback
         was successfull, this shall return 1, otherwise 0.*/
        virtual int rollback();
        
        /*! \brief release savepoint. 
         
         \return returns 1 if successfull or 0 if not
         
         Releases the savepoint and permanently adds the data to the database. Transactions are all closed, no rollback beyond this point. 
         For databases that donot support transactions and/or savepoints, this function will still pretend to function properly. 
         However no acction is carried out. If the rollback was successfull, this shall return 1, otherwise 0.*/
        virtual int releaseSavepoint();

        /*! \brief disables the keys of a given table. 
         \param DBDataSchema::Schema thisSchema: a valid Schema from which the keys are disabled
         
         \return returns 1 if successfull or 0 if not
         
         Calling this function will disable (not delete!) all the keys/indexes on a given table.*/
        virtual int disableKeys(DBDataSchema::Schema * thisSchema);
        
        /*! \brief reenables the keys of a given table. 
         \param DBDataSchema::Schema thisSchema: a valid Schema from which the keys are reenabled
         
         \return returns 1 if successfull or 0 if not
         
         Calling this function will reenable all the keys/indexes on a given table.*/
        virtual int enableKeys(DBDataSchema::Schema * thisSchema);

        /*! \brief retrieves a Schema object from a given database table. 
         \param string database: name of a database on the server
         \param string table: name of a table in the given database on the server
         
         \return returns a pointer to a Schema object describing the database table.
         
         Retrieves the table schema of a given table in a given database on the server. This method will return a
         Schema object to describe the schema of the table.*/
		virtual DBDataSchema::Schema * getSchema(std::string database, std::string table);
        
        /*! \brief generate a prepared statement from a Schema. 
         \param DBDataSchema::Schema thisSchema: a valid Schema from which the prepared statement is generated from
         
         \return returns a pointer to the prepared statement object.
         
         Generates and initialises a prepared statement from a given valid Schema for one row. This method returns a pointer to the
         prepared statement object, which differs from database API to API. Specific use needs to ensure a proper casting
         of the object.*/
		virtual void* prepareIngestStatement(DBDataSchema::Schema * thisSchema);
        
        /*! \brief generate a prepared statement from a Schema
         with multiple rows. 
         \param DBDataSchema::Schema thisSchema: a valid Schema from which the prepared statement is generated from
         \param int numElements: number of rows handles by the statement at one time
         
         \return returns a pointer to the prepared statement object.
         
         Generates and initialises a prepared statement from a given valid Schema for numElements rows. This is mostly used
         for ingesting large amaount of data. This method returns a pointer to the prepared statement object, which differs 
         from database API to API. Specific use needs to ensure a proper casting of the object.*/
        virtual void* prepareMultiIngestStatement(DBDataSchema::Schema * thisSchema, int numElements);
        
        /*! \brief insert one row into the database. 
         \param DBDataSchema::Schema * thisSchema: a valid Schema where the data should be inserted
         \param void** thisData: A pointer array to the data with length len(Schema) and the same ordering as in Schema.
         
         \return returns 1 if successfull, 0 if not
         
         Inserts one row into a given Schema. The data is stored in a void pointer array and is then cast according to the
         Schema. The length of the void pointer array has the same size as Schema and needs to be of equal ordering!*/
		virtual int insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData);
        
        /*! \brief insert one row using a
         given prepared statement into the database.  
         \param DBDataSchema::Schema * thisSchema: a valid Schema where the data should be inserted
         \param void** thisData: A pointer array to the data with length len(Schema) and the same ordering as in Schema.
         \param void* preparedStatement: a pointer to a prepared statement object
         
         \return returns 1 if successfull, 0 if not
         
         Inserts one row into a given Schema using a prepared statement. The data is stored in a void pointer array and is 
         then cast according to the Schema. The length of the void pointer array has the same size as Schema and needs 
         to be of equal ordering!*/
        virtual int insertOneRow(DBDataSchema::Schema * thisSchema, void** thisData, void* preparedStatement);
        
        /*! \brief binds a given row to a prepared statement (works as well for multi statements).  
         \param DBDataSchema::Schema * thisSchema: a valid Schema where the data should be inserted
         \param void** thisData: A pointer array to the data with length len(Schema) and the same ordering as in Schema.
         \param void* preparedStatement: a pointer to a prepared statement object
         \param int nInStmt: the position (i.e. row) in the statement where to add this row
         
         \return returns 1 if successfull, 0 if not
         
         Inserts one row into a given Schema into the nInStmt-th row using a prepared statement. The data is stored in a 
         void pointer array and is then cast according to the Schema. The length of the void pointer array has the same 
         size as Schema and needs to be of equal ordering!*/
        virtual int bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, void* preparedStatement, int nInStmt);
        
        /*! \brief binds a given row to a prepared statement (works as well for multi statements).  
         \param DBDataSchema::Schema * thisSchema: a valid Schema where the data should be inserted
         \param void** thisData: A pointer array to the data with length len(Schema) and the same ordering as in Schema.
         \param bool* isNullArray: pointer to array that holds information about whether the item is null or not
         \param void* preparedStatement: a pointer to a prepared statement object
         \param int nInStmt: the position (i.e. row) in the statement where to add this row
         
         \return returns 1 if successfull, 0 if not
         
         Inserts one row into a given Schema into the nInStmt-th row using a prepared statement. The data is stored in a 
         void pointer array and is then cast according to the Schema. The length of the void pointer array has the same 
         size as Schema and needs to be of equal ordering!*/
        virtual int bindOneRowToStmt(DBDataSchema::Schema * thisSchema, void* thisData, bool* isNullArray, void* preparedStatement, int nInStmt);

        /*! \brief executes the given statement.  
         \param void* preparedStatement: a pointer to a prepared statement object
         
         \return returns 1 if successfull, 0 if not
         
         Executes the given prepared statement.*/
        virtual int executeStmt(void* preparedStatement);        
        
        /*! \brief finalizes and releases a prepared statement.  
         \param void* preparedStatement: a pointer to a prepared statement object
         
         \return returns 1 if successfull, 0 if not
         
         Finalizes and realeases resources allocated for a prepared statement.*/
        virtual int finalizePreparedStatement(void* preparedStatement);
        
        /*! \brief return maximum number of rows possible per prepared statement  
         \param DBDataSchema::Schema * thisSchema: a valid Schema which is needed, if the number of possible rows needs to be determined from the number of elements in thisSchema
         
         \return returns number of possible rows per prepared statement*/
        virtual int maxRowsPerStmt(DBDataSchema::Schema * thisSchema);

        /*! \brief retrievs (initiates retrieval) the complete specified table
         \param DBDataSchema::Schema * thisSchema: a valid Schema which directly corresponds to the table contents (ALL ROWS!)
         
         \return returns an initialised prepared statement for this query*/
        virtual void * initGetCompleteTable(DBDataSchema::Schema * thisSchema);

        /*! \brief move cursor to next row
         \param void* preparedStatement: a pointer to a prepared statement object that holds the result of this query
         
         \return returns 1 if successfull, 0 if end of table is reached*/
        virtual int getNextRow(DBDataSchema::Schema * thisSchema, void* thisData, void * preparedStatement);
    };
}
#endif
