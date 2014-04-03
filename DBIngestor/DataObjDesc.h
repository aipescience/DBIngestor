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


/*! \file DataObjDesc.h
    \brief Data Object Descriptor Interface
 
    Interface for a Data Object Descriptor which abstracts
    the data in the input file for reading
 */

#ifndef DBIngestor_DataObjDesc_h
#define DBIngestor_DataObjDesc_h

#include <string>
#include "DType.h"
#include "Asserter.h"
#include "Converter.h"

//guarding against circular inclusion here (needed and I can see no other design possibility...)
namespace DBConverter {
    class Converter;
}

namespace DBDataSchema {

    /*! \class DataObjDesc
        \brief DataObjDesc class interface
     
        Interface class which needs to be implemented by the
        developer which abstracts the data in the input file
        for reading.
     */
	class DataObjDesc {

	private:
        /*! \var int offsetId
            safes the index/offset where this data object is located
            in the file
         */
		int offsetId;
        
        /*! \var string dataObjName
         string describing the data object
         */
		std::string dataObjName;

        /*! \var bool isHeaderItem
         flag indicating if this data item is found in the data block or
         the header
         */
		bool isHeaderItem;

        /*! \var DType dataObjDType
         datatype of the data object
         */
		DType dataObjDType;
        
        /*! \var bool isConstData
         is true if constant data is assumed for this field. the constData needs
         to be allocated and setup properly if you use this
         */
        bool isConstData;

        /*! \var bool isStoreageConstData
         is true if constant data is assumed for this field. the constant data is
         different to the isConstData behaviour as such, that this constant also works
         as a store in memory. i.e. after the value for this data object has been 
         calculated, its result will be written back to the data object for further use.
         the constData needs to be allocated and setup properly if you use this
         */
        bool isStoreageConstData;

        /*! \var void * constData
         if the value is constant (e.g. header data), this saves the data
         which does not need to be read all the time
         */
		void * constData;

        /*! \var Converter conversion
         associated converter objects
         */
        std::vector<DBConverter::Converter*> conversions;

        /* \var vector<DBAsserter::Asserter*> assertions
         list of assotiated assertion objects
         */
        std::vector<DBAsserter::Asserter*> assertions;

        /* \var bool conversionsEvaluated
         true if conversions have already been evaluated for this item in current row
         false if not
         */
        bool conversionEvaluated;

        /* \var bool assertionsEvaluated
         true if assertions have already been evaluated for this item in current row
         false if not
         */
        bool assertionsEvaluated;

	public:
        DataObjDesc();
        
        virtual ~DataObjDesc();
        
		int getOffsetId();
	
		void setOffsetId(int newOffsetId);

        std::string getDataObjName();
	
		void setDataObjName(std::string newDataObjName);
	
		bool getIsHeaderItem();
	
		void setIsHeaderItem(bool newIsHeaderItem);

		bool getIsConstItem();
        
		void setIsConstItem(bool newIsConstItem, bool newIsStorage);

		bool getIsStorageItem();

		void * getConstData();
        
		void setConstData(void * newConstData);

        void updateConstData(void * newConstData);

        DBAsserter::Asserter * getAssertion(unsigned long index);
	
		void addAssertion(DBAsserter::Asserter * newAssertion);
        
        unsigned long getNumAssertions();
	
        DBConverter::Converter * getConversion(unsigned long index);
	
		void addConverter(DBConverter::Converter * newConverter);

        unsigned long getNumConverters();

		DType getDataObjDType();
	
		void setDataObjDType(DType newDataObjDType);

        bool getConversionEvaluated();

        bool setConversionEvaluated(bool value);

        bool getAssertionEvaluated();

        bool setAssertionEvaluated(bool value);

        bool resetForNextRow();
	};
}

#endif
