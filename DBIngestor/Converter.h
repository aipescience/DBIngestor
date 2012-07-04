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


/*! \file Converter.h
 \brief Interface class for Converter functions
 
 Interface for Converter functions that can be used to validate
 the data
 */

#ifndef DBIngestor_Converter_h
#define DBIngestor_Converter_h

#include <string>
#include <vector>
#include "DType.h"
#include "DataObjDesc.h"

//guarding against circular inclusion here (needed and I can see no other design possibility...)
namespace DBDataSchema {
    class DataObjDesc;
}

namespace DBConverter {
    typedef struct {
        int varNum;
        int numTypes;
        DBDataSchema::DType * validTypeArray;
    } convFunctionParam;
    
    /*! \class Converter
     \brief Converter class interface
     
     Implement this class, if you want to build your own Converter object. 
     */
	class Converter {

	private:
        /*! \var string name
         identifier string for the Converter
         */
		std::string name;

        /*! \var vector<DBDataSchema::DType> DTypeArray
         supported DTypes that the Converter can handle
         */
        std::vector<DBDataSchema::DType> DTypeArray;
        
        /*! \var vector<convFunctionParam> functionParamArray
         holds the configuration of parameters this converter can takes
         */
        std::vector<convFunctionParam> functionParamArray;

        /*! \var vector<DBDataSchema::DataObjDesc*> dataObjArray
         holds the data objects that link to the apropriate data
         */
        std::vector<DBDataSchema::DataObjDesc*> dataObjArray;

        /*! \brief registers the type of parNum-th parameter for a specific instance of this converter
         \param int parNum: the number of the parameter to set
         \param DBDataSchema::DType parType: type of this parameter
         \return 1 if ok, 0 if not
         
         Converters with parameters need to be instanciated for each column the conversion is applied. This
         method registeres the specific type of a parameter with the converter, that will then be used to
         apply the conversion.*/
		int registerInternalParameter(int parNum, DBDataSchema::DType parType);
        
    protected:
        void setName(std::string newString);

        void setDTypeArray(DBDataSchema::DType * newDTypeArray, int size);
        
        void setFunctionParameters(convFunctionParam * paramArray, int size);

        /*! \var vector<DBDataSchema::DType> currFuncInstanceDTypes
         holds the data types to each parameter of a specific instance of the converter
         */
        std::vector<DBDataSchema::DType> currFuncInstanceDTypes;

        /*! \var vector<void*> functionValues
         holds the specific void pointers to the values needed for this converter
         */
        std::vector<void*> functionValues;

	public:
        Converter();
        
        virtual ~Converter();
        
        virtual Converter * clone() = 0;
        
        /*! \brief check whether DType is supporter by this Converter
         \param DBDataSchema::DType thisDType: dtype to test whether this converter works or not
         \return 1 if ok, 0 if not
         
         Checks whether the DType of this data object can be handled by this Converter. If not, throw an
         error.*/
		int checkDType(DBDataSchema::DType thisDType);
	
        /*! \brief registers the type of parNum-th parameter for a specific instance of this converter
         \param int parNum: the number of the parameter to set
         \param DBDataSchema::DataObjDesc * dataObj: pointer to a data object that links the parameter with the data in the data file
         \return 1 if ok, 0 if not
         
         Converters with parameters need to be instanciated for each column the conversion is applied. This
         method registeres the data object with a parameter of the converter, that will then be used to
         apply the conversion.*/
		int registerParameter(int parNum, DBDataSchema::DataObjDesc * dataObj);

        /*! \brief retrieves the registered parameters at position parNum
         \param int parNum: the number of the parameter to set
         \return link to DataObjDesc object
         
         Retrieves the DataObjDesc at position parNum of all the registered objects..*/
		DBDataSchema::DataObjDesc * getParameterDatObj(int parNum);

        /*! \brief if the specific implementation supports parameters, this function sets the specified parameter
         \param int parNum: the number of the parameter to set
         \param void* value: value to set
         \return 1 if ok, 0 if not
         
         Sets the parNum-th parameter as defined by the implementor, with a specified value. This needs to be
         implemented by the converter implementer.*/
		int setParameter(int parNum, void* value);
	
        /*! \brief if the specific implementation supports parameters, this function returns the specified parameter
         \param int parNum: the number of the parameter to set
         \return a void pointer to the parameter variable.
         
         Sets the parNum-th parameter as defined by the implementor, with a specified value. This needs to be
         implemented by the converter implementer.*/
		void* getParameter(int parNum);
	
        /*! \brief retrieves the number of parameters this converter has
         \return number of parameters in this converter
         
         Returns the number of parameters in the parameter array.*/
        unsigned long getNumParameters();

        /*! \brief executes the conversion for a specific data object and value
         \param DBDataSchema::DType thisDType: dtype to execute this convertion for
         \param void* value: pointer to the value that needs to be converted
         \return 1 if convertion was successfull, 0 if convertion is not successfull, -1 if this returns NULL
         
         Runs the converter on the specified value. Alters the value passed to the function and returns 1 on success.*/
		virtual bool execute(DBDataSchema::DType thisDType, void* value) = 0;

        virtual std::string getName();
    };
}

#endif
