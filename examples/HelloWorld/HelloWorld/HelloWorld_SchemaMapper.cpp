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

#include <iostream>

#include "HelloWorld_SchemaMapper.h"
#include <SchemaItem.h>
#include <DataObjDesc.h>
#include <DType.h>
#include <DBType.h>

using namespace std;
using namespace DBDataSchema;

namespace HelloWorld {
    
    HelloWorldSchemaMapper::HelloWorldSchemaMapper() {
        
    }

    HelloWorldSchemaMapper::HelloWorldSchemaMapper(DBAsserter::AsserterFactory * newAssertFac, DBConverter::ConverterFactory * newConvFac) {
        assertFac = newAssertFac;
        convFac = newConvFac;
    }

    HelloWorldSchemaMapper::~HelloWorldSchemaMapper() {
        
    }
    
    DBDataSchema::Schema * HelloWorldSchemaMapper::generateSchema(string dbName, string tblName) {
        DBDataSchema::Schema * returnSchema = new Schema();

        //set database and table name
        returnSchema->setDbName(dbName);
        returnSchema->setTableName(tblName);

        //setup schema items and add them to the schema
        
        //this is a string colum, where we will produce "Hello World"
        //first create the data object describing the input data:
        DataObjDesc * col1Obj = new DataObjDesc();
        col1Obj->setDataObjName("Col1");
        col1Obj->setDataObjDType(DT_STRING);
        col1Obj->setIsConstItem(false);
        col1Obj->setIsHeaderItem(false);
        
        //then describe the SchemaItem which represents the data on the server side
        SchemaItem * schemaItem1 = new SchemaItem();
        schemaItem1->setColumnName(EMPTY_SCHEMAITEM_NAME);
        schemaItem1->setColumnDBType(DBT_CHAR);
        schemaItem1->setDataDesc(col1Obj);
        
        //add schema item to the schema
        returnSchema->addItemToSchema(schemaItem1);

        
        //this is a constant column
        DataObjDesc * col2Obj = new DataObjDesc();
        col2Obj->setDataObjName("Col2");
        col2Obj->setDataObjDType(DT_UINT8);
        col2Obj->setIsConstItem(true);
        col2Obj->setIsHeaderItem(false);
        
        //assign constant value to this column. needs allocation first
        int64_t * constValue = (int64_t*)malloc(sizeof(int64_t));
        *constValue = 42;
        col2Obj->setConstData(constValue);
        
        //we now want to add a converter to this column        
        //assign a converter (we are only interested in half the answer to everything...)
        col2Obj->addConverter(convFac->getConverter("CONV_DIVIDE"));
        
        //register our constant divisor
        //this is a constant value that has been passed to the function. create a dataobject for it
        
        DataObjDesc * constConvObj = new DataObjDesc();
        constConvObj->setOffsetId(0);                         //this is the first parameter in the function - any additional parameter would have higher id
        constConvObj->setDataObjName("CONST_CONV_ITEM");      //some random name...
        constConvObj->setIsHeaderItem(0);                     
        constConvObj->setIsConstItem(1);                      //this is a constant...
        constConvObj->setDataObjDType(DT_REAL8);              //using a float here...
        
        //same as above when registering constants....
        double * constValue2 = (double*)malloc(sizeof(double));
        *constValue2 = 2.0;
        constConvObj->setConstData(constValue2);                            //assigning the constant value
        col2Obj->getConversion(0)->registerParameter(0, constConvObj);      //register this constant value with the 
                                                                            //first converter of this object (we only have one here)

        
        //then describe the SchemaItem which represents the data on the server side
        SchemaItem * schemaItem2 = new SchemaItem();
        schemaItem2->setColumnName("const");
        schemaItem2->setColumnDBType(DBT_UINTEGER);
        schemaItem2->setDataDesc(col2Obj);
        
        //add schema item to the schema
        returnSchema->addItemToSchema(schemaItem2);
       
        
        return returnSchema;
    }
}