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

#include "Reader.h"
#include "dbingestor_error.h"
#include <assert.h>
#include <stdio.h>

using namespace DBReader;
using namespace std;

Reader::Reader() {
    header = NULL;
    schema = NULL;
    readCount = 0;
}

Reader::~Reader() {

}

void Reader::openFile(string newFileName) {
    
}

void Reader::closeFile() {
    
}

void Reader::rewind() {
    
}

void Reader::skipHeader() {
    
}

HeaderReader * Reader::getHeader() {
	return header;
}

void Reader::setHeader(HeaderReader * newHeader) {
    header = newHeader;
}

DBDataSchema::Schema * Reader::getSchema() {
    return schema;
}

void Reader::setSchema(DBDataSchema::Schema * newSchema) {
    assert(newSchema != NULL);
    schema = newSchema;
}

void Reader::checkAssertions(DBDataSchema::DataObjDesc * thisItem, void* result) {
    //checking this assertion only once per row, is it alreads evaluated?
    if(thisItem->getAssertionEvaluated() == true) {
        return;
    }

    for(int i=0; i<thisItem->getNumAssertions(); i++) {
        DBAsserter::Asserter * currAsserter = thisItem->getAssertion(i);
        if(currAsserter->execute(thisItem->getDataObjDType(), result) == 0) {
            printf("Error in Assertion\n");
            printf("Assertion Number: %i\n", i);
            printf("Assertion Name: %s\n", currAsserter->getName().c_str());
            DBIngestor_error("DBIngestor: Error in assertion. Assertion failed.\n", this);
        }
    }

    thisItem->setAssertionEvaluated(true);
}

bool Reader::applyConversions(DBDataSchema::DataObjDesc * thisItem, void* result)  {
    int err;

    //checking this assertion only once per row, is it alreads evaluated?
    if(thisItem->getConversionEvaluated() == true) {
        return;
    }
    
    for(int i=0; i<thisItem->getNumConverters(); i++) {
        DBConverter::Converter * currConverter = thisItem->getConversion(i);
        
        //read values for variable converters
        if(currConverter->getNumParameters() > 0) {
            for(int j=0; j<currConverter->getNumParameters(); j++) {
                //we donot need to apply asserters here... they have already been checked
                getItemInRow(currConverter->getParameterDatObj(j), 0, 1, currConverter->getParameter(j));
                currConverter->setParameter(j, currConverter->getParameter(j));
            }
        }
        
        err = currConverter->execute(thisItem->getDataObjDType(), result);
        
        if(err == 0) {
            printf("Error in Conversion\n");
            printf("Conversion Number: %i\n", i);
            printf("In item: %s\n", thisItem->getDataObjName().c_str());
            printf("Assertion Name: %s\n", currConverter->getName().c_str());
            DBIngestor_error("DBIngestor: Error in conversion. Conversion returned an error.\n", this);
        }
        
        if(err == -1) {
            return true;
        }
    }

    thisItem->setConversionEvaluated(true);
    
    return false;
}

unsigned long long Reader::getReadCount() {

}