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
#include <stdio.h>
#include <stdlib.h>

#include "HelloWorld_Reader.h"

namespace HelloWorld {
    HelloWorldReader::HelloWorldReader() {
        helloWorldString = "Hello World";
        counter = 0;
    }
    
    HelloWorldReader::~HelloWorldReader() {
        
    }
    
    int HelloWorldReader::getNextRow() {
        counter++;
        
        if(counter <= 10) {
            return 1;
        } else {
            return 0;
        }
    }
    
    bool HelloWorldReader::getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result) {
        //reroute constant items:
        if(thisItem->getIsConstItem() == true) {
            getConstItem(thisItem, 1, 1, result);
        } else if (thisItem->getIsHeaderItem() == true) {
            printf("We never told you to read headers...\n");
            exit(EXIT_FAILURE);
        } else {
            getDataItem(thisItem, result);
        }
        
        //check assertions
        checkAssertions(thisItem, result);
        
        //apply conversion
        applyConversions(thisItem, result);

        return 0;
    }
    
    void HelloWorldReader::getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result) {
        //check if this is "Col1" and if yes, point to the string containing "Hello world" into the result variable
        if(thisItem->getDataObjName().compare("Col1") == 0) {
            //allocate a string to hold "Hello world" - don't worry, this will be freed through the ingestor!
            char * charArray = (char*)malloc(128 * sizeof(char));
            
            strcpy(charArray, helloWorldString.c_str());
            
            *(char**)(result) = charArray;
        } else {
            //printf("Something went wrong...\n");
            //exit(EXIT_FAILURE);
        }
    }

    void HelloWorldReader::getConstItem(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result) {
        memcpy(result, thisItem->getConstData(), DBDataSchema::getByteLenOfDType(thisItem->getDataObjDType()));
    }
}
