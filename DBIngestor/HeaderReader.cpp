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
#include "HeaderReader.h"
#include "dbingestor_error.h"
#include <assert.h>
#include "DType.h"
#include <stdio.h>

#include <boost/xpressive/xpressive.hpp>
#include <boost/algorithm/string.hpp>

using namespace DBReader;
using namespace std;

HeaderReader::HeaderReader() {
    headerContent = "";
    askUserToValidateRead = 1;
}

HeaderReader::~HeaderReader() {
    
}

void HeaderReader::parseHeader(FILE * filePointer) {
	DBIngestor_error("Not yet implemented");
}

void* HeaderReader::getItemBySearch(DBDataSchema::DataObjDesc * thisItem, string searchString) {
    //parse the header item if needed
    if(thisItem->getConstData() == NULL) {
        void * tmpResult = malloc(DBDataSchema::getByteLenOfDType(thisItem->getDataObjDType()));
        
        string regex = ".*";
        regex.append(searchString);
        regex.append("\\s*(.+?)\\s+.*");
        
        boost::xpressive::sregex rex = boost::xpressive::sregex::compile( regex );
        boost::xpressive::smatch what;
        if( regex_match(headerContent, what, rex) ) {
            string result = what[1].str();
            DBDataSchema::castStringToDType(result, thisItem->getDataObjDType(), tmpResult);
        } else {
            printf("Error DBIngestor:\n");
            printf("Could not find header item: %s\n", searchString.c_str());
            DBIngestor_error("HeaderReader: Header read error: Could not read the header item.\n");
        }
        
        //ask user for validation
        if(askUserToValidateRead == 1) {
            string answer;
            printf("\nPlease validate this header read:\n");
            printf("You wanted to extract: %s\n", searchString.c_str());
            printf("I have found: %s\n", what[1].str().c_str());
            printf("Please enter (Y/n): ");
            getline(cin, answer);
            
            if(answer.length() != 0 && answer.at(0) != 'Y' && answer.at(0) != 'y') {
                DBIngestor_error("HeaderReader: You did not approve my suggestion. Please adjust the structure file and try again.\n");
            }
        }
        
        thisItem->setConstData(tmpResult);
    }
    
    return thisItem->getConstData();
}

string HeaderReader::getHeaderContent() {
	return headerContent;
}

void HeaderReader::setHeaderContent(string newHeaderContent) {
    headerContent.clear();
    headerContent.append(newHeaderContent);
}

bool HeaderReader::getAskUserToValidateRead() {
    return askUserToValidateRead;
}

void HeaderReader::setAskUserToValidateRead(bool val) {
    assert(val == 0 || val == 1);
    
    askUserToValidateRead = val;
}
