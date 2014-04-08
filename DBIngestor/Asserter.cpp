/*  
 *  Copyright (c) 2012 - 2014, Adrian M. Partl <apartl@aip.de>, 
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

#include "dbingestor_error.h"
#include <assert.h>
#include "Asserter.h"

using namespace DBAsserter;

Asserter::Asserter() {

}

Asserter::~Asserter() {
    
}

int Asserter::checkDType(DBDataSchema::DType thisDType) {
    for(int i=0; i < DTypeArray.size(); i++) {
        if(thisDType == DTypeArray.at(i)) {
            return 1;
        }
    }
    
    return 0;
}

std::string Asserter::getName() {
	return name;
}

void Asserter::setName(std::string newString) {
    name = newString;
}

void Asserter::setDTypeArray(DBDataSchema::DType *newDTypeArray, int size) {
    assert(newDTypeArray != NULL);
    assert(size >= 0);
    
    for(int i=0; i<size; i++) {
        DTypeArray.push_back(newDTypeArray[i]);
    }
}

int Asserter::setParameter(int parNum, void* value) {
	throw "Not yet implemented";
}

void* Asserter::getParameter(int parNum) {
	throw "Not yet implemented";
}

bool Asserter::execute(DBDataSchema::DType thisDType, void* value) {
	throw "Not yet implemented";
}
