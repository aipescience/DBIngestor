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

#include <stdlib.h>
#include <stdio.h>
#include "dbingestor_error.h"
#include <assert.h>
#include "AsserterFactory.h"

////////////////////////////////////////////////////
// your custom asserter goes here:
////////////////////////////////////////////////////
#include "Asserters/assert_isnotinf.h"
#include "Asserters/assert_isnotnan.h"
#include "Asserters/assert_isnegative.h"
#include "Asserters/assert_ispositive.h"

using namespace DBAsserter;
using namespace std;

AsserterFactory::AsserterFactory() {
    registerAssert<assert_isnotinf>();
    registerAssert<assert_isnotnan>();
    registerAssert<assert_ispositive>();
    registerAssert<assert_isnegative>();
}

AsserterFactory::~AsserterFactory() {
    
}

Asserter * AsserterFactory::getAsserter(string name) {
    int i;
    for (i = 0; i < nameArray.size(); i++) {
        if(name.compare(nameArray.at(i)) == 0) {
            return asserterArray.at(i);
        }
    }
    
    printf("Error DBIngestor:\n");
    printf("Could not find asserter with the name: %s\n", name.c_str());
	DBIngestor_error("AsserterFactory: could not find the requested asserter...\n");
    
    return NULL;
}

template <class Type> void AsserterFactory::registerAssert(Asserter * thisAssert) {
    assert(thisAssert != NULL);
    
    nameArray.push_back(thisAssert->getName());
    asserterArray.push_back(thisAssert);
}