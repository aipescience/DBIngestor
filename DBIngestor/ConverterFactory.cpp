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
#include "ConverterFactory.h"

////////////////////////////////////////////////////
// your custom Converter goes here:
////////////////////////////////////////////////////
#include "Converters/convert_floor.h"
#include "Converters/convert_multiply.h"
#include "Converters/convert_add.h"
#include "Converters/convert_subtract.h"
#include "Converters/convert_divide.h"
#include "Converters/convert_madd.h"
#include "Converters/convert_isGt.h"
#include "Converters/convert_isEq.h"
#include "Converters/convert_isGe.h"
#include "Converters/convert_isLe.h"
#include "Converters/convert_isLt.h"
#include "Converters/convert_isNe.h"
#include "Converters/convert_ifthenelse.h"
#include "Converters/convert_concat.h"
#include "Converters/convert_3concat.h"
#include "Converters/convert_sqrt.h"
#include "Converters/convert_power.h"
#include "Converters/convert_periodicBound.h"
#include "Converters/convert_set.h"
#include "Converters/convert_postset.h"

using namespace DBConverter;
using namespace std;

ConverterFactory::ConverterFactory() {
    registerConvert<convert_floor>();
    registerConvert<convert_multiply>();
    registerConvert<convert_add>();
    registerConvert<convert_subtract>();
    registerConvert<convert_divide>();
    registerConvert<convert_madd>();
    registerConvert<convert_isgt>();
    registerConvert<convert_iseq>();
    registerConvert<convert_isge>();
    registerConvert<convert_isle>();
    registerConvert<convert_islt>();
    registerConvert<convert_isne>();
    registerConvert<convert_ifthenelse>();
    registerConvert<convert_concat>();
    registerConvert<convert_3concat>();
    registerConvert<convert_sqrt>();
    registerConvert<convert_power>();
    registerConvert<convert_periodicbound>();
    registerConvert<convert_set>();
    registerConvert<convert_postset>();
}

ConverterFactory::~ConverterFactory() {
    
}

Converter * ConverterFactory::getConverter(string name) {
    int i;
    for (i = 0; i < nameArray.size(); i++) {
        if(name.compare(nameArray.at(i)) == 0) {
            //if this is a converter that takes functional arguments,
            //instanciate a new one and return pointer
            if(ConverterArray.at(i)->getNumParameters() != 0) {
                return ConverterArray.at(i)->clone(); 
            } else {
                return ConverterArray.at(i);
            }
        }
    }
    
    printf("Error DBIngestor:\n");
    printf("Could not find Converter with the name: %s\n", name.c_str());
	DBIngestor_error("ConverterFactory: could not find the requested Converter...\n", NULL);
    
    return NULL;
}

template <class Type> void ConverterFactory::registerConvert(Converter * thisConvert) {
    assert(thisConvert != NULL);
    
    nameArray.push_back(thisConvert->getName());
    ConverterArray.push_back(thisConvert);
}
