/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef HYPERTABLE_METADATA_H
#define HYPERTABLE_METADATA_H

#include <map>
#include <string>

#include <expat.h>

#include "Common/ByteString.h"
#include "Common/StringExt.h"

#include "Hypertable/RangeServer/RangeInfo.h"

using namespace hypertable;

namespace hypertable {

  typedef map<string, RangeInfoPtr> RangeInfoMapT;

  typedef map<string, RangeInfoMapT> TableMapT;

  class Metadata {

  public:
    Metadata(const char *fname=0);
    int GetRangeInfo(std::string &tableName, std::string &startRow, std::string &endRow, RangeInfoPtr &rangeInfoPtr);
    void AddRangeInfo(RangeInfoPtr &rangePtr);

    void Sync(const char *fname=0);
    void Display();

    static Metadata *NewInstance(const char *fname);
    friend Metadata *NewInstance(const char *fname);

    TableMapT     mTableMap;

  private:

    std::string   mFilename;

    static Metadata     *msMetadata;
    static RangeInfo    *msRange;
    static std::string   msCollectedText;

    static void StartElementHandler(void *userData, const XML_Char *name, const XML_Char **atts);
    static void EndElementHandler(void *userData, const XML_Char *name);
    static void CharacterDataHandler(void *userData, const XML_Char *s, int len);

  };

}

#endif // HYPERTABLE_METADATA_H
