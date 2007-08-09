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

#include <cassert>
#include <iostream>
#include <fstream>
#include <string>

#include <expat.h>

extern "C" {
#include <stdio.h>
#include <unistd.h>
}

#include <boost/algorithm/string.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/StringExt.h"

#include "Metadata.h"

using namespace hypertable;
using namespace std;

Metadata    *Metadata::msMetadata = 0;
RangeInfo  *Metadata::msRange = 0;
std::string  Metadata::msCollectedText = "";

Metadata *Metadata::NewInstance(const char *fname) {
  char *data = 0;
  off_t len;

  XML_Parser parser = XML_ParserCreate("US-ASCII");

  XML_SetElementHandler(parser, &StartElementHandler, &EndElementHandler);
  XML_SetCharacterDataHandler(parser, &CharacterDataHandler);

  if ((data = FileUtils::FileToBuffer(fname, &len)) == 0)
    exit(1);

  msMetadata = new Metadata(fname);

  if (XML_Parse(parser, data, len, 1) == 0) {
    std::string errStr = (std::string)"Metadata Parse Error: " + (const char *)XML_ErrorString(XML_GetErrorCode(parser)) + " line " + (int)XML_GetCurrentLineNumber(parser) + ", offset " + (int)XML_GetCurrentByteIndex(parser);
    cerr << errStr << endl;
    exit(1);
  }

  XML_ParserFree(parser);  

  delete [] data;

  return msMetadata;
}



Metadata::Metadata(const char *fname) : mTableMap(), mFilename(fname) {
}



/**
 */
void Metadata::StartElementHandler(void *userData, const XML_Char *name, const XML_Char **atts) {

  if (!strcmp(name, "Metadata")) {
  }
  else if (!strcmp(name, "RangeInfo")) {
    assert(msRange == 0);
    msRange = new RangeInfo;
  }
  else if (!strcmp(name, "TableName")) {
    msCollectedText = "";
  }
  else if (!strcmp(name, "StartRow")) {
    msCollectedText = "";
  }
  else if (!strcmp(name, "EndRow")) {
    msCollectedText = "";
  }
  else if (!strcmp(name, "CellStore")) {
    msCollectedText = "";
  }
  else if (!strcmp(name, "LogDir")) {
    msCollectedText = "";
  }
  else if (!strcmp(name, "SplitLogDir")) {
    msCollectedText = "";
  }
  else if (!strcmp(name, "SplitPoint")) {
    msCollectedText = "";
  }
  else {
    cerr << "Unrecognized element : " << name << endl;
    exit(1);
  }

  return;
}



void Metadata::EndElementHandler(void *userData, const XML_Char *name) {

  if (!strcmp(name, "Metadata")) {
  }
  else if (!strcmp(name, "RangeInfo")) {
    assert(msRange != 0);
    RangeInfoPtr tmpPtr(msRange);
    msMetadata->AddRangeInfo(tmpPtr);
    msRange = 0;
  }
  else if (!strcmp(name, "TableName")) {
    assert(msRange != 0);
    boost::trim(msCollectedText);
    assert(msCollectedText.length() > 0);
    msRange->SetTableName(msCollectedText);
  }
  else if (!strcmp(name, "StartRow")) {
    string startRow;
    assert(msRange != 0);
    msRange->GetStartRow(startRow);
    assert(startRow == "");
    boost::trim(msCollectedText);
    if (msCollectedText.length() > 0)
      msRange->SetStartRow(msCollectedText);
  }
  else if (!strcmp(name, "EndRow")) {
    string endRow;
    assert(msRange != 0);
    msRange->GetEndRow(endRow);
    assert(endRow == "");
    boost::trim(msCollectedText);
    if (msCollectedText.length() > 0)
      msRange->SetEndRow(msCollectedText);
  }
  else if (!strcmp(name, "CellStore")) {
    assert(msRange != 0);
    boost::trim(msCollectedText);
    msRange->AddCellStore(msCollectedText);
  }
  else if (!strcmp(name, "LogDir")) {
    assert(msRange != 0);
    boost::trim(msCollectedText);
    msRange->SetLogDir(msCollectedText);
  }
  else if (!strcmp(name, "SplitLogDir")) {
    assert(msRange != 0);
    boost::trim(msCollectedText);
    msRange->SetSplitLogDir(msCollectedText);
  }
  else if (!strcmp(name, "SplitPoint")) {
    assert(msRange != 0);
    boost::trim(msCollectedText);
    msRange->SetSplitPoint(msCollectedText);
  }
  else {
    cerr << "Unrecognized element : " << name << endl;
    exit(1);
  }
  msCollectedText = "";
}



void Metadata::CharacterDataHandler(void *userData, const XML_Char *s, int len) {
  msCollectedText.assign(s, len);
}


int Metadata::GetRangeInfo(std::string &tableName, std::string &startRow, std::string &endRow, RangeInfoPtr &rangeInfoPtr) {
  TableMapT::iterator tmIter = mTableMap.find(tableName);
  if (tmIter == mTableMap.end())
    return Error::RANGESERVER_RANGE_NOT_FOUND;

  RangeInfoMapT::iterator iter = (*tmIter).second.find(startRow);
  if (iter == (*tmIter).second.end())
    return Error::RANGESERVER_RANGE_NOT_FOUND;

  std::string rangeEndRow;

  (*iter).second->GetEndRow(rangeEndRow);
  if (rangeEndRow != endRow)
    return Error::RANGESERVER_RANGE_MISMATCH;

  rangeInfoPtr = (*iter).second;
  return Error::OK;
}


void Metadata::AddRangeInfo(RangeInfoPtr &rangePtr) {
  string tableName, startRow;
  rangePtr->GetTableName(tableName);
  rangePtr->GetStartRow(startRow);
  assert(tableName != "");
  TableMapT::iterator iter = mTableMap.find(tableName);
  if (iter == mTableMap.end()) {
    mTableMap[tableName] = RangeInfoMapT();
    iter = mTableMap.find(tableName);
  }
  (*iter).second[startRow] = rangePtr;
}


void Metadata::Sync(const char *fname) {
  string tmpFile = (fname == 0) ? mFilename + ".tmp" : (string)fname + ".tmp";
  ofstream outfile(tmpFile.c_str());
  string tableName;
  string startRow;
  string endRow;
  string logDir;
  string splitLogDir;
  string splitPoint;
  vector<string> cellStoreVector;

  outfile << "<Metadata>" << endl;
  for (TableMapT::iterator tmIter = mTableMap.begin(); tmIter != mTableMap.end(); tmIter++) {

    for (RangeInfoMapT::iterator iter = (*tmIter).second.begin(); iter != (*tmIter).second.end(); iter++) {
      RangeInfoPtr rangePtr = (*iter).second;

      rangePtr->GetTableName(tableName);
      rangePtr->GetStartRow(startRow);
      rangePtr->GetEndRow(endRow);
      rangePtr->GetLogDir(logDir);
      rangePtr->GetSplitLogDir(splitLogDir);
      rangePtr->GetSplitPoint(splitPoint);
      cellStoreVector.clear();
      rangePtr->GetTables(cellStoreVector);

      outfile << "  <RangeInfo>" << endl;
      outfile << "    <TableName>" << tableName << "</TableName>" << endl;
      outfile << "    <StartRow>" << startRow << "</StartRow>" << endl;
      outfile << "    <EndRow>" << endRow << "</EndRow>" << endl;
      outfile << "    <LogDir>" << logDir << "</LogDir>" << endl;
      outfile << "    <SplitLogDir>" << splitLogDir << "</SplitLogDir>" << endl;
      outfile << "    <SplitPoint>" << splitPoint << "</SplitPoint>" << endl;
      for (size_t i=0; i<cellStoreVector.size(); i++)
	outfile << "    <CellStore>" << cellStoreVector[i] << "</CellStore>" << endl;
      outfile << "  </RangeInfo>" << endl;
    }
  }
  outfile << "</Metadata>" << endl;

  outfile.close();

  if (fname != 0)
    mFilename = fname;
  
  unlink(mFilename.c_str());

  if (rename(tmpFile.c_str(), mFilename.c_str()) != 0) {
    std::string errMsg = (const char *)"rename('" + tmpFile + "', '" + mFilename + "') failed";
    perror(errMsg.c_str());
    exit(1);
  }

}


void Metadata::Display() {
  string tableName;
  string startRow;
  string endRow;
  string logDir;
  string splitLogDir;
  string splitPoint;
  vector<string> cellStoreVector;

  for (TableMapT::iterator tmIter = mTableMap.begin(); tmIter != mTableMap.end(); tmIter++) {

    cout << endl;
    cout << "Ranges for table '" << (*tmIter).first << "'" << endl;

    for (RangeInfoMapT::iterator iter = (*tmIter).second.begin(); iter != (*tmIter).second.end(); iter++) {

      RangeInfoPtr rangePtr = (*iter).second;

      rangePtr->GetTableName(tableName);
      rangePtr->GetStartRow(startRow);
      rangePtr->GetEndRow(endRow);
      rangePtr->GetLogDir(logDir);
      rangePtr->GetSplitLogDir(splitLogDir);
      rangePtr->GetSplitPoint(splitPoint);
      rangePtr->GetTables(cellStoreVector);

      cout << "StartRow = \"" << startRow << "\"" << endl;
      cout << "EndRow = \"" << endRow << "\"" << endl;
      cout << "LogDir = \"" << logDir << "\"" << endl;
      cout << "SplitLogDir = \"" << splitLogDir << "\"" << endl;
      cout << "SplitPoint = \"" << splitPoint << "\"" << endl;
      for (size_t i=0; i<cellStoreVector.size(); i++)
	cout << "CellStore = " << cellStoreVector[i] << endl;
    }
    
  }
}

