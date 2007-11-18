/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

#include "Key.h"
#include "Metadata.h"

using namespace Hypertable;
using namespace std;

Metadata    *Metadata::ms_metadata = 0;
RangeInfo  *Metadata::ms_range = 0;
std::string  Metadata::ms_collected_text = "";

Metadata *Metadata::new_instance(const char *fname) {
  char *data = 0;
  off_t len;

  XML_Parser parser = XML_ParserCreate("US-ASCII");

  XML_SetElementHandler(parser, &start_element_handler, &end_element_handler);
  XML_SetCharacterDataHandler(parser, &character_data_handler);

  if ((data = FileUtils::file_to_buffer(fname, &len)) == 0)
    exit(1);

  ms_metadata = new Metadata(fname);

  if (XML_Parse(parser, data, len, 1) == 0) {
    std::string errStr = (std::string)"Metadata Parse Error: " + (const char *)XML_ErrorString(XML_GetErrorCode(parser)) + " line " + (int)XML_GetCurrentLineNumber(parser) + ", offset " + (int)XML_GetCurrentByteIndex(parser);
    cerr << errStr << endl;
    exit(1);
  }

  XML_ParserFree(parser);  

  delete [] data;

  return ms_metadata;
}



Metadata::Metadata(const char *fname) : m_table_map(), m_filename(fname) {
}



/**
 */
void Metadata::start_element_handler(void *userData, const XML_Char *name, const XML_Char **atts) {

  if (!strcmp(name, "Metadata")) {
  }
  else if (!strcmp(name, "RangeInfo")) {
    assert(ms_range == 0);
    ms_range = new RangeInfo;
  }
  else if (!strcmp(name, "TableName")) {
    ms_collected_text = "";
  }
  else if (!strcmp(name, "StartRow")) {
    ms_collected_text = "";
  }
  else if (!strcmp(name, "EndRow")) {
    ms_collected_text = "";
  }
  else if (!strcmp(name, "Location")) {
    ms_collected_text = "";
  }
  else if (!strcmp(name, "CellStore")) {
    ms_collected_text = "";
  }
  else if (!strcmp(name, "LogDir")) {
    ms_collected_text = "";
  }
  else if (!strcmp(name, "SplitLogDir")) {
    ms_collected_text = "";
  }
  else if (!strcmp(name, "SplitPoint")) {
    ms_collected_text = "";
  }
  else {
    cerr << "Unrecognized element : " << name << endl;
    exit(1);
  }

  return;
}



void Metadata::end_element_handler(void *userData, const XML_Char *name) {

  if (!strcmp(name, "Metadata")) {
  }
  else if (!strcmp(name, "RangeInfo")) {
    assert(ms_range != 0);
    RangeInfoPtr tmpPtr(ms_range);
    ms_metadata->add_range_info(tmpPtr);
    ms_range = 0;
  }
  else if (!strcmp(name, "TableName")) {
    assert(ms_range != 0);
    boost::trim(ms_collected_text);
    assert(ms_collected_text.length() > 0);
    ms_range->set_table_name(ms_collected_text);
  }
  else if (!strcmp(name, "StartRow")) {
    string startRow;
    assert(ms_range != 0);
    ms_range->get_start_row(startRow);
    assert(startRow == "");
    boost::trim(ms_collected_text);
    if (ms_collected_text.length() > 0)
      ms_range->set_start_row(ms_collected_text);
  }
  else if (!strcmp(name, "EndRow")) {
    string endRow;
    assert(ms_range != 0);
    ms_range->get_end_row(endRow);
    assert(endRow == "");
    boost::trim(ms_collected_text);
    if (ms_collected_text.length() > 0)
      ms_range->set_end_row(ms_collected_text);
    else {
      ms_collected_text = Key::END_ROW_MARKER;
      ms_range->set_end_row(ms_collected_text);
    }
  }
  else if (!strcmp(name, "Location")) {
    assert(ms_range != 0);
    boost::trim(ms_collected_text);
    ms_range->set_location(ms_collected_text);
  }
  else if (!strcmp(name, "CellStore")) {
    assert(ms_range != 0);
    boost::trim(ms_collected_text);
    ms_range->add_cell_store(ms_collected_text);
  }
  else if (!strcmp(name, "LogDir")) {
    assert(ms_range != 0);
    boost::trim(ms_collected_text);
    ms_range->set_log_dir(ms_collected_text);
  }
  else if (!strcmp(name, "SplitLogDir")) {
    assert(ms_range != 0);
    boost::trim(ms_collected_text);
    ms_range->set_split_log_dir(ms_collected_text);
  }
  else if (!strcmp(name, "SplitPoint")) {
    assert(ms_range != 0);
    boost::trim(ms_collected_text);
    ms_range->set_split_point(ms_collected_text);
  }
  else {
    cerr << "Unrecognized element : " << name << endl;
    exit(1);
  }
  ms_collected_text = "";
}



void Metadata::character_data_handler(void *userData, const XML_Char *s, int len) {
  ms_collected_text.assign(s, len);
}


int Metadata::get_range_info(std::string &tableName, std::string &endRow, RangeInfoPtr &rangeInfoPtr) {
  TableMapT::iterator tmIter = m_table_map.find(tableName);
  if (tmIter == m_table_map.end())
    return Error::RANGESERVER_RANGE_NOT_FOUND;

  RangeInfoMapT::iterator iter = (*tmIter).second.find(endRow);
  if (iter == (*tmIter).second.end())
    return Error::RANGESERVER_RANGE_NOT_FOUND;

  rangeInfoPtr = (*iter).second;
  return Error::OK;
}


void Metadata::add_range_info(RangeInfoPtr &rangePtr) {
  string tableName, endRow;
  rangePtr->get_table_name(tableName);
  rangePtr->get_end_row(endRow);
  assert(tableName != "");
  TableMapT::iterator iter = m_table_map.find(tableName);
  if (iter == m_table_map.end()) {
    m_table_map[tableName] = RangeInfoMapT();
    iter = m_table_map.find(tableName);
  }
  (*iter).second[endRow] = rangePtr;
}


void Metadata::sync(const char *fname) {
  string tmpFile = (fname == 0) ? m_filename + ".tmp" : (string)fname + ".tmp";
  ofstream outfile(tmpFile.c_str());
  string tableName;
  string startRow;
  string endRow;
  string location;
  string logDir;
  string splitLogDir;
  string splitPoint;
  vector<string> cellStoreVector;

  outfile << "<Metadata>" << endl;
  for (TableMapT::iterator tmIter = m_table_map.begin(); tmIter != m_table_map.end(); tmIter++) {

    for (RangeInfoMapT::iterator iter = (*tmIter).second.begin(); iter != (*tmIter).second.end(); iter++) {
      RangeInfoPtr rangePtr = (*iter).second;

      rangePtr->get_table_name(tableName);
      rangePtr->get_start_row(startRow);
      rangePtr->get_end_row(endRow);
      rangePtr->get_location(location);
      rangePtr->get_log_dir(logDir);
      rangePtr->get_split_log_dir(splitLogDir);
      rangePtr->get_split_point(splitPoint);
      cellStoreVector.clear();
      rangePtr->get_tables(cellStoreVector);

      outfile << "  <RangeInfo>" << endl;
      outfile << "    <TableName>" << tableName << "</TableName>" << endl;
      outfile << "    <StartRow>" << startRow << "</StartRow>" << endl;
      outfile << "    <EndRow>" << endRow << "</EndRow>" << endl;
      outfile << "    <Location>" << location << "</Location>" << endl;
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
    m_filename = fname;
  
  unlink(m_filename.c_str());

  if (rename(tmpFile.c_str(), m_filename.c_str()) != 0) {
    std::string errMsg = (const char *)"rename('" + tmpFile + "', '" + m_filename + "') failed";
    perror(errMsg.c_str());
    exit(1);
  }

}


void Metadata::display() {
  string tableName;
  string startRow;
  string endRow;
  string location;
  string logDir;
  string splitLogDir;
  string splitPoint;
  vector<string> cellStoreVector;

  for (TableMapT::iterator tmIter = m_table_map.begin(); tmIter != m_table_map.end(); tmIter++) {

    cout << endl;
    cout << "Ranges for table '" << (*tmIter).first << "'" << endl;

    for (RangeInfoMapT::iterator iter = (*tmIter).second.begin(); iter != (*tmIter).second.end(); iter++) {

      RangeInfoPtr rangePtr = (*iter).second;

      rangePtr->get_table_name(tableName);
      rangePtr->get_start_row(startRow);
      rangePtr->get_end_row(endRow);
      rangePtr->get_location(location);
      rangePtr->get_log_dir(logDir);
      rangePtr->get_split_log_dir(splitLogDir);
      rangePtr->get_split_point(splitPoint);
      rangePtr->get_tables(cellStoreVector);

      cout << "StartRow = \"" << startRow << "\"" << endl;
      cout << "EndRow = \"" << endRow << "\"" << endl;
      cout << "Location = \"" << location << "\"" << endl;
      cout << "LogDir = \"" << logDir << "\"" << endl;
      cout << "SplitLogDir = \"" << splitLogDir << "\"" << endl;
      cout << "SplitPoint = \"" << splitPoint << "\"" << endl;
      for (size_t i=0; i<cellStoreVector.size(); i++)
	cout << "CellStore = " << cellStoreVector[i] << endl;
    }
    
  }
}

