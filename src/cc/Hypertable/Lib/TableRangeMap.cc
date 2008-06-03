#include "TableRangeMap.h"

TableRangeMap::TableRangeMap(std::string TableName)
{
  m_client = new Client("mapreduce","/usr/local/0.9.0.5/conf/hypertable.cfg");
  
  m_user_table = m_client->open_table(TableName);
  m_meta_table = m_client->open_table("METADATA");

  m_user_table->get_identifier(&m_table_id);
}

std::vector<RangeLocationInfo> *TableRangeMap::getMap()
{
  char startrow[16];
  std::string endrow;
  ScanSpec meta_scan_spec;
  TableScannerPtr scanner;
  std::vector<RangeLocationInfo> *range_vector;
  Cell cell;
  
  snprintf(startrow, 16, "%u:", m_table_id.id);
  
  /* set up start row */
  meta_scan_spec.start_row = startrow;
  meta_scan_spec.start_row_inclusive = true;
  
  /* set up an end row for the range query */
  endrow = startrow;
  endrow.append(1,0xff);
  endrow.append(1,0xff);
  meta_scan_spec.end_row = endrow.c_str();
  meta_scan_spec.end_row_inclusive = true;
  
  /* get latest version only */
  meta_scan_spec.max_versions = 1;
  meta_scan_spec.return_deletes = false;
  
  /* select columns */
  meta_scan_spec.columns.clear();
  meta_scan_spec.columns.push_back("StartRow");
  meta_scan_spec.columns.push_back("Location");
  
  scanner = m_meta_table->create_scanner(meta_scan_spec);
  
  range_vector = new std::vector<RangeLocationInfo>();
  while (scanner->next(cell))
  {
    RangeLocationInfo range;
    char *v;
    /* 
      the first cell is a StartRow and the start of the range
      is encoded within the cell value, so extract it first.
    */
    v = new char[cell.value_len + 1];
    memcpy(v, cell.value, cell.value_len);
    v[cell.value_len] = 0;
    range.start_row = std::string(v);
    delete v;
    v = NULL;
    
    /*
      the last row of the range (not inclusive) is encoded within the row key
      so extract it next
      
      note below: I rely on the assumption that
      the row key is 0 terminated. the below code will
      cause problems otherwise
    */
    range.end_row = std::string(cell.row_key+2);
    
    /* the second cell is Location */
    scanner->next(cell);
    
    v = new char[cell.value_len+1];
    memcpy(v, cell.value, cell.value_len);
    v[cell.value_len] = 0;
    range.location = std::string(v);
    delete v;
    v = NULL;
    
    range_vector->push_back(range);
  }
  
  return range_vector;
}
