#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include "hadoop/SerialUtils.hh"
#include "Hypertable/Lib/Client.h"

class TableMap: public HadoopPipes::Mapper {
public:
  TableMap(HadoopPipes::MapContext& context) { }
  
  void map(HadoopPipes::MapContext& context)
  {
    context.emit("cells", "1");
  }
};

class TableReduce: public HadoopPipes::Reducer {
public:
  TableReduce(HadoopPipes::ReduceContext& context) { }
  void reduce(HadoopPipes::ReduceContext& context)
  {
    /* reduce stuff goes in here */
    int sum = 0;
    while (context.nextValue()) {
      sum += HadoopUtils::toInt(context.getInputValue());
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(sum));
  }
};

class TableReader: public HadoopPipes::RecordReader {
private:
  ClientPtr m_client;
  TablePtr m_table;
  TableScannerPtr m_scanner;
  
public:
  TableReader(HadoopPipes::MapContext& context)
  {    
    const HadoopPipes::JobConf *job = context.getJobConf();
    std::string tableName = job->get("hypertable.table.name");
    bool allColumns = job->getBoolean("hypertable.table.columns.all");
     
    m_client = new Client("MR", "/usr/local/0.9.0.6/conf/hypertable.cfg");

    m_table = m_client->open_table(tableName);
    
    ScanSpec scan_spec;
    
    std::string start_row;
    std::string end_row;
    std::string tablename;
    
    HadoopUtils::StringInStream stream(context.getInputSplit());
    HadoopUtils::deserializeString(tablename, stream);
    HadoopUtils::deserializeString(start_row, stream);
    HadoopUtils::deserializeString(end_row, stream);
    
    std::cout << "Scanning range:" << std::endl;
    std::cout << "\tstart row:" << start_row << std::endl;
    std::cout << "\tend row:" << end_row << std::endl;
    
    scan_spec.start_row = start_row.c_str();
    scan_spec.start_row_inclusive = true;
    
    scan_spec.end_row = end_row.c_str();
    scan_spec.end_row_inclusive = true;
    
    scan_spec.return_deletes = false;
    
    if (allColumns == false) {
      std::vector<string> columns;
      using namespace boost::algorithm;
      split(columns, job->get("hypertable.table.columns"), is_any_of(", "));
      BOOST_FOREACH(const std::string &c, columns) {
        scan_spec.columns.push_back(c.c_str());
      }
      m_scanner = m_table->create_scanner(scan_spec);
    } else {
      m_scanner = m_table->create_scanner(scan_spec);
    }
  }

  ~TableReader() {
  }

  virtual bool next(std::string& key, std::string& value) {
    Cell cell;
    
    if (m_scanner->next(cell)) {
      key = cell.row_key;
      char *s = new char[cell.value_len+1];
      memcpy(s, cell.value, cell.value_len);
      s[cell.value_len] = 0;
      value = s;
      delete s;
      
      std::cout << "key: " << key << std::endl;
      std::cout << "value: " << value << std::endl;
      return true;
    } else {
      return false;
    }
  }

  /**
   * The progress of the record reader through the split as a value between
   * 0.0 and 1.0.
   */
  virtual float getProgress() {
    /* calculate the progress by counting relative progress within the range */
    /* i.e. <rows so far>/<total rows within the range> */
    return 1.0f;
  }
};

class TableWriter: public HadoopPipes::RecordWriter {
public:
  TableWriter(HadoopPipes::ReduceContext& context) {
    const HadoopPipes::JobConf* job = context.getJobConf();
    int part = job->getInt("mapred.task.partition");
  }

  ~TableWriter() {
  }

  void emit(const std::string& key, const std::string& value) {
    std::cout << "key = " << key.c_str() << " value = " << value.c_str() << std::endl;
  }
};

int main(int argc, char *argv[]) {
  System::Initialize();
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<TableMap, 
                              TableReduce, void, void, TableReader,
                              TableWriter>());
}
