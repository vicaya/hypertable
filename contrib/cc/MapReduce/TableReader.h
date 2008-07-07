#ifndef MAPREDUCE_TABLE_RECORD_READER
#define MAPREDUCE_TABLE_RECORD_READER

#include <Common/Compat.h>
#include <Hypertable/Lib/Client.h>
#include <hadoop/Pipes.hh>
#include <hadoop/TemplateFactory.hh>
#include <hadoop/StringUtils.hh>
#include <hadoop/SerialUtils.hh>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <string>

namespace Mapreduce {
  using namespace Hypertable;

  class TableReader : public HadoopPipes::RecordReader {
  private:
    ClientPtr m_client;
    TablePtr m_table;
    TableScannerPtr m_scanner;

  public:
    TableReader(HadoopPipes::MapContext& context);
    ~TableReader();

    virtual bool next(std::string& key, std::string& value);
    virtual float getProgress() { return 1.0f; }
  };
}

#endif

