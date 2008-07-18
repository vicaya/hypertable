#include <Common/Compat.h>
#include <Common/System.h>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>

#include <hadoop/Pipes.hh>
#include <hadoop/TemplateFactory.hh>
#include <hadoop/StringUtils.hh>
#include <hadoop/SerialUtils.hh>
#include <Hypertable/Lib/Client.h>
#include "../TableReader.h"

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
  Hypertable::System::initialize(argv[0]);
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<TableMap, 
                              TableReduce, void, void, Mapreduce::TableReader,
                              TableWriter>());
}

