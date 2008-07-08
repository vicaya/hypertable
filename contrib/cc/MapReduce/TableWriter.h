#include <Common/Compat.h>
#include <Common/System.h>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>

#include <hadoop/Pipes.hh>
#include <hadoop/TemplateFactory.hh>
#include <hadoop/StringUtils.hh>
#include <hadoop/SerialUtils.hh>
#include <Hypertable/Lib/Client.h>

namespace Mapreduce {
  using namespace Hypertable;

  class TableWriter: public HadoopPipes::RecordWriter {
  private:
    ClientPtr m_client;
    TableMutatorPtr m_mutator;

  public:
    TableWriter(HadoopPipes::ReduceContext& context) {
      const HadoopPipes::JobConf* job = context.getJobConf();
      int part = job->getInt("mapred.task.partition");

      std::string out_table = job->getBoolean("hypertable.table.output");
      m_client = new Client("MR", "conf/hypertable.cfg");

      m_table = m_client->open_table(out_table);
      m_mutator = m_table->create_mutator(50);
    }

    ~TableWriter() {
    }

    virtual void emit(const std::string& key, const std::string& value) {
      std::cout << "key = " << key.c_str() << " value = " << value.c_str() << std::endl;
      /*
       * Insert data here. 
       * Note: Think of configuration mechanism for that, so
       * users can run MR without writing their own Writers per job
       * m_mutator->set(...);
       */
    }
  };
}
