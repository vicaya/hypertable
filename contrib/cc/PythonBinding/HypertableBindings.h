#ifndef _HAVE_HYPERTABLE_BINDINGS
#define _HAVE_HYPERTABLE_BINDINGS

#include <Common/Compat.h>
#include <Common/System.h>
#include <Hypertable/Lib/Client.h>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>
#include <map>
#include <boost/python.hpp>
#include <boost/python/class.hpp>
#include <boost/python/module.hpp>
#include <boost/python/borrowed.hpp>
#include <boost/python/def.hpp>

using namespace boost::python;
using Hypertable::ScanSpecBuilder;

class TableScanner {
private:
  Hypertable::TableScannerPtr m_scanner;
  
public:
  TableScanner() {}
  TableScanner(Hypertable::TableScannerPtr ptr) : m_scanner(ptr)
  {
  }
  
  bool next(Hypertable::Cell& cell)
  {
    return m_scanner->next(cell);
  }  
};

class Table {
private:
  Hypertable::TablePtr m_table_ptr;
  
public:
  Table()
  {
  }
  
  Table(Hypertable::TablePtr ptr) : m_table_ptr(ptr)
  {
  }
  
  TableScanner create_scanner(object pyspec)
  {
    extract<ScanSpecBuilder&> ex(pyspec);

    ScanSpecBuilder& spec = ex();
    return TableScanner(m_table_ptr->create_scanner(spec, 10));
  }
};

class Client {
private:
  Hypertable::ClientPtr m_client;
  
public:
  Client(const std::string& argv0)
  {
    m_client = new Hypertable::Client(Hypertable::System::locate_install_dir(argv0.c_str()));
  }
  
  Table open_table(const std::string& name)
  {
    Hypertable::TablePtr t = m_client->open_table(name); 
    return Table(t);
  }
};

std::string cell_value(Hypertable::Cell& c)
{
  char *tmp = new char[c.value_len+1];
  memcpy(tmp, c.value, c.value_len);
  tmp[c.value_len] = 0;

  std::string ret(tmp);
  delete[] tmp;
  return ret;
}


BOOST_PYTHON_MODULE(ht)
{
  class_<Client>("Client", init<const std::string& >())
    .def("open_table", &Client::open_table)
    ;
  
  class_<Table>("Table", "Table representation")
    .def("create_scanner", &Table::create_scanner)
    ;
  
  class_<TableScanner>("TableScanner")
    .def("next", &TableScanner::next)
    ;
    
  class_<ScanSpecBuilder>("ScanSpecBuilder", "scan spec docstring")
    .def("add_row_interval", &ScanSpecBuilder::add_row_interval)
    .def("add_column", &ScanSpecBuilder::add_column)
    ;

  class_<Hypertable::Cell>("Cell")
    .def_readonly("row_key", &Hypertable::Cell::row_key)
    .def_readonly("column_family", &Hypertable::Cell::column_family)
    .def_readonly("column_qualifier", &Hypertable::Cell::column_qualifier)
    .def_readonly("timestamp", &Hypertable::Cell::timestamp)
    .def("value", cell_value)
    .def_readonly("flag", &Hypertable::Cell::flag)
    ;
}

#endif

