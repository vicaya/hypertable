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

class KeySpec {
private:
  Hypertable::KeySpec m_key;
  std::string m_key_str;
  std::string m_column_qualifier;
  std::string m_column_family;

public:
  KeySpec(std::string& key, std::string column_family, std::string column_qualifier)
  {
    m_key_str = key;
    m_column_qualifier = column_qualifier;
    m_column_family = column_family;

    m_key.row_len = m_key_str.size();
    if (m_key_str.size() > 0)
      m_key.row = m_key_str.c_str();

    m_key.column_family = column_family.c_str();

    m_key.column_qualifier_len = column_qualifier.size();
    if (column_qualifier.size() > 0)
      m_key.column_qualifier = column_qualifier.c_str();
  }

  Hypertable::KeySpec& compile() { return m_key; }
};

class TableMutator {
private:
  Hypertable::TableMutatorPtr m_mutator;

public:
  TableMutator() {}
  TableMutator(Hypertable::TableMutatorPtr mutator) : m_mutator(mutator)
  {
  }

  void set(std::string row, std::string column, std::string qualifier, const char *val, unsigned int value_len)
  {
    KeySpec k(row, column, qualifier);
    m_mutator->set(k.compile(), val, value_len);
  }

  void flush() { m_mutator->flush(); }
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

  TableMutator create_mutator(unsigned int timeout)
  {
    return TableMutator(m_table_ptr->create_mutator(timeout));
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

  void create_table(const std::string& name, const std::string& schema)
  {
    m_client->create_table(name, schema);
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
    .def("create_table", &Client::create_table)
    ;

  class_<Table>("Table", "Table representation")
    .def("create_scanner", &Table::create_scanner)
    .def("create_mutator", &Table::create_mutator)
    ;

  class_<TableMutator>("TableMutator", "Table Mutator class")
    .def("set", &TableMutator::set)
    .def("flush", &TableMutator::flush)
    ;

  class_<TableScanner>("TableScanner")
    .def("next", &TableScanner::next)
    ;

  class_<ScanSpecBuilder>("ScanSpecBuilder", "scan spec docstring")
    .def("add_row_interval", &ScanSpecBuilder::add_row_interval)
    .def("add_column", &ScanSpecBuilder::add_column)
    .def("clear", &ScanSpecBuilder::clear)
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

