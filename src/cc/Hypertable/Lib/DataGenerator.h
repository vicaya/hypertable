/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_DATAGENERATOR_H
#define HYPERTABLE_DATAGENERATOR_H

#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

extern "C" {
#include <limits.h>
#include <stdlib.h>
}

#include "Common/Config.h"
#include "Common/Random.h"
#include "Common/String.h"

#include "Cell.h"
#include "DataGeneratorRowComponent.h"
#include "DataGeneratorQualifier.h"
#include "DataGeneratorColumn.h"

using namespace Hypertable::Config;
using namespace std;

namespace Hypertable {

  struct DataGeneratorPolicy : Config::Policy {
    static void init_options() {
      allow_unregistered_options(true);
      file_desc().add_options()
        ("DataGenerator.MaxBytes", i64(),
         "Maximum number of bytes of key and value data to generate")
        ("DataGenerator.Seed", i32()->default_value(1234),
         "Pseudo-random number generator seed")
        ("rowkey.order", str()->default_value("random"), "Order in which to "
         "generate row keys.  Valid values are 'random' or 'ascending'")
        ("rowkey.component.<n>.type", i32(), "Type of row key component.  "
         "Valid values are 'string' and 'integer'")
        ("rowkey.component.<n>.format", str(), "printf-style format string "
         "for rendering key component.")
        ("rowkey.component.<n>.minimum", str(), "Minimum value.")
        ("rowkey.component.<n>.maximum", str(), "Maximum value.")
        ("<column>.qualifier.type", str(), "Type of qualifier")
        ("<column>.qualifier.size", i32(), "Size of qualifier")
        ("<column>.qualifier.charset", str(),
         "Set of characters to use when generating string qualifiers")
        ("<column>.value.size", i32(), "Size of value")
        ("<column>.value.source", i32(), "Source file to pull value data from")
        ;
      cmdline_hidden_desc().add(file_desc());
    }
  };

  class DataGenerator;

  /**
   * Provides an STL-style iterator on DataGenerator objects.
   */
  class DataGeneratorIterator : public iterator<forward_iterator_tag, Cell> {

    friend class DataGenerator;

  public:

    Cell& operator*() { return m_cell; }

    void next();
    DataGeneratorIterator& operator++();
    DataGeneratorIterator& operator++(int n);

    bool operator!=(const DataGeneratorIterator& other) const {
      return m_amount < other.m_amount;
    }

  private:
    DataGeneratorIterator(DataGenerator *generator);
    DataGeneratorIterator(int64_t amount) : m_generator(0), m_amount(amount) {  }
    DataGenerator *m_generator;
    std::vector<RowComponent *> m_row_components;
    std::vector<Column *> m_columns;
    Cell m_cell;
    int64_t m_amount;
    String m_row;
    int32_t  m_next_column;
  };


  /**
   *
   */
  class DataGenerator {

  public: 
    typedef DataGeneratorIterator iterator;
    friend class DataGeneratorIterator;

  public:
    DataGenerator();
    iterator begin() { Random::seed(m_seed); return DataGeneratorIterator(this); } 
    iterator end() { return DataGeneratorIterator(m_limit); } 

  protected:
    int64_t  m_limit;
    uint32_t m_seed;
    std::vector<RowComponentSpec> m_row_component_specs;
    std::vector<ColumnSpec> m_column_specs;
  };

}

#endif // HYPERTABLE_DATAGENERATOR_H
