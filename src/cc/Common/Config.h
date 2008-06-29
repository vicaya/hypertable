/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License.
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

#ifndef HYPERTABLE_CONFIG_H
#define HYPERTABLE_CONFIG_H

#include <boost/program_options.hpp>
#include "Common/System.h"

namespace Hypertable { namespace Config {
  using namespace boost::program_options;
  typedef options_description Desc;
  typedef positional_options_description PositionalDesc;
  typedef variables_map VarMap;

  /** stored option variables map singleton */
  extern VarMap varmap;

  /** cfg filename */
  extern String cfgfile;

  // Options description accessors
  Desc &description(const char *name = NULL);
  void description(const Desc &);

  // init helpers
  void init_default_options();

  void parse_args(int argc, char *argv[], const Desc *desc = NULL,
                  const Desc *hidden = NULL, const PositionalDesc *p = NULL);
  void init_default_actions();

  /**
   * Init with options
   * must be inlined to do proper version check
   *
   * @param argc - argument count (from main)
   * @param argv - argument array (from main)
   * @param desc - options description
   * @param hidden - hidden options description
   * @param p - positional options description
   */
  inline void init(int argc, char *argv[], const Desc *desc = NULL,
                   const Desc *hidden = NULL, const PositionalDesc *p = NULL) {
    System::initialize(argv[0]);
    if (desc) description(*desc);
    init_default_options();
    parse_args(argc, argv, desc, hidden, p);
    init_default_actions();
  }

}} // namespace Hypertable::Config

#endif // HYPERTABLE_CONFIG_H
