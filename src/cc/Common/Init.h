/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_INIT_H
#define HYPERTABLE_INIT_H

#include "Common/Config.h"
#include "Common/System.h"

namespace Hypertable { namespace Config {

  /**
   * Init with policy (with init_options (called before parse_args) and
   * init (called after parse_args) methods
   *
   * See parse_args for params
   */
  template <class PolicyT>
  inline void init_with_policy(int argc, char *argv[], const Desc *desc = 0) {
    try {
      System::initialize();

      ScopedRecLock lock(rec_mutex);
      properties = new Properties();

      if (desc)
        cmdline_desc(*desc);

      file_loaded = false;
      PolicyT::init_options();
      parse_args(argc, argv);
      PolicyT::init();
      sync_aliases(); // init can generate more aliases
      //properties->notify();

      if (get_bool("verbose"))
        properties->print(std::cout);
    }
    catch (Exception &e) {
      PolicyT::on_init_error(e);
    }
  }

  /**
   * Convenience function (more of a demo) to init with a list of polices
   * @see init_with
   */
  template <class PolicyListT>
  inline void init_with_policies(int argc, char *argv[], const Desc *desc = 0) {
    typedef typename Join<PolicyListT>::type Combined;
    init_with_policy<Combined>(argc, argv, desc);
  }

  /**
   * Init with default policy
   */
  inline void init(int argc, char *argv[], const Desc *desc = NULL) {
    init_with_policy<DefaultPolicy>(argc, argv, desc);
  }

}} // namespace Hypertable::Config

#endif /* HYPERTABLE_INIT_H */
