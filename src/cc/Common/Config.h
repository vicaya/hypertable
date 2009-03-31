/** -*- C++ -*-
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

#include "Common/Mutex.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Meta.h"
#include "Common/Properties.h"

namespace Hypertable { namespace Config {
  using namespace Property;
  typedef PropertiesDesc Desc;

  /** global config mutex */
  extern RecMutex rec_mutex;

  /** config filename */
  extern String filename;

  /** whether a config file was loaded after init */
  extern bool file_loaded;

  /** stored option variables map singleton */
  extern PropertiesPtr properties;

  /**
   * check existence of config value
   */
  inline bool has(const String &name) {
    HT_ASSERT(properties);
    return properties->has(name);
  }

  /**
   * Check if a config value is defaulted
   */
  inline bool defaulted(const String &name) {
    HT_ASSERT(properties);
    return properties->defaulted(name);
  }

  /**
   * get config value
   */
  template <typename T>
  T get(const String &name) {
    HT_ASSERT(properties);
    return properties->get<T>(name);
  }

  /**
   * get config value with default
   */
  template <typename T>
  T get(const String &name, const T &default_value) {
    HT_ASSERT(properties);
    return properties->get<T>(name, default_value);
  }

  /** @see Properties */
  HT_PROPERTIES_ABBR_ACCESSORS

  // Options description accessors
  /**
   * Get the command line options description
   *
   * @param usage - optional usage string (first time)
   */
  Desc &cmdline_desc(const char *usage = NULL);

  /**
   * Set the command line options description
   */
  void cmdline_desc(const Desc &);

  /**
   * Get the command line hidden options description (for positional options)
   */
  Desc &cmdline_hidden_desc();

  /**
   * Get the command line positional options description
   */
  PositionalDesc &cmdline_positional_desc();

  /**
   * Get the config file options description
   */
  Desc &file_desc(const char *usage = NULL);

  /**
   * Set the config file options description
   */
  void file_desc(const Desc &);

  // init helpers - don't call directly unless...
  void init_default_options();
  void init_default_actions();

  /**
   * Interface and base of config policy
   */
  struct Policy {
    static void init_options() { }
    static void init() { }
    static void on_init_error(Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      std::exit(1);
    }
    static void cleanup() { }
  };

  /**
   * Default init policy
   */
  struct DefaultPolicy : Policy {
    static void init_options() { init_default_options(); }
    static void init() { init_default_actions(); }
  };

  /**
   * Helpers to compose init policies
   */
  template <class CarT, class CdrT>
  struct Cons {
    static void init_options() {
      CarT::init_options();
      CdrT::init_options();
    }
    static void init() {
      CarT::init();
      CdrT::init();
    }
    static void on_init_error(Exception &e) {
      CarT::on_init_error(e);
      CdrT::on_init_error(e);
    }
    static void cleanup() {
      CdrT::cleanup();  // in reverse order
      CarT::cleanup();
    }
  };

  struct NullPolicy { };

  // specialization for type list algorithms
  template <class PolicyT>
  struct Cons<NullPolicy, PolicyT> {
    static void init_options() { PolicyT::init_options(); }
    static void init() { PolicyT::init(); }
    static void on_init_error(Exception &e) { PolicyT::on_init_error(e); }
    static void cleanup() { PolicyT::cleanup(); }
  };

  // conversion from policy list to combined policy
  template <class PolicyListT>
  struct Join {
    typedef typename Meta::fold<PolicyListT, NullPolicy,
            Cons<Meta::_1, Meta::_2> >::type type;
  };


  /**
   * Init helper, has side effects (setting singletons etc.) unlike above
   */
  void parse_args(int argc, char *argv[]);

  /**
   * Parse config file. Throws CONFIG_BAD_CFG_FILE on error
   *
   * @param fname - config filename
   * @param desc - options description
   * @param allow_unregistered - allow unregistered options
   */
  void parse_file(const String &fname, const Desc &desc);


  /**
   * Setup command line option alias for config file option.
   * Typically used in policy init_options functions.
   * Command line option has higher priority.
   *
   * @param cmdline_opt - command line option name
   * @param file_opt - cfg file option name
   * @param overwrite - overwrite existing alias
   */
  void alias(const String &cmdline_opt, const String &file_opt,
             bool overwrite = false);

  /**
   * Sync alias values. Typically called after parse_* functions to
   * setup values in the config variable map.
   */
  void sync_aliases();

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

  /**
   * Toggle allow unregistered options
   */
  bool allow_unregistered_options(bool choice);
  bool allow_unregistered_options();

}} // namespace Hypertable::Config

#endif // HYPERTABLE_CONFIG_H
