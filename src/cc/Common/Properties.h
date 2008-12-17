/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_PROPERTIES_H
#define HYPERTABLE_PROPERTIES_H

#include <boost/program_options.hpp>

#include "Common/ReferenceCount.h"
#include "Common/Error.h"

// Required declarations for custom validators (otherwise no specializations)
namespace boost { namespace program_options {

typedef std::vector<std::string> Strings;
typedef std::vector<int64_t> Int64s;

extern void validate(boost::any &v, const Strings &values, int64_t *, int);
extern void validate(boost::any &v, const Strings &values, int32_t *, int);
extern void validate(boost::any &v, const Strings &values, uint16_t *, int);

// pre 1.35 vector<T> validate doesn't pickup user defined validate for T
#if BOOST_VERSION < 103500
template<typename T>
void validate(boost::any& v, const Strings &s, std::vector<T>*, int) {
  if (v.empty())
      v = boost::any(std::vector<T>());

  std::vector<T>* tv = boost::any_cast< std::vector<T> >(&v);
  assert(NULL != tv);

  for (unsigned i = 0; i < s.size(); ++i) {
    try {
      // so we can pick up user defined validate for T here
      boost::any a;
      Strings sv;
      sv.push_back(s[i]);
      validate(a, sv, (T*)0, 0);
      tv->push_back(boost::any_cast<T>(a));
    }
    catch(const bad_lexical_cast& /*e*/) {
      boost::throw_exception(invalid_option_value(s[i]));
    }
  }
}
#endif // < boost 1.35

}} // namespace boost::program_options

// convenience/abbreviated accessors
#define HT_PROPERTIES_ABBR_ACCESSORS \
  inline bool get_bool(const String &name) { return get<bool>(name); } \
  inline String get_str(const String &name) { return get<String>(name); } \
  inline Strings get_strs(const String &name) { return get<Strings>(name); } \
  inline uint16_t get_i16(const String &name) { return get<uint16_t>(name); } \
  inline int32_t get_i32(const String &name) { return get<int32_t>(name); } \
  inline int64_t get_i64(const String &name) { return get<int64_t>(name); } \
  inline Int64s get_i64s(const String &name) { return get<Int64s>(name); } \
  inline bool get_bool(const String &name, bool default_value) { \
    return get(name, default_value); \
  } \
  inline String get_str(const String &name, const String &default_value) { \
    return get(name, default_value); \
  } \
  inline Strings get_strs(const String &name, const Strings &default_value) { \
    return get(name, default_value); \
  } \
  inline uint16_t get_i16(const String &name, uint16_t default_value) { \
    return get(name, default_value); \
  } \
  inline int32_t get_i32(const String &name, int32_t default_value) { \
    return get(name, default_value); \
  } \
  inline int64_t get_i64(const String &name, int64_t default_value) { \
    return get(name, default_value); \
  } \
  inline Int64s get_i64s(const String &name, const Int64s &default_value) { \
    return get(name, default_value); \
  }

namespace Hypertable {

namespace Po = boost::program_options;

typedef std::vector<String> Strings;
typedef std::vector<int64_t> Int64s;

namespace Property {

const uint64_t K = 1000;
const uint64_t KiB = 1024;
const uint64_t M = K * 1000;
const uint64_t MiB = KiB * 1024;
const uint64_t G = M * 1000;
const uint64_t GiB = MiB * 1024;

// Abbrs for some common types
inline Po::typed_value<bool> *boo() { return Po::value<bool>(); }
inline Po::typed_value<String> *str() { return Po::value<String>(); }
inline Po::typed_value<Strings> *strs() { return Po::value<Strings>(); }
inline Po::typed_value<uint16_t> *i16() { return Po::value<uint16_t>(); }
inline Po::typed_value<int32_t> *i32() { return Po::value<int32_t>(); }
inline Po::typed_value<int64_t> *i64() { return Po::value<int64_t>(); }
inline Po::typed_value<Int64s> *i64s() { return Po::value<Int64s>(); }

} // namespace Property

typedef Po::options_description PropertiesDesc;
typedef Po::positional_options_description PositionalDesc;

class Properties : public ReferenceCount {
  typedef Po::variable_value Value;
  typedef Po::variables_map Map;
  typedef std::map<String, String> AliasMap;

public:
  Properties() : m_need_alias_sync(false) {}
  Properties(const String &filename, const PropertiesDesc &desc,
             bool allow_unregistered = false)
    : m_need_alias_sync(false) { load(filename, desc, allow_unregistered); }

  /**
   * load a property config file
   *
   * @param filename - name of the config file
   * @param desc - properties description
   * @param allow_unregistered - allow unregistered properties
   */
  void load(const String &filename, const PropertiesDesc &desc,
            bool allow_unregistered = false);

  /**
   * Parse arguments. Throws CONFIG_INVALID_ARGUMENT on error
   *
   * @param argc - argument count (from main)
   * @param argv - argument array (from main)
   * @param desc - options description
   * @param hidden - hidden options description
   * @param p - positional options description
   */
  void
  parse_args(int argc, char *argv[], const PropertiesDesc &desc,
             const PropertiesDesc *hidden = 0, const PositionalDesc *p = 0);

  /**
   * Same as above, except taking vector of strings as arguments
   */
  void
  parse_args(const std::vector<String> &args, const PropertiesDesc &desc,
             const PropertiesDesc *hidden = 0, const PositionalDesc *p = 0);

  /**
   * Get the value of option of type T. Throws if option is not defined.
   *
   * @param name - name of the option
   */
  template <typename T>
  T get(const String &name) const {
    try { return m_map[name].template as<T>(); }
    catch (std::exception &e) {
      HT_THROWF(Error::CONFIG_GET_ERROR, "getting value of %s: %s",
                name.c_str(), e.what());
    }
  }

  /**
   * Get the value of option of type T. Returns supplied default value if
   * not found. Try use the first form in usual cases and supply default
   * values in the config descriptions, as it validates the name and is less
   * error prone.
   *
   * @param name - name of the option
   * @param default_value - default value to return if not found
   */
  template <typename T>
  T get(const String &name, const T &default_value) const {
    try {
      Map::const_iterator it = m_map.find(name);

      if (it != m_map.end())
        return (*it).second.template as<T>();

      return default_value;
    }
    catch (std::exception &e) {
      HT_THROWF(Error::CONFIG_GET_ERROR, "getting value of %s: %s",
                name.c_str(), e.what());
    }
  }

  /**
   * Check whether a property have default value
   *
   * @param name - name of the property
   * @return true if the value is default
   */
  bool defaulted(const String &name) const {
    return m_map[name].defaulted();
  }

  bool has(const String &name) const { return m_map.count(name); }

  HT_PROPERTIES_ABBR_ACCESSORS

  /**
   * Add property to the map
   *
   * @param name - name/key of the property
   * @param v - value of the property
   * @param defaulted - whether the value is default
   */
  std::pair<Map::iterator, bool>
  add(const String &name, const boost::any &v, bool defaulted = false) {
    m_need_alias_sync = true;
    return m_map.insert(Map::value_type(name, Value(v, defaulted)));
  }

  /**
   * Set a property in the map, create if not found
   *
   * @param name - name/key of the property
   * @param v - value of the property
   * @param defaulted - whether the value is default
   */
  void set(const String &name, const boost::any &v, bool defaulted = false) {
    std::pair<Map::iterator, bool> r = add(name, v, defaulted);

    if (!r.second)
      (*r.first).second = Value(v, defaulted);
  }

  /**
   * Setup an property alias. Primary has higher priority, meaning when
   * aliases are sync'ed primary value can override secondary value
   *
   * @param primary - primary property name
   * @param secondary - secondary property name
   * @param overwrite - whether to overwrite existing alias
   */
  void alias(const String &primary, const String &secondary,
             bool overwrite = false);

  /**
   * Sync alias values. So properties that are aliases to each other
   * have the same value. Value priority: primary non-default >
   * secondary non-default > primary default > secondary default
   */
  void sync_aliases();

  /**
   * Print config key-value map
   *
   * @param out - output stream
   * @param include_default - including default values or not
   */
  void print(std::ostream &out, bool include_default = false);

  /**
   * Helper to print boost::any used by property values
   */
  static String to_str(const boost::any &);

private:
  bool m_need_alias_sync;
  Map m_map;
  AliasMap m_alias_map;
};

typedef intrusive_ptr<Properties> PropertiesPtr;

/**
 * Convenient help class for access parts of properties
 */
class SubProperties {
public:
  SubProperties(PropertiesPtr &props, const String &prefix)
    : m_props(props), m_prefix(prefix) { }

  String full_name(const String &name) const { return m_prefix + name; }

  template <typename T>
  T get(const String &name) const {
    return m_props->get<T>(full_name(name));
  }

  template <typename T>
  T get(const String &name, const T &default_value) const {
    return m_props->get(full_name(name), default_value);
  }

  bool defaulted(const String &name) const {
    return m_props->defaulted(full_name(name));
  }

  bool has(const String &name) const {
    return m_props->has(full_name(name));
  }

  HT_PROPERTIES_ABBR_ACCESSORS

private:
  PropertiesPtr m_props;
  String m_prefix;
};

} // namespace Hypertable

#endif // HYPERTABLE_PROPERTIES_H
