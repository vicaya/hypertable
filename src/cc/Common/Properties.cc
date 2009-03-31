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

#include "Common/Compat.h"
#include "Common/Properties.h"
#include "Common/Logger.h"
#include "Common/Abi.h"

#include <errno.h>
#include <fstream>

using namespace Hypertable;
using namespace Property;
using namespace boost::program_options;

// Custom validator defintions
namespace boost { namespace program_options {

void validate(boost::any &v, const Strings &values, int64_t *, int) {
  validators::check_first_occurrence(v);
  const std::string &s = validators::get_single_string(values);
  char *last;
  int64_t result = strtoll(s.c_str(), &last, 0);

  if (s.c_str() == last)
    throw invalid_option_value(s);

  switch (*last) {
    case 'k':
    case 'K': result *= 1000LL;         break;
    case 'm':
    case 'M': result *= 1000000LL;      break;
    case 'g':
    case 'G': result *= 1000000000LL;   break;
    case '\0':                          break;
    default: throw invalid_option_value(s +": unknown suffix");
  }
  v = any(result);
}

void validate(boost::any &v, const Strings &values, double *, int) {
  validators::check_first_occurrence(v);
  const std::string &s = validators::get_single_string(values);
  char *last;
  double result = strtod(s.c_str(), &last);

  if (s.c_str() == last)
    throw invalid_option_value(s);

  switch (*last) {
    case 'k':
    case 'K': result *= 1000LL;         break;
    case 'm':
    case 'M': result *= 1000000LL;      break;
    case 'g':
    case 'G': result *= 1000000000LL;   break;
    case '\0':                          break;
    default: throw invalid_option_value(s +": unknown suffix");
  }
  v = any(result);
}

void validate(boost::any &v, const Strings &values, int32_t *, int) {
  validate(v, values, (int64_t *)0, 0);
  int64_t res = any_cast<int64_t>(v);

  if (res > INT32_MAX || res < INT32_MIN) {
    const std::string &s = validators::get_single_string(values);
    throw invalid_option_value(s +": number out of range of 32-bit integer");
  }
  v = any((int32_t)res);
}

void validate(boost::any &v, const Strings &values, uint16_t *, int) {
  validate(v, values, (int64_t *)0, 0);
  int64_t res = any_cast<int64_t>(v);

  if (res > UINT16_MAX) {
    const std::string &s = validators::get_single_string(values);
    throw invalid_option_value(s +": number out of range of 16-bit integer");
  }
  v = any((uint16_t)res);
}

}} // namespace boost::program_options


namespace {

void
parse(command_line_parser &parser, const PropertiesDesc &desc,
      variables_map &result, const PropertiesDesc *hidden,
      const PositionalDesc *p, bool allow_unregistered) {
  try {
    PropertiesDesc full(desc);

    if (hidden)
      full.add(*hidden);

    parser.options(full);

    if (p)
      parser.positional(*p);

#if BOOST_VERSION >= 103500
    if (allow_unregistered)
      store(parser.allow_unregistered().run(), result);
    else
      store(parser.run(), result);
#else
    store(parser.run(), result);
#endif
  }
  catch (std::exception &e) {
    HT_THROW(Error::CONFIG_BAD_ARGUMENT, e.what());
  }
}

} // local namespace


void
Properties::load(const String &fname, const PropertiesDesc &desc,
                 bool allow_unregistered) {
  m_need_alias_sync = true;

  try {
    std::ifstream in(fname.c_str());

    if (!in)
      HT_THROWF(Error::CONFIG_BAD_CFG_FILE, "%s", strerror(errno));


#if BOOST_VERSION >= 103500
    parsed_options parsed_opts = parse_config_file(in, desc, allow_unregistered);
    store(parsed_opts, m_map);
    for (size_t i=0; i<parsed_opts.options.size(); i++) {
      if (parsed_opts.options[i].unregistered && parsed_opts.options[i].string_key != "")
        m_map.insert(Map::value_type(parsed_opts.options[i].string_key, Value(parsed_opts.options[i].value[0], false)));
    }
#else
    store(parse_config_file(in, desc), m_map);
#endif

      }
  catch (std::exception &e) {
    HT_THROWF(Error::CONFIG_BAD_CFG_FILE, "%s: %s", fname.c_str(), e.what());
  }
}

void
Properties::parse_args(int argc, char *argv[], const PropertiesDesc &desc,
                       const PropertiesDesc *hidden, const PositionalDesc *p,
                       bool allow_unregistered) {
  // command_line_parser can't seem to handle argc = 0
  const char *dummy = "_";

  if (argc < 1) {
    argc = 1;
    argv = (char **)&dummy;
  }
  HT_TRY("parsing arguments",
    command_line_parser parser(argc, argv);
    parse(parser, desc, m_map, hidden, p, allow_unregistered));
}

void
Properties::parse_args(const std::vector<String> &args,
                       const PropertiesDesc &desc, const PropertiesDesc *hidden,
                       const PositionalDesc *p, bool allow_unregistered) {
  HT_TRY("parsing arguments",
    command_line_parser parser(args);
    parse(parser, desc, m_map, hidden, p, allow_unregistered));
}


// As of boost 1.38, notify will segfault if anything is added to
// the variables_map by means other than store, which is too limiting.
// So don't call notify after add/setting properties manually or die
void Properties::notify() {
  boost::program_options::notify(m_map);
}


void Properties::alias(const String &primary, const String &secondary,
                       bool overwrite) {
  if (overwrite)
    m_alias_map[primary] = secondary;
  else
    m_alias_map.insert(std::make_pair(primary, secondary));

  m_need_alias_sync = true;
}


void Properties::sync_aliases() {
  if (!m_need_alias_sync)
    return;

  foreach(const AliasMap::value_type &v, m_alias_map) {
    Map::iterator it1 = m_map.find(v.first);
    Map::iterator it2 = m_map.find(v.second);

    if (it1 != m_map.end()) {
      if (it2 == m_map.end())           // secondary missing
        m_map.insert(std::make_pair(v.second, (*it1).second));
      else if (!(*it1).second.defaulted())
        (*it2).second = (*it1).second;  // non-default primary trumps
      else if (!(*it2).second.defaulted())
        (*it1).second = (*it2).second;  // otherwise use non-default secondary
    }
    else if (it2 != m_map.end()) {      // primary missing
      m_map.insert(std::make_pair(v.first, (*it2).second));
    }
  }
  m_need_alias_sync = false;
}

// need to be updated when adding new option type
String Properties::to_str(const boost::any &v) {
  if (v.type() == typeid(String))
    return boost::any_cast<String>(v);

  if (v.type() == typeid(uint16_t))
    return format("%u", (unsigned)boost::any_cast<uint16_t>(v));

  if (v.type() == typeid(int32_t))
    return format("%d", boost::any_cast<int32_t>(v));

  if (v.type() == typeid(int64_t))
    return format("%llu", (Llu)boost::any_cast<int64_t>(v));

  if (v.type() == typeid(double))
    return format("%g", boost::any_cast<double>(v));

  if (v.type() == typeid(bool)) {
    bool bval = boost::any_cast<bool>(v);
    return bval ? "true" : "false";
  }
  if (v.type() == typeid(Strings)) {
    const Strings *strs = boost::any_cast<Strings>(&v);
    return format_list(*strs);
  }
  if (v.type() == typeid(Int64s)) {
    const Int64s *i64s = boost::any_cast<Int64s>(&v);
    return format_list(*i64s);
  }
  if (v.type() == typeid(Doubles)) {
    const Doubles *f64s = boost::any_cast<Doubles>(&v);
    return format_list(*f64s);
  }

  return format("value of type '%s'", demangle(v.type().name()).c_str());
}

void
Properties::print(std::ostream &out, bool include_default) {
  foreach(const Map::value_type &kv, m_map) {
    bool isdefault = kv.second.defaulted();

    if (include_default || !isdefault) {
      out << kv.first <<'='<< to_str(kv.second.value());

      if (isdefault)
        out <<" (default)";

      out << std::endl;
    }
  }
}
