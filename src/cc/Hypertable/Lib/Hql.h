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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_HQL_H
#define HYPERTABLE_HQL_H

//#define BOOST_SPIRIT_DEBUG  ///$$$ DEFINE THIS WHEN DEBUGGING $$$///
#include <boost/spirit/core.hpp>
#include <boost/spirit/symbols/symbols.hpp>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>

extern "C" {
#include <limits.h>
}

#include "Schema.h"

using namespace std;
using namespace boost::spirit;

#define display_string(str) // cerr << str << endl

namespace Hypertable {

  namespace HQL {

    const int COMMAND_CREATE_TABLE = 1;
   
    struct interpreter_state {
      int command;
      std::string table_name;
      Schema::ColumnFamily *cf;
      Schema::AccessGroup *ag;
      Schema::ColumnFamilyMapT cf_map;
      Schema::AccessGroupMapT ag_map;
    };

    struct set_command {
      set_command(interpreter_state &state, int cmd)
      { state.command = cmd; }
      void operator()(char const *, char const *) const { 
	display_string("set_command");
      }
    };

    struct set_table_name {
      set_table_name(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_table_name");
	state.table_name = std::string(str, end-str);
      }
      interpreter_state &state;
    };

#if 0
    struct set_test {
      set_test(const char *label_) : label(label_) { }
      void operator()(char const *str, char const *end) const { 
	cout << label << " " << std::string(str, end-str) << endl;
      }
      const char *label;
    };
#endif

    struct create_column_family {
      create_column_family(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_column_family");
	state.cf = new Schema::ColumnFamily();
	state.cf->name = std::string(str, end-str);
	Schema::ColumnFamilyMapT::const_iterator iter = state.cf_map.find(state.cf->name);
	if (iter != state.cf_map.end())
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("Column family '") + state.cf->name + " multiply defined.");
	state.cf_map[state.cf->name] = state.cf;
      }
      interpreter_state &state;
    };

    struct set_column_family_max_versions {
      set_column_family_max_versions(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_column_family_max_versions");
        state.cf->max_versions = (uint32_t)strtol(str, 0, 10);
      }
      interpreter_state &state;
    };

    struct set_ttl {
      set_ttl(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
	display_string("set_ttl");
	char *end_ptr;
	double ttl = strtod(str, &end_ptr);
	std::string unit_str = std::string(end_ptr, end-end_ptr);
	std::transform(unit_str.begin(), unit_str.end(), unit_str.begin(), ::tolower );
	if (unit_str == "months")
	  state.cf->ttl = (time_t)(ttl * 2592000.0);
	else if (unit_str == "weeks")
	  state.cf->ttl = (time_t)(ttl * 604800.0);
	else if (unit_str == "days")
	  state.cf->ttl = (time_t)(ttl * 86400.0);
	else if (unit_str == "hours")
	  state.cf->ttl = (time_t)(ttl * 3600.0);
	else if (unit_str == "minutes")
	  state.cf->ttl = (time_t)(ttl * 60.0);
	else 
	  state.cf->ttl = (time_t)ttl;
      }
      interpreter_state &state;
    };

    struct create_access_group {
      create_access_group(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("create_access_group");
	std::string name = std::string(str, end-str);
	Schema::AccessGroupMapT::const_iterator iter = state.ag_map.find(name);
	if (iter != state.ag_map.end())
	  state.ag = (*iter).second;
	else {
	  state.ag = new Schema::AccessGroup();
	  state.ag->name = name;
	  state.ag_map[state.ag->name] = state.ag;
	}
      }
      interpreter_state &state;
    };

    struct set_access_group_in_memory {
      set_access_group_in_memory(interpreter_state &state_) : state(state_) {  }
      void operator()(char const *, char const *) const { 
	display_string("set_access_group_in_memory");
	state.ag->in_memory=true;
      }
      interpreter_state &state;
    };

    struct set_access_group_blocksize {
      set_access_group_blocksize(interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &blocksize) const { 
	display_string("set_access_group_blocksize");
	state.ag->blocksize = blocksize;
      }
      interpreter_state &state;
    };

    struct add_column_family {
      add_column_family(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("add_column_family");
	std::string name = std::string(str, end-str);

	Schema::ColumnFamilyMapT::const_iterator iter = state.cf_map.find(name);
	if (iter == state.cf_map.end())
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("Access Group '") + state.ag->name + "' includes unknown column family '" + name + "'");
	if ((*iter).second->ag != "" && (*iter).second->ag != state.ag->name)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("Column family '") + name + "' can belong to only one access group");
	(*iter).second->ag = state.ag->name;
      }
      interpreter_state &state;
    };

    struct interpreter : public grammar<interpreter> {
      interpreter(interpreter_state &state_) : state(state_) {}

      template <typename ScannerT>
      struct definition {

	definition(interpreter const &self)  {
#ifdef BOOST_SPIRIT_DEBUG
	  debug(); // define the debug names
#endif

	  keywords =
	    "access", "ACCESS", "Access", "GROUP", "group", "Group";

	  /**
	   * OPERATORS
	   */
	  chlit<>     PLUS('+');
	  chlit<>     MINUS('-');
	  chlit<>     STAR('*');
	  chlit<>     SLASH('/');
	  chlit<>     COMMA(',');
	  chlit<>     SEMI(';');
	  chlit<>     COLON(':');
	  chlit<>     EQUAL('=');
	  chlit<>     LT('<');
	  strlit<>    LE("<=");
	  strlit<>    GE(">=");
	  chlit<>     GT('>');
	  chlit<>     LPAREN('(');
	  chlit<>     RPAREN(')');
	  chlit<>     LBRACK('[');
	  chlit<>     RBRACK(']');
	  chlit<>     POINTER('^');
	  chlit<>     DOT('.');
	  strlit<>    DOTDOT("..");

	  /**
	   * TOKENS
	   */
	  typedef inhibit_case<strlit<> > token_t;

	  token_t CREATE       = as_lower_d["create"];
	  token_t TABLE        = as_lower_d["table"];
	  token_t MAX_VERSIONS = as_lower_d["max_versions"];
	  token_t TTL          = as_lower_d["ttl"];
	  token_t MONTHS       = as_lower_d["months"];
	  token_t MONTH        = as_lower_d["month"];
	  token_t WEEKS        = as_lower_d["weeks"];
	  token_t WEEK         = as_lower_d["week"];
	  token_t DAYS         = as_lower_d["days"];
	  token_t DAY          = as_lower_d["day"];
	  token_t HOURS        = as_lower_d["hours"];
	  token_t HOUR         = as_lower_d["hour"];
	  token_t MINUTES      = as_lower_d["minutes"];
	  token_t MINUTE       = as_lower_d["minute"];
	  token_t SECONDS      = as_lower_d["seconds"];
	  token_t SECOND       = as_lower_d["second"];
	  token_t IN_MEMORY    = as_lower_d["in_memory"];
	  token_t BLOCKSIZE    = as_lower_d["blocksize"];
	  token_t ACCESS       = as_lower_d["access"];
	  token_t GROUP        = as_lower_d["group"];

	  /**
	   * Start grammar definition
	   */
	  identifier
	    = lexeme_d[
		(alpha_p >> *(alnum_p | '_'))
		- (keywords)
	    ];

	  string_literal
	    = lexeme_d[ chlit<>('\'') >>
			+( strlit<>("\'\'") | anychar_p-chlit<>('\'') ) >>
			chlit<>('\'') ];

	  statement
	    = create_table_statement[set_command(self.state, COMMAND_CREATE_TABLE)]
	    ;

	  create_table_statement
	    = CREATE >> TABLE >> identifier[set_table_name(self.state)]
		     >> !( create_definitions )
	    ;

	  create_definitions 
	    = LPAREN >> create_definition 
		     >> *( COMMA >> create_definition ) 
		     >> RPAREN
	    ;

	  create_definition
	    = column_definition
	      | access_group_definition
	    ;

	  column_definition
	    = identifier[create_column_family(self.state)] >> *( column_option )
	    ;

	  column_option
	    = max_versions_option
	    | ttl_option
	    ;

	  max_versions_option
	    = MAX_VERSIONS >> EQUAL >> lexeme_d[ (+digit_p)[set_column_family_max_versions(self.state)] ]
	    ;

	  ttl_option
	    = TTL >> EQUAL >> duration[set_ttl(self.state)]
	    ;

	  duration
	    = ureal_p >> 
	    (  MONTHS | MONTH | WEEKS | WEEK | DAYS | DAY | HOURS | HOUR | MINUTES | MINUTE | SECONDS | SECOND )
	    ;

	  access_group_definition
	    = ACCESS >> GROUP >> identifier[create_access_group(self.state)] 
		     >> *( access_group_option ) 
		     >> !( LPAREN >> identifier[add_column_family(self.state)]
		     >> *( identifier[add_column_family(self.state)] ) >> RPAREN )
	    ;

	  access_group_option
	    = in_memory_option[set_access_group_in_memory(self.state)]
	    | blocksize_option
	    ;

	  in_memory_option
	    = IN_MEMORY
	    ;

	  blocksize_option
	    = BLOCKSIZE >> EQUAL >> uint_p[set_access_group_blocksize(self.state)]
	    ;


	  /**
	   * End grammar definition
	   */

	}

#ifdef BOOST_SPIRIT_DEBUG
        void debug() {
	  BOOST_SPIRIT_DEBUG_RULE(column_definition);
	  BOOST_SPIRIT_DEBUG_RULE(column_option);
	  BOOST_SPIRIT_DEBUG_RULE(create_definition);
	  BOOST_SPIRIT_DEBUG_RULE(create_definitions);
	  BOOST_SPIRIT_DEBUG_RULE(create_table_statement);
	  BOOST_SPIRIT_DEBUG_RULE(duration);
	  BOOST_SPIRIT_DEBUG_RULE(identifier);
	  BOOST_SPIRIT_DEBUG_RULE(max_versions_option);
	  BOOST_SPIRIT_DEBUG_RULE(statement);
	  BOOST_SPIRIT_DEBUG_RULE(string_literal);
	  BOOST_SPIRIT_DEBUG_RULE(ttl_option);
	  BOOST_SPIRIT_DEBUG_RULE(access_group_definition);
	  BOOST_SPIRIT_DEBUG_RULE(access_group_option);
	  BOOST_SPIRIT_DEBUG_RULE(in_memory_option);
	  BOOST_SPIRIT_DEBUG_RULE(blocksize_option);
	}
#endif

	rule<ScannerT> const&
	start() const { return statement; }
	symbols<> keywords;
	rule<ScannerT>
	column_definition, column_option, create_definition, create_definitions,
	create_table_statement, duration, identifier, max_versions_option,
        statement, string_literal, ttl_option, access_group_definition,
        access_group_option, in_memory_option, blocksize_option;
      };

      interpreter_state &state;
    };
  }
}

#endif // HYPERTABLE_HQL_H
