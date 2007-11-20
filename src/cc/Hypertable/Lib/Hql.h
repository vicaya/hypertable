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
      void operator()(char const* /*str*/, char const* /*end*/) const {  }
    };

    struct set_table_name {
      set_table_name(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	state.table_name = std::string(str, end-str);
      }
      interpreter_state &state;
    };

    struct set_test {
      set_test(const char *label_) : label(label_) { }
      void operator()(char const *str, char const *end) const { 
	cout << label << " " << std::string(str, end-str) << endl;
      }
      const char *label;
    };

    struct create_column_family {
      create_column_family(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	state.cf = new Schema::ColumnFamily();
	state.cf->name = std::string(str, end-str);
	Schema::ColumnFamilyMapT::const_iterator iter = state.cf_map.find(state.cf->name);
	if (iter != state.cf_map.end())
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("Column family '") + state.cf->name + " multiply defined.");
	state.cf_map[state.cf->name] = state.cf;
      	cout << "create_column_family name=" << std::string(str, end-str) << endl;
      }
      interpreter_state &state;
    };

    struct set_column_family_max_versions {
      set_column_family_max_versions(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
        state.cf->cellLimit = (uint32_t)strtol(str, 0, 10);
      	cout << "set_column_family_max_versions" << endl;
      }
      interpreter_state &state;
    };

    struct set_ttl {
      set_ttl(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
	char *end_ptr;
	double ttl = strtod(str, &end_ptr);
	std::string unit_str = std::string(end_ptr, end-end_ptr);
	std::transform(unit_str.begin(), unit_str.end(), unit_str.begin(), ::tolower );
	if (unit_str == "months")
	  state.cf->expireTime = (time_t)(ttl * 2592000.0);
	else if (unit_str == "weeks")
	  state.cf->expireTime = (time_t)(ttl * 604800.0);
	else if (unit_str == "days")
	  state.cf->expireTime = (time_t)(ttl * 86400.0);
	else if (unit_str == "hours")
	  state.cf->expireTime = (time_t)(ttl * 3600.0);
	else if (unit_str == "minutes")
	  state.cf->expireTime = (time_t)(ttl * 60.0);
	else 
	  state.cf->expireTime = (time_t)ttl;
      	cout << "set_ttl (" << ttl << " " << unit_str << ")" << endl;
      }
      interpreter_state &state;
    };

    struct create_access_group {
      create_access_group(interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	std::string name = std::string(str, end-str);
	Schema::AccessGroupMapT::const_iterator iter = state.ag_map.find(name);
	if (iter != state.ag_map.end())
	  state.ag = (*iter).second;
	else {
	  state.ag = new Schema::AccessGroup();
	  state.ag->name = name;
	  state.cf_map[state.cf->name] = state.cf;  
	}
      	cout << "create_access_group name=" << std::string(str, end-str) << endl;
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
	    "access", "ACCESS", "GROUP", "group";

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
	  token_t WEEKS        = as_lower_d["weeks"];
	  token_t DAYS         = as_lower_d["days"];
	  token_t HOURS        = as_lower_d["hours"];
	  token_t MINUTES      = as_lower_d["minutes"];
	  token_t SECONDS      = as_lower_d["seconds"];
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
	    (  MONTHS | WEEKS | DAYS | HOURS | MINUTES | SECONDS )
	    ;

	  access_group_definition
	    = ACCESS >> GROUP >> identifier[create_access_group(self.state)] 
		     >> *( access_group_option ) >> !( LPAREN >> identifier >> *( identifier ) >> RPAREN )
	    ;

	  access_group_option
	    = in_memory_option
	    | blocksize_option
	    ;

	  in_memory_option
	    = IN_MEMORY
	    ;

	  blocksize_option
	    = BLOCKSIZE >> EQUAL >> uint_p
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
