/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "Common/Compat.h"

#include <cstring>
#include <iostream>

#include "Hypertable/Lib/LoadDataEscape.h"

namespace {

  const char *doc1 = "Twas the night before christmas\n"
    "and all through the house\n"
    "not a creature was sleeping\n"
    "not even a mouse\n";

  const char *doc2 = "Twas the night before christmas\n"
    "and all through the house\n"
    "not a creature was sleeping\n"
    "not even a mouse";

  const char *doc3 = "Twas\tthe\tnight\tbefore\tchristmas\n"
    "and\tall\tthrough\tthe\thouse\n"
    "not\ta\tcreature\twas\tsleeping\n"
    "not\teven\ta\tmouse\n";

  const char *doc4 = "\t\n\t\n\t\n\t\n";

  void validate_escaper(const char *doc, size_t len);

}

using namespace Hypertable;
using namespace std;

int main(int argc, char **argv) {

  validate_escaper(doc1, strlen(doc1));
  validate_escaper(doc2, strlen(doc2));
  validate_escaper(doc3, strlen(doc3));
  validate_escaper(doc4, strlen(doc4));

  return 0;
}

namespace {

  void validate_escaper(const char *doc, size_t len) {
    LoadDataEscape escaper;
    char *out_buf, *escape_buf;
    size_t out_len, escape_len;

    escaper.escape(doc, len, (const char **)&out_buf, &out_len);

    escape_buf = new char [ out_len + 1 ];
    memcpy((void *)escape_buf, out_buf, out_len);
    escape_len = out_len;
    escape_buf[escape_len] = 0;

    if (strchr(escape_buf, '\n') != 0) {
      cout << "'\\n' found in escaped buffer" << endl;
      exit(1);
    }

    if (strchr(escape_buf, '\t') != 0) {
      cout << "'\\t' found in escaped buffer" << endl;
      exit(1);
    }

    escaper.unescape(escape_buf, escape_len, (const char **)&out_buf, &out_len);

    if (out_len != len) {
      cout << "Unescaped buffer size " << out_len
           << " differs from original size " << len << endl;
      exit(1);
    }

    if (memcmp(doc, out_buf, len)) {
      cout << "Unescaped buffer differs from original" << endl;
      exit(1);
    }

  }

}
