/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_CHARARENA_H
#define HYPERTABLE_CHARARENA_H

#include <cstdlib>
#include <iostream>
#include <cstring>
#include <cstddef>
#include <cassert>
#include <boost/noncopyable.hpp>

namespace Hypertable {

/**
 * A simple/fast allocator for null terminated char strings
 * Good for hash_map/map with char *keys
 */
class CharArena : boost::noncopyable {
private: // types
  enum {
    DEFAULT_PAGE_SZ = 4096      // number of chars
  };
  struct Page {
    Page *next_page;
    char *alloc_end;
    char *page_end;
    char buf[1];

    Page(size_t sz) : next_page(0) {
      alloc_end = buf;
      page_end = buf + sz - sizeof(Page);
    }

    size_t 
    remain() { return page_end - alloc_end; }

    char *
    alloc(size_t sz) {
      assert(sz <= remain());
      char *start = alloc_end;
      alloc_end += sz;
      return start;
    }
  };

private: // data
  Page *m_cur_page;
  size_t m_page_sz;
  size_t m_page_limit;
  size_t m_pages;
  size_t m_total;
  size_t m_alloced;

private: // helpers
  Page *
  alloc_page(size_t sz, bool prepend = true) {
    Page *page = (Page *) std::malloc(sz);

    assert(page);
    new (page) Page(sz);
    
    if (prepend)
      page->next_page = m_cur_page;
    else if (m_cur_page) {
      // insert after cur page, for big/full pages
      page->next_page = m_cur_page->next_page;
      m_cur_page->next_page = page;
    }
    else // don't leak it
      m_cur_page = page;

    ++m_pages;
    m_total += sz;

    return page;
  }

public: // API
  CharArena(size_t page_sz = DEFAULT_PAGE_SZ) :
            m_cur_page(0), m_page_sz(page_sz), m_pages(0),
            m_total(0), m_alloced(0) {
    assert(page_sz > sizeof(Page));
    m_cur_page = alloc_page(page_sz);
    m_page_limit = m_cur_page->remain();
  }
  ~CharArena() { free(); }

  char *
  alloc(size_t sz) {
    m_alloced += sz;

    if (sz > m_page_limit) {
      Page *p = alloc_page(sz + sizeof(Page) + 1, false);
      return p->alloc(sz);
    }
    if (!m_cur_page || sz > m_cur_page->remain()) {
      m_cur_page = alloc_page(m_page_sz);
    }
    return m_cur_page->alloc(sz);
  }

  char *
  dup(const char *s) {
    if (!s)
      return NULL;

    size_t len = std::strlen(s) + 1;
    char *copy = alloc(len);
    memcpy(copy, s, len);
    return copy;
  }

  void
  free() {
    Page *page;

    while (m_cur_page) {
      page = m_cur_page;
      m_cur_page = m_cur_page->next_page;
      std::free(page);
    }
    m_pages = m_total = m_alloced = 0;
  }

  std::ostream&
  dump_stat(std::ostream& out) const {
    out <<"pages="<< m_pages
      <<", bytes="<< m_total
      <<", alloc="<< m_alloced
      <<"("<< m_alloced * 100. / m_total
      <<"%)";
    return out;
  }
};

inline std::ostream&
operator<<(std::ostream& out, const CharArena &m) {
  return m.dump_stat(out);
}

} // namespace Hypertable

#endif // !HYPERTABLE_CHARARENA_H
