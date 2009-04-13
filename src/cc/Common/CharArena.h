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
#include <algorithm>

#include "Common/Logger.h"
#include <boost/static_assert.hpp>

namespace Hypertable {

struct DefaultPageAllocator {
  void *allocate(size_t sz) { return std::malloc(sz); }
  void deallocate(void *p) { std::free(p); }
};

/**
 * A simple/fast allocator to avoid individual deletes/frees
 * Good for usage patterns that just:
 * load, use and free the entire thing repeatedly.
 */
template <typename CharT = char, class PageAllocT = DefaultPageAllocator>
class PageArena {
private: // types
  enum { DEFAULT_PAGE_SIZE = 8192 }; // 8KB
  typedef PageArena<CharT, PageAllocT> SelfT;

  struct Page {
    Page *next_page;
    char *alloc_end;
    const char *page_end;
    char buf[1];

    Page(const char *end) : next_page(0), page_end(end) {
      alloc_end = buf;
    }

    size_t
    remain() { return page_end - alloc_end; }

    CharT *
    alloc(size_t sz) {
      assert(sz <= remain());
      char *start = alloc_end;
      alloc_end += sz;
      return (CharT *)start;
    }
  };

private: // data
  Page *m_cur_page;
  size_t m_used;        // total number of bytes allocated by users
  size_t m_page_limit;  // capacity in bytes of an empty page
  size_t m_page_size;   // page size in number of bytes
  size_t m_pages;       // number of pages allocated
  size_t m_total;       // total number of bytes occupied by pages
  PageAllocT m_allocator;

private: // helpers
  Page *
  alloc_page(size_t sz, bool prepend = true) {
    Page *page = (Page *) m_allocator.allocate(sz);
    HT_EXPECT(page, Error::BAD_MEMORY_ALLOCATION);
    new (page) Page((char *)page + sz);

    if (HT_LIKELY(prepend))
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

  // not pubicly copy construtable
  PageArena(const SelfT &copy) { operator=(copy); }

  // not publicly assignable; shallow copy for swap
  SelfT &operator=(const SelfT &x) {
    m_cur_page = x.m_cur_page;
    m_page_limit = x.m_page_limit;
    m_page_size = x.m_page_size;
    m_pages = x.m_pages;
    m_total = x.m_total;
    m_used = x.m_used;
    return *this;
  }

public: // API
  /** constructor */
  PageArena(size_t page_size = DEFAULT_PAGE_SIZE,
            const PageAllocT &alloc = PageAllocT())
    : m_cur_page(0), m_used(0), m_page_limit(0), m_page_size(page_size),
      m_pages(0), m_total(0), m_allocator(alloc) {
    BOOST_STATIC_ASSERT(sizeof(CharT) == 1);
    HT_ASSERT(page_size > sizeof(Page));
  }
  ~PageArena() { free(); }

  /** allocate sz bytes */
  CharT *
  alloc(size_t sz) {
    m_used += sz;

    if (!m_cur_page) {
      m_cur_page = alloc_page(m_page_size);
      m_page_limit = m_cur_page->remain();
    }

    // common case
    if (HT_LIKELY(sz <= m_cur_page->remain()))
      return m_cur_page->alloc(sz);

    // normal overflow
    if (sz <= m_page_limit && m_cur_page->remain() < m_page_limit / 2) {
      m_cur_page = alloc_page(m_page_size);
      return m_cur_page->alloc(sz);
    }
    // big enough objects to have their own page
    Page *p = alloc_page(sz + sizeof(Page) + 1, false);
    return p->alloc(sz);
  }

  /** allocate sz bytes */
  CharT *
  alloc_aligned(size_t sz) {
    size_t align_offset;

    if (!m_cur_page) {
      m_cur_page = alloc_page(m_page_size);
      m_page_limit = m_cur_page->remain();
    }

    if (sizeof(void *) == 8)
      align_offset = (8 - (size_t)(((uint64_t)m_cur_page->alloc_end) & 0x7)) & 0x7;
    else
      align_offset = (4 - (size_t)(((uint32_t)m_cur_page->alloc_end) & 0x3)) & 0x3;

    size_t adjusted_sz = sz + align_offset;

    // common case
    if (HT_LIKELY(adjusted_sz <= m_cur_page->remain())) {
      m_used += adjusted_sz;
      return m_cur_page->alloc(adjusted_sz) + align_offset;
    }

    m_used += sz;

    // normal overflow
    if (sz <= m_page_limit && m_cur_page->remain() < m_page_limit / 2) {
      m_cur_page = alloc_page(m_page_size);
      return m_cur_page->alloc(sz);
    }
    // big enough objects to have their own page
    Page *p = alloc_page(sz + sizeof(Page) + 1, false);
    return p->alloc(sz);
  }

  /** realloc for newsz bytes */
  CharT *
  realloc(void *p, size_t oldsz, size_t newsz) {
    HT_ASSERT(m_cur_page);
    // if we're the last one on the current page with enough space
    if ((char *)p + oldsz == m_cur_page->alloc_end
        && (char *)p + newsz  < m_cur_page->page_end) {
      m_cur_page->alloc_end = (char *)p + newsz;
      return (CharT *)p;
    }
    CharT *copy = alloc(newsz);
    memcpy(copy, p, newsz > oldsz ? oldsz : newsz);
    return copy;
  }

  /** duplicate a null terminated string s */
  CharT *
  dup(const char *s) {
    if (HT_UNLIKELY(!s))
      return NULL;

    size_t len = std::strlen(s) + 1;
    CharT *copy = alloc(len);
    memcpy(copy, s, len);
    return copy;
  }

  /** duplicate a buffer of size len */
  CharT *
  dup(const void *s, size_t len) {
    if (HT_UNLIKELY(!s))
      return NULL;

    CharT *copy = alloc(len);
    memcpy(copy, s, len);

    return copy;
  }

  /** free the whole arena */
  void
  free() {
    Page *page;

    while (m_cur_page) {
      page = m_cur_page;
      m_cur_page = m_cur_page->next_page;
      m_allocator.deallocate(page);
    }
    m_pages = m_total = m_used = 0;
  }

  void clear() { return free(); }

  /** swap with another allocator efficiently */
  void
  swap(SelfT &x) {
    SelfT tmp(*this);
    *this = x;
    x = tmp;
  }

  /** dump some allocator stats */
  std::ostream&
  dump_stat(std::ostream& out) const {
    out <<"pages="<< m_pages
      <<", total="<< m_total
      <<", used="<< m_used
      <<"("<< m_used * 100. / m_total
      <<"%)";
    return out;
  }

  /** stats accessors */
  size_t used() const { return m_used; }
  size_t pages() const { return m_pages; }
  size_t total() const { return m_total; }
};

typedef PageArena<> CharArena;
typedef PageArena<unsigned char> ByteArena;

template <typename CharT, class PageAlloc>
inline std::ostream&
operator<<(std::ostream& out, const PageArena<CharT, PageAlloc> &m) {
  return m.dump_stat(out);
}

} // namespace Hypertable

#endif // !HYPERTABLE_CHARARENA_H
