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

#ifndef HYPERTABLE_PAGEARENA_H
#define HYPERTABLE_PAGEARENA_H

#include <cstdlib>
#include <iostream>
#include <cstring>
#include <cstddef>
#include <cassert>
#include <algorithm>

#include <boost/noncopyable.hpp>
#include <boost/static_assert.hpp>

#include "Common/Logger.h"

namespace Hypertable {

struct DefaultPageAllocator {
  void *allocate(size_t sz) { return std::malloc(sz); }
  void deallocate(void *p) { std::free(p); }
  void freed(size_t sz) { /* mostly for effcient bulk stat reporting */ }
};

/**
 * A simple/fast allocator to avoid individual deletes/frees
 * Good for usage patterns that just:
 * load, use and free the entire container repeatedly.
 */
template <typename CharT = char, class PageAllocatorT = DefaultPageAllocator>
class PageArena : boost::noncopyable {
 private: // types
  enum { DEFAULT_PAGE_SIZE = 8192 }; // 8KB
  typedef PageArena<CharT, PageAllocatorT> Self;

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

    CharT *
    alloc_down(size_t sz) {
      assert(sz <= remain());
      page_end -= sz;
      return (CharT *)page_end;
    }
  };

 private: // data
  Page *m_cur_page;
  size_t m_used;        // total number of bytes allocated by users
  size_t m_page_limit;  // capacity in bytes of an empty page
  size_t m_page_size;   // page size in number of bytes
  size_t m_pages;       // number of pages allocated
  size_t m_total;       // total number of bytes occupied by pages
  PageAllocatorT m_page_allocator;

 private: // helpers
  Page *
  alloc_page(size_t sz, bool prepend = true) {
    Page *page = (Page *) m_page_allocator.allocate(sz);
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

  inline void ensure_cur_page() {
    if (HT_UNLIKELY(!m_cur_page)) {
      m_cur_page = alloc_page(m_page_size);
      m_page_limit = m_cur_page->remain();
    }
  }

  inline bool is_normal_overflow(size_t sz) {
    return sz <= m_page_limit && m_cur_page->remain() < m_page_limit / 2;
  }

  CharT *alloc_big(size_t sz) {
    // big enough object to have their own page
    Page *p = alloc_page(sz + sizeof(Page), false);
    return p->alloc(sz);
  }

 public: // API
  /** constructor */
  PageArena(size_t page_size = DEFAULT_PAGE_SIZE,
            const PageAllocatorT &alloc = PageAllocatorT())
    : m_cur_page(0), m_used(0), m_page_limit(0), m_page_size(page_size),
      m_pages(0), m_total(0), m_page_allocator(alloc) {
    BOOST_STATIC_ASSERT(sizeof(CharT) == 1);
    HT_ASSERT(page_size > sizeof(Page));
  }
  ~PageArena() { free(); }

  size_t page_size() { return m_page_size; }

  void set_page_size(size_t sz) {
    HT_ASSERT(sz > sizeof(Page));
    m_page_size = sz;
  }

  /** allocate sz bytes */
  CharT *
  alloc(size_t sz) {
    m_used += sz;
    ensure_cur_page();

    // common case
    if (HT_LIKELY(sz <= m_cur_page->remain()))
      return m_cur_page->alloc(sz);

    if (is_normal_overflow(sz)) {
      m_cur_page = alloc_page(m_page_size);
      return m_cur_page->alloc(sz);
    }
    return alloc_big(sz);
  }

  /** allocate sz bytes */
  CharT *
  alloc_aligned(size_t sz) {
    m_used += sz;
    ensure_cur_page();

    size_t align_offset = get_align_offset(m_cur_page->alloc_end);
    size_t adjusted_sz = sz + align_offset;

    // common case
    if (HT_LIKELY(adjusted_sz <= m_cur_page->remain())) {
      m_used += align_offset;
      return m_cur_page->alloc(adjusted_sz) + align_offset;
    }
    if (is_normal_overflow(sz)) {
      m_cur_page = alloc_page(m_page_size);
      return m_cur_page->alloc(sz);
    }
    return alloc_big(sz);
  }

  /**
   * allocate from the end of the page.
   * - allow better packing/space saving for certain scenarios
   */
  CharT *
  alloc_down(size_t sz) {
    m_used += sz;
    ensure_cur_page();

    // common case
    if (HT_LIKELY(sz <= m_cur_page->remain()))
      return m_cur_page->alloc_down(sz);

    if (is_normal_overflow(sz)) {
      m_cur_page = alloc_page(m_page_size);
      return m_cur_page->alloc_down(sz);
    }
    return alloc_big(sz);
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
      m_page_allocator.deallocate(page);
    }
    m_page_allocator.freed(m_total);
    m_pages = m_total = m_used = 0;
  }

  /** swap with another allocator efficiently */
  void
  swap(Self &x) {
    std::swap(m_cur_page, x.m_cur_page);
    std::swap(m_page_limit, x.m_page_limit);
    std::swap(m_page_size, x.m_page_size);
    std::swap(m_pages, x.m_pages);
    std::swap(m_total, x.m_total);
    std::swap(m_used, x.m_used);
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

#endif // !HYPERTABLE_PAGEARENA_H
