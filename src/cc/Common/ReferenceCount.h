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
#ifndef HYPERTABLE_REFERENCECOUNT_H
#define HYPERTABLE_REFERENCECOUNT_H

#include <boost/intrusive_ptr.hpp>

#include "atomic.h"

namespace Hypertable {

  class ReferenceCount;

  void intrusive_ptr_add_ref(ReferenceCount *rc);
  void intrusive_ptr_release(ReferenceCount *rc);

  /**
   * This class is meant to be a base class for all classes that
   * want to be referenced by Boost intrusive pointers.  It contains
   * an atomic reference count variable and has the required Boost
   * intrusive_ptr functions, intrusive_ptr_add_ref and
   * intrusive_ptr_release, defined for it.
   */
  class ReferenceCount {
  public:
    ReferenceCount() { atomic_set(&refCount, 0); }
    virtual ~ReferenceCount() { return; }
    friend void intrusive_ptr_add_ref(ReferenceCount *rc);
    friend void intrusive_ptr_release(ReferenceCount *rc);
  private:
    atomic_t refCount;
  };

  /**
   * Atomically increments reference count.
   *
   * @param rc pointer to a ReferenceCount object
   */
  inline void intrusive_ptr_add_ref(ReferenceCount *rc) {
    atomic_inc_return(&rc->refCount);
  }

  /**
   * Atomically decrements reference count, deleting the
   * ReferenceCount object when the count reaches zero
   *
   * @param rc pointer to a ReferenceCount object
   */
  inline void intrusive_ptr_release(ReferenceCount *rc) {
    if (atomic_sub_and_test(1, &rc->refCount))
      delete rc;
  }

}

#endif // HYPERTABLE_REFERENCECOUNT_H
