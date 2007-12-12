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

#ifndef HYPERSPACE_HANDLECALLBACK_H
#define HYPERSPACE_HANDLECALLBACK_H

#include <string>

#include "Common/ReferenceCount.h"

namespace Hyperspace {

  /**
   * The following event masks are ORed together and
   * passed in as the eventMask argument to Open()
   * to indicate which events should be reported to
   * the application for the opened handle.
   */
  enum {
    EVENT_MASK_ATTR_SET           = 0x0001,
    EVENT_MASK_ATTR_DEL           = 0x0002,
    EVENT_MASK_CHILD_NODE_ADDED   = 0x0004,
    EVENT_MASK_CHILD_NODE_REMOVED = 0x0008,
    EVENT_MASK_LOCK_ACQUIRED      = 0x0010,
    EVENT_MASK_LOCK_RELEASED      = 0x0020,
    EVENT_MASK_LOCK_GRANTED       = 0x0040
  };

  const char *event_mask_to_string(uint32_t mask);

  /**
   * A callback object derived from this class gets passed
   * into each Open() call.  Node state changes get reported
   * to the application via this callback.  Internally, this
   * object gets inspected to determine what events should be
   * delivered back to the application.
   */
  class HandleCallback : public Hypertable::ReferenceCount {
  public:
    /** Constructor.  Sets the event mask.
     *
     * @param event_mask mask of events to register
     */
    HandleCallback(uint32_t event_mask) : m_event_mask(event_mask) { return; }

    /** Invoked when an attribute gets set on the file associated
     * with the registered handle
     *
     * @param name the name of the attribute that was set
     */
    virtual void attr_set(std::string name) { return; }

    /** Invoked when an attribute gets deleted from the file associated
     * with the registered handle
     *
     * @param name the name of the attribute that was deleted
     */
    virtual void attr_del(std::string name) { return; }

    /** Invoked when a child node gets added to the directory associated
     * with the registered handle
     *
     * @param name the name of the attribute that was deleted
     */    
    virtual void child_node_added(std::string name) { return; }

    /** Invoked when an attribute gets deleted from the file associated
     * with the registered handle
     *
     * @param name the name of the attribute that was deleted
     */    
    virtual void child_node_removed(std::string name) { return; }

    /** Invoked when a lock gets acquired on the file associated with
     * the registered handle
     *
     * @param mode the mode in which the lock was acquired
     */    
    virtual void lock_acquired(uint32_t mode) { return; }

    /** Invoked when a lock gets released on the file associated with
     * the registered handle
     */    
    virtual void lock_released() { return; }

    /** Returns the event mask of this callback object
     *
     * @return the event mask
     */    
    int get_event_mask() { return m_event_mask; }

  protected:
    uint32_t m_event_mask;
  };
  typedef boost::intrusive_ptr<HandleCallback> HandleCallbackPtr;

}

#endif // HYPERSPACE_HANDLECALLBACK_H
