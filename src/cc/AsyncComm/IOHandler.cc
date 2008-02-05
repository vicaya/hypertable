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


#include <cstdio>
#include <iostream>
using namespace std;

extern "C" {
#include <errno.h>
#if defined(__APPLE__)
#include <sys/event.h>
#endif
}

#include "Common/Logger.h"

#include "IOHandler.h"
#include "Reactor.h"
using namespace Hypertable;

#if defined(__linux__)

void IOHandler::add_poll_interest(int mode) {
  struct epoll_event event;

  m_poll_interest |= mode;

  memset(&event, 0, sizeof(struct epoll_event));
  event.data.ptr = this;
  event.events = EPOLLERR | EPOLLHUP;

  if (m_poll_interest & Reactor::READ_READY)
    event.events |= EPOLLIN;
  if (m_poll_interest & Reactor::WRITE_READY)
    event.events |= EPOLLOUT;

  if (epoll_ctl(m_reactor_ptr->pollFd, EPOLL_CTL_MOD, m_sd, &event) < 0) {
    /**
    HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_MOD, sd=%d) (mode=%x) : %s", 
		 m_reactor_ptr->pollFd, m_sd, mode, strerror(errno));
    *((int *)0) = 1;
    **/
  }
}



void IOHandler::remove_poll_interest(int mode) {
  struct epoll_event event;

  m_poll_interest &= ~mode;

  memset(&event, 0, sizeof(struct epoll_event));
  event.data.ptr = this;
  event.events = EPOLLERR | EPOLLHUP;

  if (m_poll_interest & Reactor::READ_READY)
    event.events |= EPOLLIN;
  if (m_poll_interest & Reactor::WRITE_READY)
    event.events |= EPOLLOUT;

  if (epoll_ctl(m_reactor_ptr->pollFd, EPOLL_CTL_MOD, m_sd, &event) < 0) {
    HT_ERRORF("epoll_ctl(EPOLL_CTL_MOD, sd=%d) (mode=%x) : %s", m_sd, mode, strerror(errno));
    exit(1);
  }
}



void IOHandler::display_event(struct epoll_event *event) {
  char buf[128];

  buf[0] = 0;
  if (event->events & EPOLLIN)
    strcat(buf, "EPOLLIN ");
  else if (event->events & EPOLLOUT)
    strcat(buf, "EPOLLOUT ");
  else if (event->events & EPOLLPRI)
    strcat(buf, "EPOLLPRI ");
  else if (event->events & EPOLLERR)
    strcat(buf, "EPOLLERR ");
  else if (event->events & EPOLLHUP)
    strcat(buf, "EPOLLHUP ");
  else if (event->events & EPOLLET)
    strcat(buf, "EPOLLET ");
  else if (event->events & EPOLLONESHOT)
    strcat(buf, "EPOLLONESHOT ");

  if (buf[0] == 0)
    sprintf(buf, "0x%x ", event->events);

  clog << "epoll events = " << buf << endl;

  return;
}



#elif defined(__APPLE__)

void IOHandler::add_poll_interest(int mode) {
  struct kevent events[2];
  int count=0;
  if (mode & Reactor::READ_READY) {
    EV_SET(&events[count], m_sd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, this);
    count++;
  }
  if (mode & Reactor::WRITE_READY) {
    EV_SET(&events[count], m_sd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, this);
    count++;
  }
  assert(count > 0);
    
  if (kevent(m_reactor_ptr->kQueue, events, count, 0, 0, 0) == -1) {
    HT_ERRORF("kevent(sd=%d) (mode=%x) : %s", m_sd, mode, strerror(errno));
    exit(1);
  }
  m_poll_interest |= mode;
}

void IOHandler::remove_poll_interest(int mode) {
  struct kevent devents[2];
  int count = 0;

  if (mode & Reactor::READ_READY) {
    EV_SET(&devents[count], m_sd, EVFILT_READ, EV_DELETE, 0, 0, 0);
    count++;
  }

  if (mode & Reactor::WRITE_READY) {
    EV_SET(&devents[count], m_sd, EVFILT_WRITE, EV_DELETE, 0, 0, 0);
    count++;
  }

  if (kevent(m_reactor_ptr->kQueue, devents, count, 0, 0, 0) == -1 && errno != ENOENT) {
    HT_ERRORF("kevent(sd=%d) (mode=%x) : %s", m_sd, mode, strerror(errno));
    exit(1);
  }
}


/**
 *
 */
void IOHandler::display_event(struct kevent *event) {

  clog << "kevent: ident=" << hex << (long)event->ident;

  switch (event->filter) {
  case EVFILT_READ:
    clog << ", EVFILT_READ, fflags=";
    if (event->fflags & NOTE_LOWAT)
      clog << "NOTE_LOWAT";
    else
      clog << event->fflags;
    break;
  case EVFILT_WRITE:
    clog << ", EVFILT_WRITE, fflags=";
    if (event->fflags & NOTE_LOWAT)
      clog << "NOTE_LOWAT";
    else
      clog << event->fflags;
    break;
  case EVFILT_AIO:
    clog << ", EVFILT_AIO, fflags=" << event->fflags;
    break;
  case EVFILT_VNODE:
    clog << ", EVFILT_VNODE, fflags={";
    if (event->fflags & NOTE_DELETE)
      clog << " NOTE_DELETE";
    if (event->fflags & NOTE_WRITE)
      clog << " NOTE_WRITE";
    if (event->fflags & NOTE_EXTEND)
      clog << " NOTE_EXTEND";
    if (event->fflags & NOTE_ATTRIB)
      clog << " NOTE_ATTRIB";
    if (event->fflags & NOTE_LINK)
      clog << " NOTE_LINK";
    if (event->fflags & NOTE_RENAME)
      clog << " NOTE_RENAME";
    if (event->fflags & NOTE_REVOKE)
      clog << " NOTE_REVOKE";
    clog << " }";
    break;
  case EVFILT_PROC:
    clog << ", EVFILT_VNODE, fflags={";
    if (event->fflags & NOTE_EXIT)
      clog << " NOTE_EXIT";
    if (event->fflags & NOTE_FORK)
      clog << " NOTE_FORK";
    if (event->fflags & NOTE_EXEC)
      clog << " NOTE_EXEC";
    if (event->fflags & NOTE_TRACK)
      clog << " NOTE_TRACK";
    if (event->fflags & NOTE_TRACKERR)
      clog << " NOTE_TRACKERR";
    if (event->fflags & NOTE_CHILD)
      clog << " NOTE_CHILD";
    clog << " pid=" << (event->flags & NOTE_PDATAMASK);
    break;
  case EVFILT_SIGNAL:
    clog << ", EVFILT_SIGNAL, fflags=" << event->fflags;
    break;
  case EVFILT_TIMER:
    clog << ", EVFILT_TIMER, fflags={";
    if (event->fflags & NOTE_SECONDS)
      clog << " NOTE_SECONDS";
    if (event->fflags & NOTE_USECONDS)
      clog << " NOTE_USECONDS";
    if (event->fflags & NOTE_NSECONDS)
      clog << " NOTE_NSECONDS";
    if (event->fflags & NOTE_ABSOLUTE)
      clog << " NOTE_ABSOLUTE";
    clog << " }";
    break;
  }

  if(event->flags != 0) {
    clog << ", flags=";
    if ((event->flags & EV_EOF) || (event->flags & EV_ERROR)) {
      clog << "{";
      if (event->flags & EV_EOF)
	clog << " EV_EOF";
      if (event->flags & EV_ERROR)
	clog << " EV_ERROR";
      clog << "}";
    }
    else
      clog << hex << event->flags;
  }
  clog << ", data=" << dec << (long)event->data << endl;
}

#endif
