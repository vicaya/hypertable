/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#ifndef HYPERTABLE_DFSBROKER_FILE_DEVICE_H
#define HYPERTABLE_DFSBROKER_FILE_DEVICE_H

#include "Common/Compat.h"
#include "Common/StringExt.h"
#include "Common/StaticBuffer.h"
#include "Common/Error.h"

#include <boost/cstdint.hpp>               // intmax_t.
#include <boost/iostreams/categories.hpp>  // tags.
#include <boost/iostreams/detail/ios.hpp>  // openmode, seekdir, int types.
#include <boost/shared_ptr.hpp>

#include "Client.h"

namespace Hypertable { namespace DfsBroker {

  using namespace boost::iostreams;
  using namespace std;
  /**
   * These classes are intended to be Dfs equivalent of boost::iostreams::basic_file,
   * boost::iostreams::basic_file_source, boost::iostreams::basic_file_sink
   */

  // Forward declarations
  class FileSource;
  class FileSink;

  namespace {
    const uint32_t READAHEAD_BUFFER_SIZE = 1024*1024;
    const uint32_t OUTSTANDING_READS = 1;
  }

  class FileDevice {
  public:
    friend class FileSource;
    friend class FileSink;

    typedef char char_type;
    struct category : public device_tag, public closable_tag
      {};
    // Ctor
    FileDevice(ClientPtr &client, const String &filename,
               BOOST_IOS::openmode mode = BOOST_IOS::in);

    // open
    virtual void open(ClientPtr &client, const String &filename,
                      BOOST_IOS::openmode mode = BOOST_IOS::in);
    virtual bool is_open() const;

    // read
    virtual size_t read(char_type* dst, size_t amount);
    virtual size_t bytes_read();
    virtual size_t length();

    // write
    virtual size_t write(const char_type *src, size_t amount);
    virtual size_t bytes_written();

    // close
    virtual void close();

  private:
    struct impl {
      impl(ClientPtr &client, const String &filename,
           BOOST_IOS::openmode mode)
           : m_client(client), m_filename(filename), m_open(false),
             m_bytes_read(0), m_bytes_written(0), m_length(0)
      {
        // if file is opened for output then create if it doesn't exist
        if (mode & BOOST_IOS::in) {
          if (!m_client->exists(filename))
            HT_THROW(Error::FILE_NOT_FOUND, (String)"dfs://" + filename);
          m_length = m_client->length(filename);
          m_fd = m_client->open_buffered(filename, READAHEAD_BUFFER_SIZE, OUTSTANDING_READS);
        }
        else if (mode & BOOST_IOS::out) {
          m_fd = m_client->create(filename, true, -1 , -1, -1);
        }
        m_open = true;
      }

      ~impl() {
        close();
      }

      void close() {
         if (m_open) {
          m_client->close(m_fd);
          m_bytes_read = m_bytes_written = 0;
          m_open = false;
        }
      }

      size_t read(char_type *dst, size_t amount) {
        if (amount + m_bytes_read > m_length)
          amount = m_length - m_bytes_read;
        if (amount > 0 ) {
          size_t bytes_read = m_client->read(m_fd, (void *)dst, amount);
          m_bytes_read += bytes_read;
          return bytes_read;
        }
        else {
          return -1;
        }
      }

      size_t bytes_read() {
        return m_bytes_read;
      }

      size_t length() {
        return m_length;
      }

      size_t write(const char_type *dst, size_t amount) {
        StaticBuffer write_buffer((void *)dst, amount, false);
        size_t bytes_written =  m_client->append(m_fd, write_buffer);
        m_bytes_written += bytes_written;
        return bytes_written;
      }

      size_t bytes_written() {
        return m_bytes_written;
      }

      ClientPtr m_client;
      String m_filename;
      int m_fd;
      bool m_open;
      size_t m_bytes_read;
      size_t m_bytes_written;
      size_t m_length;
    };

    boost::shared_ptr<impl> pimpl_;
  };

  class FileSource : private FileDevice {
  public:
    typedef char char_type;
    struct category : source_tag, closable_tag
      {};

    using FileDevice::is_open;
    using FileDevice::close;
    using FileDevice::read;
    using FileDevice::bytes_read;
    using FileDevice::length;

    FileSource(ClientPtr &client, const String& filename)
      : FileDevice(client, filename, BOOST_IOS::in)
      {}

    void open(ClientPtr &client, const String &filename) {
      FileDevice::open(client, filename, BOOST_IOS::in);
    }
  };

  class FileSink : private FileDevice {
  public:
    typedef char char_type;
    struct category : sink_tag, closable_tag
      {};

    using FileDevice::is_open;
    using FileDevice::close;
    using FileDevice::write;
    using FileDevice::bytes_written;

    FileSink(ClientPtr &client, const String &filename)
      : FileDevice(client, filename, BOOST_IOS::out)
      {}

    void open(ClientPtr &client, const String &filename) {
      FileDevice::open(client, filename, BOOST_IOS::out);
    }
  };
}} // namespace Hypertable::DfsBroker
#endif // HYPERTABLE_DFSBROKER_FILE_DEVICE_H


