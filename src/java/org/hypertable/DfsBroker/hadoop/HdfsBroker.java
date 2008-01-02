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

package org.hypertable.DfsBroker.hadoop;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;

/**
 * This is the actual HdfsBroker object that contains all of the application
 * logic.  It has a method for each of the request types (e.g. Open, Close,
 * Read, Write, etc.)  There is only one of these objects for each server
 * instance which carries out all of the requests from all connections.
 */
public class HdfsBroker {

    static final Logger log = Logger.getLogger("org.hypertable.DfsBroker.hadoop");

    protected static AtomicInteger msUniqueId = new AtomicInteger(0);

    public HdfsBroker(Comm comm, Properties props) throws IOException {
	String str;

	if (props.getProperty("verbose").equalsIgnoreCase("true"))
	    mVerbose = true;
	str = props.getProperty("HdfsBroker.fs.default.name");
	if (str == null) {
	    java.lang.System.err.println("error: 'HdfsBroker.fs.default.name' property not specified.");
	    java.lang.System.exit(1);
	}

	if (mVerbose) {
	    java.lang.System.out.println("HdfsBroker.Server.fs.default.name=" + str);
	}

	mConf.set("fs.default.name", str);
	mConf.set("dfs.client.buffer.dir", "/tmp");
	mConf.setInt("dfs.client.block.write.retries", 3);
	mFilesystem = FileSystem.get(mConf);
    }


    /**
     *
     */
    public OpenFileMap GetOpenFileMap() {
	return mOpenFileMap;
    }


    /**
     *
     */
    public void Open(ResponseCallbackOpen cb, String fileName, int bufferSize) {
	int fd;
	OpenFileData ofd;
	int error = Error.OK;

	try {

	    if (fileName.endsWith("/")) {
		log.severe("Unable to open file, bad filename: " + fileName);
		error = cb.error(Error.DFSBROKER_BAD_FILENAME, fileName);
		return;
	    }

	    fd = msUniqueId.incrementAndGet();

	    if (mVerbose)
		log.info("Opening file '" + fileName + "' bs=" + bufferSize + " handle = " + fd);

	    ofd = mOpenFileMap.Create(fd, cb.GetAddress());

	    ofd.is = mFilesystem.open(new Path(fileName));

	    // todo:  do something with bufferSize

	    error = cb.response(fd);

	}
	catch (FileNotFoundException e) {
	    log.info("File not found: " + fileName);
	    error = cb.error(Error.DFSBROKER_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception while opening file '" + fileName + "' - " + e.getMessage());
	    error = cb.error(Error.DFSBROKER_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to 'open' command - " + Error.GetText(error));
    }


    /**
     *
     */
    public void Close(ResponseCallback cb, int fd) {
	OpenFileData ofd;
	int error = Error.OK;

	try {

	    ofd = mOpenFileMap.Remove(fd);

	    if (mVerbose)
		log.info("Closing handle " + fd);

	    if (ofd == null) {
		error = Error.DFSBROKER_BAD_FILE_HANDLE;
		throw new IOException("Invalid file handle " + fd);
	    }

	    if (ofd.is != null)
		ofd.is.close();
	    if (ofd.os != null)
		ofd.os.close();

	    error = cb.response_ok();
	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(error, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Error sending CLOSE response back");
    }

    public void Create(ResponseCallbackCreate cb, String fileName, boolean overwrite,
		       int bufferSize, short replication, long blockSize) {
	int fd;
	OpenFileData ofd;
	int error = Error.OK;

	try {

	    if (fileName.endsWith("/")) {
		log.severe("Unable to open file, bad filename: " + fileName);
		error = cb.error(Error.DFSBROKER_BAD_FILENAME, fileName);
		return;
	    }

	    fd = msUniqueId.incrementAndGet();
	    ofd = mOpenFileMap.Create(fd, cb.GetAddress());

	    if (mVerbose)
		log.info("Creating file '" + fileName + "' handle = " + fd);

	    if (replication == -1)
		replication = mFilesystem.getDefaultReplication();

	    if (bufferSize == -1)
		bufferSize = mConf.getInt("io.file.buffer.size", 4096);

	    if (blockSize == -1)
		blockSize = mFilesystem.getDefaultBlockSize();

	    ofd.os = mFilesystem.create(new Path(fileName), overwrite, bufferSize, replication, blockSize);

	    error = cb.response(fd);
	}
	catch (FileNotFoundException e) {
	    log.info("File not found: " + fileName);
	    error = cb.error(Error.DFSBROKER_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception while creating file '" + fileName + "' - " + e.getMessage());
	    error = cb.error(Error.DFSBROKER_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to 'create' command - " + Error.GetText(error));
    }


    /**
     *
     */
    public void Length(ResponseCallbackLength cb, String fileName) {
	int error = Error.OK;
	long length;

	try {

	    if (mVerbose)
		log.info("Getting length of file '" + fileName);

	    length = mFilesystem.getLength(new Path(fileName));

	    error = cb.response(length);
	    
	}
	catch (FileNotFoundException e) {
	    log.info("File not found: " + fileName);
	    error = cb.error(Error.DFSBROKER_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception while getting length of file '" + fileName + "' - " + e.getMessage());
	    error = cb.error(Error.DFSBROKER_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to 'length' command - " + Error.GetText(error));
    }


    /**
     *
     */
    public void Mkdirs(ResponseCallback cb, String fileName) {
	int error = Error.OK;

	try {

	    if (mVerbose)
		log.info("Making directory '" + fileName + "'");

	    if (!mFilesystem.mkdirs(new Path(fileName)))
		throw new IOException("Problem creating directory '" + fileName + "'");

	    error = cb.response_ok();
	    
	}
	catch (FileNotFoundException e) {
	    log.info("File not found: " + fileName);
	    error = cb.error(Error.DFSBROKER_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception while making directory '" + fileName + "' - " + e.getMessage());
	    error = cb.error(Error.DFSBROKER_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to 'mkdirs' command - " + Error.GetText(error));
    }


    public void Read(ResponseCallbackRead cb, int fd, int amount) {
	int error = Error.OK;
	OpenFileData ofd;

	try {

	    if ((ofd = mOpenFileMap.Get(fd)) == null) {
		error = Error.DFSBROKER_BAD_FILE_HANDLE;
		throw new IOException("Invalid file handle " + fd);
	    }

	    /**
	    if (mVerbose)
		log.info("Reading " + amount + " bytes from fd=" + fd);
	    */

	    if (ofd.is == null)
		throw new IOException("File handle " + fd + " not open for reading");

	    long offset = ofd.is.getPos();

	    byte [] data = new byte [ amount ];
	    int nread = 0;

	    try {
		ofd.is.readFully(data, 0, data.length);
		nread = data.length;
	    }
	    catch (EOFException e) {
		nread = ofd.is.read(data, 0, data.length);
	    }

	    error = cb.response(offset, nread, data);

	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(error, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Error sending CLOSE response back");
    }

    public void Write(ResponseCallbackWrite cb, int fd, int amount, byte [] data) {
	int error = Error.OK;
	OpenFileData ofd;

	try {

	    /**
	    if (Global.verbose)
		log.info("Write request handle=" + fd + " amount=" + mAmount);
	    */

	    if ((ofd = mOpenFileMap.Get(fd)) == null) {
		error = Error.DFSBROKER_BAD_FILE_HANDLE;
		throw new IOException("Invalid file handle " + fd);
	    }

	    if (ofd.os == null)
		throw new IOException("File handle " + ofd + " not open for writing");

	    long offset = ofd.os.getPos();

	    ofd.os.write(data, 0, amount);

	    error = cb.response(offset, amount);
	}
	catch (IOException e) {
	    e.printStackTrace();
	    error = cb.error(error, e.getMessage());
	}
	catch (BufferUnderflowException e) {
	    e.printStackTrace();
	    error = cb.error(Error.PROTOCOL_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Error sending WRITE response back");
    }

    public void PositionRead(ResponseCallbackPositionRead cb, int fd, long offset, int amount) {
	int error = Error.OK;
	OpenFileData ofd;

	try {

	    if ((ofd = mOpenFileMap.Get(fd)) == null) {
		error = Error.DFSBROKER_BAD_FILE_HANDLE;
		throw new IOException("Invalid file handle " + fd);
	    }

	    /**
	    if (mVerbose)
		log.info("Reading " + amount + " bytes from fd=" + fd);
	    */

	    if (ofd.is == null)
		throw new IOException("File handle " + fd + " not open for reading");

	    byte [] data = new byte [ amount ];

	    ofd.is.seek(offset);

	    int nread = 0;

	    try {
		ofd.is.readFully(data, 0, data.length);
		nread = data.length;
	    }
	    catch (EOFException e) {
		nread = ofd.is.read(data, 0, data.length);
	    }

	    error = cb.response(offset, nread, data);

	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(error, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Error sending PREAD response back");
    }

    /**
     *
     */
    public void Remove(ResponseCallback cb, String fileName) {
	int error = Error.OK;

	try {

	    if (mVerbose)
		log.info("Removing file '" + fileName);

	    if (!mFilesystem.delete(new Path(fileName)))
		throw new IOException("Problem deleting file '" + fileName + "'");

	    error = cb.response_ok();

	}
	catch (FileNotFoundException e) {
	    log.info("File not found: " + fileName);
	    error = cb.error(Error.DFSBROKER_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception while removing file '" + fileName + "' - " + e.getMessage());
	    error = cb.error(Error.DFSBROKER_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to 'remove' command - " + Error.GetText(error));
    }

    public void Seek(ResponseCallback cb, int fd, long offset) {
	int error = Error.OK;
	OpenFileData ofd;

	try {

	    if ((ofd = mOpenFileMap.Get(fd)) == null) {
		error = Error.DFSBROKER_BAD_FILE_HANDLE;
		throw new IOException("Invalid file handle " + fd);
	    }

	    if (mVerbose)
		log.info("Seek request handle=" + fd + " offset=" + offset);

	    if (ofd.is == null)
		throw new IOException("File handle " + fd + " not open for reading");

	    ofd.is.seek(offset);

	    error = cb.response_ok();

	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(error, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Error sending SEEK response back");
    }

    /**
     *
     */
    public void Flush(ResponseCallback cb) {
	// todo: implement me!!
	log.info("Flush not implemented!");
	if (cb.response_ok() != Error.OK)
	    log.severe("Error sending SEEK response back");
    }

    /**
     *
     */
    public void Rmdir(ResponseCallback cb, String fileName) {
	int error = Error.OK;

	try {

	    if (mVerbose)
		log.info("Removing directory '" + fileName + "'");

	    if (!mFilesystem.delete(new Path(fileName)))
		throw new IOException("Problem deleting directory '" + fileName + "'");

	    error = cb.response_ok();
	    
	}
	catch (FileNotFoundException e) {
	    log.info("File not found: " + fileName);
	    error = cb.error(Error.DFSBROKER_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception while removing directory '" + fileName + "' - " + e.getMessage());
	    error = cb.error(Error.DFSBROKER_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to 'rmdir' command - " + Error.GetText(error));
    }


    /**
     *
     */
    public void Readdir(ResponseCallbackReaddir cb, String dirName) {
	int error = Error.OK;
	String pathStr;

	try {

	    if (mVerbose)
		log.info("Readdir('" + dirName + "')");

	    Path [] paths = mFilesystem.listPaths(new Path(dirName));

	    String [] listing = new String [ paths.length ];
	    for (int i=0; i<paths.length; i++) {
		pathStr = paths[i].toString();
		int lastSlash = pathStr.lastIndexOf('/');
		if (lastSlash == -1)
		    listing[i] = pathStr;
		else
		    listing[i] = pathStr.substring(lastSlash+1);
	    }

	    error = cb.response(listing);
	    
	}
	catch (FileNotFoundException e) {
	    log.info("File not found: " + dirName);
	    error = cb.error(Error.DFSBROKER_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception while reading directory '" + dirName + "' - " + e.getMessage());
	    error = cb.error(Error.DFSBROKER_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to 'readdir' command - " + Error.GetText(error));
    }



    private Configuration mConf = new Configuration();
    private FileSystem    mFilesystem;
    private boolean       mVerbose = false;
    private OpenFileMap   mOpenFileMap = new OpenFileMap();
}
