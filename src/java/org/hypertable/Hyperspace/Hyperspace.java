/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.hypertable.Hyperspace;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Properties;
import java.util.logging.Logger;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;


public class Hyperspace {

    static final Logger log = Logger.getLogger("org.hypertable.Hyperspace");

    public Hyperspace(Comm comm, Properties props) {

	if (props.getProperty("verbose").equalsIgnoreCase("true"))
	    mVerbose = true;

	// Determine working directory
	if ((mBasedir = props.getProperty("Hyperspace.dir")) == null) {
	    java.lang.System.err.println("Required config property 'Hyperspace.dir' not found.");
	    java.lang.System.exit(1);
	}
	if (mBasedir.endsWith("/"))
	    mBasedir = mBasedir.substring(0, mBasedir.length()-1);

	if (mVerbose)
	    java.lang.System.out.println("Hyperspace.dir=" + mBasedir);
    }

    public void Create(ResponseCallback cb, String fileName) {
	int error = Error.OK;

	if (mVerbose)
	    log.info("Creating file '" + fileName + "'");

	try {

	    if (fileName.indexOf(".attr.") != -1)
		throw new IOException("Bad filename (" + fileName + ")");

	    String baseDir = fileName.startsWith("/") ? mBasedir : mBasedir + "/";

	    int lastSlash = fileName.lastIndexOf("/");

	    // make sure parent exists, throw an informative message if not
	    if (lastSlash > 0) {
		String relParent = fileName.substring(0, lastSlash);
		File absParent = new File(baseDir, relParent);
		if (!absParent.exists()) 
		    throw new FileNotFoundException(relParent);
	    }

	    (new File(baseDir + fileName)).createNewFile();

	    error = cb.response_ok();

	}
	catch (FileNotFoundException e) {
	    log.info("File not found - " + e.getMessage());
	    error = cb.error(Error.HYPERTABLEFS_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(Error.HYPERTABLEFS_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to CREATE command - " + Error.GetText(error));
	
    }


    public void Exists(ResponseCallback cb, String fileName) {
	int error = Error.OK;
	boolean exists = false;

	File absName = fileName.startsWith("/") ? new File(mBasedir + fileName) : new File(mBasedir + "/" + fileName);

	exists = absName.exists();

	if (mVerbose)
	    log.info("Checking for existance of file '" + fileName + "' : " + exists);

	if (exists)
	    error = cb.response_ok();
	else
	    error = cb.error(Error.HYPERTABLEFS_FILE_NOT_FOUND, fileName);

	if (error != Error.OK)
	    log.severe("Problem sending response to EXISTS command - " + Error.GetText(error));

    }


    public void AttrSet(ResponseCallback cb, String fileName, String attrName, String attrValue) {
	int error = Error.OK;

	try {

	    if (mVerbose)
		log.info("AttrSet file=" + fileName + " attr=" + attrName);

	    String baseDir = fileName.startsWith("/") ? mBasedir : mBasedir + "/";

	    if (!(new File(baseDir + fileName)).exists())
		throw new FileNotFoundException(fileName);

	    File attrFile = new File(baseDir + fileName + ".attr." + attrName);

	    FileWriter out = new FileWriter(attrFile);
	    out.write(attrValue);
	    out.close();

	    error = cb.response_ok();

	}
	catch (FileNotFoundException e) {
	    log.info("File not found - " + e.getMessage());
	    error = cb.error(Error.HYPERTABLEFS_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(Error.HYPERTABLEFS_IO_ERROR, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to ATTRSET command - " + Error.GetText(error));

    }

    public void AttrDel(ResponseCallback cb, String fileName, String attrName) {
	int error = Error.HYPERTABLEFS_IO_ERROR;

	try {

	    if (mVerbose)
		log.info("AttrDel file=" + fileName + " attr=" + attrName);

	    String baseDir = fileName.startsWith("/") ? mBasedir : mBasedir + "/";

	    if (!(new File(baseDir + fileName)).exists())
		throw new FileNotFoundException(fileName);

	    File attrFile = new File(baseDir + fileName + ".attr." + attrName);

	    if (!attrFile.exists()) {
		error = Error.HYPERTABLEFS_ATTR_NOT_FOUND;
		throw new IOException("Attribute '" + attrName + "' not found");
	    }

	    attrFile.delete();

	    error = cb.response_ok();
	    
	}
	catch (FileNotFoundException e) {
	    log.info("File not found - " + e.getMessage());
	    error = cb.error(Error.HYPERTABLEFS_FILE_NOT_FOUND, e.getMessage());
	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(error, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to ATTRDEL command - " + Error.GetText(error));

    }


    public void AttrGet(ResponseCallbackAttrGet cb, String fileName, String attrName) {
	int error = Error.HYPERTABLEFS_IO_ERROR;

	try {

	    if (mVerbose)
		log.info("AttrGet file=" + fileName + " attr=" + attrName);

	    String baseDir = fileName.startsWith("/") ? mBasedir : mBasedir + "/";

	    if (!(new File(baseDir + fileName)).exists())
		throw new FileNotFoundException(fileName);

	    File attrFile = new File(baseDir + fileName + ".attr." + attrName);

	    if (!attrFile.exists()) {
		error = Error.HYPERTABLEFS_ATTR_NOT_FOUND;
		throw new IOException("Attribute '" + attrName + "' not found");
	    }

	    long length = attrFile.length();

	    byte [] attrBytes = new byte [ (int)length ];

	    FileInputStream is = new FileInputStream(attrFile);
	    is.read(attrBytes, 0, attrBytes.length);
	    is.close();

	    error = cb.response( new String(attrBytes) );

	}
	catch (FileNotFoundException e) {
	    log.info("File not found - " + fileName);
	    error = cb.error(Error.HYPERTABLEFS_FILE_NOT_FOUND, fileName);
	}
	catch (IOException e) {
	    log.info("I/O exception - " + e.getMessage());
	    error = cb.error(error, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to ATTRGET command - " + Error.GetText(error));

    }


    public void Mkdirs(ResponseCallback cb, String dirName) {
	int error = Error.HYPERTABLEFS_IO_ERROR;

	if (mVerbose)
	    log.info("Creating directory '" + dirName + "'");

	try {

	    if (dirName.indexOf(".attr.") != -1) {
		log.info("Bad directory name - " + dirName);
		throw new IOException("Bad directory name - " + dirName);
	    }

	    File absName = dirName.startsWith("/") ? new File(mBasedir + dirName) : new File(mBasedir + "/" + dirName);

	    if (!absName.mkdirs()) {
		log.info("mkdirs failure - " + absName);
		error = Error.HYPERTABLEFS_CREATE_FAILED;
		throw new IOException("mkdirs failure - " + dirName);
	    }

	    error = cb.response_ok();
	}
	catch (IOException e) {
	    error = cb.error(error, e.getMessage());
	}

	if (error != Error.OK)
	    log.severe("Problem sending response to MKDIRS command - " + Error.GetText(error));
    }

    private boolean mVerbose;
    private String  mBasedir;
}
