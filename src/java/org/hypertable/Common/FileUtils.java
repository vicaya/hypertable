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


package org.hypertable.Common;

import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;

public class FileUtils {

    // Returns the contents of the file in a byte array.
    public static byte[] FileToBuffer(File file) throws IOException {
	InputStream is = new FileInputStream(file);
    
	// Get the size of the file
	long length = file.length();
    
	// You cannot create an array using a long type.
	// It needs to be an int type.
	// Before converting to an int type, check
	// to ensure that file is not larger than Integer.MAX_VALUE.
	if (length > Integer.MAX_VALUE) {
	    // File is too large
	}
    
	// Create the byte array to hold the data
	byte[] bytes = new byte[(int)length];
    
	// Read in the bytes
	int offset = 0;
	int numRead = 0;
	while (offset < bytes.length
	       && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
	    offset += numRead;
	}
    
	// Ensure all the bytes have been read in
	if (offset < bytes.length) {
	    throw new IOException("Could not completely read file " + file.getName());
	}
    
	// Close the input stream and return bytes
	is.close();

	return bytes;
    }
}
