/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
