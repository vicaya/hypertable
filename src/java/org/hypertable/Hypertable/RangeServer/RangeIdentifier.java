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

package org.hypertable.Hypertable.RangeServer;

import java.text.ParseException;

public class RangeIdentifier {
    public RangeIdentifier(String idStr) throws ParseException {
	int openBracket = idStr.indexOf('[');
	if (openBracket == -1)
	    tableName = idStr;
	else {
	    tableName = idStr.substring(0, openBracket);
	    int closeBracket = idStr.lastIndexOf(']');
	    if (closeBracket == -1)
		throw new ParseException("Bad range specifier (no closing brace)", 0);
	    if (closeBracket-openBracket > 1) {
		String rangeStr = idStr.substring(openBracket+1, closeBracket);
		int colon = rangeStr.indexOf(':');
		if (colon == -1)
		    endRow = rangeStr;
		else if (colon == 0) {
		    endRow = rangeStr.substring(1);
		}
		else {
		    startRow = rangeStr.substring(0, colon);
		    if (colon+1 < rangeStr.length())
			endRow = rangeStr.substring(colon+1);
		}
	    }
	}
    }
    public String tableName = null;
    public String startRow = null;
    public String endRow = null;
}

