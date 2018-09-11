/**
* Copyright 2018 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/
package org.zuinnote.hadoop.office.format.common.parser.msexcel.internal;

import java.io.InputStream;

/**
 * Parses .xlsx files in pull mode instead of push
 *
 */
public class XSSFPullParser {
	
	private boolean nextBeingCalled;
	private boolean finalized;
	
	/**
	 * 
	 * @param sheetInputStream sheet in xlsx format input stream
	 */
	public XSSFPullParser(InputStream sheetInputStream) {
		
		this.nextBeingCalled=false;
		this.finalized=false;
	}
	
	public boolean hasNext() {
		this.nextBeingCalled=true;
		if (this.finalized) { // we finished already - no more to read
			return false;
		}
		// search for the next row
		return false;
	}
	
	public Object[] getNext() {
		if (!this.nextBeingCalled) { // skip to the next tag
			if (this.hasNext()==false) {
				return null;
			}
		}
		if (this.finalized) { // no more to read
			return null;
		}
		// read all cells in row and create SpreadSheetCellDAOs
		this.nextBeingCalled=false;
		return null;
	}

}
