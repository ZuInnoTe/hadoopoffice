/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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


/**
 * Simple Mapper extracting the cell content from each cell as Text
 */
package org.zuinnote.hadoop.office.example.tasks;

/**
* Author: Jörn Franke (zuinnote@gmail.com)
*
*/
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import org.zuinnote.hadoop.office.format.mapreduce.*;
import org.zuinnote.hadoop.office.format.common.dao.*;

import java.util.*;

import org.zuinnote.hadoop.office.format.common.dao.TextArrayWritable;

public  class HadoopOfficeExcelMap  extends Mapper<Text, ArrayWritable, Text, TextArrayWritable> {
private final static Text NULL = new Text(""); // ArrayWritable cannot handle nulls



@Override
public void setup(Context context) throws IOException, InterruptedException {
}
@Override
  public void map(Text key, ArrayWritable value, Context context) throws IOException, InterruptedException {
		// get the cells of the current row
		SpreadSheetCellDAO[] valueArray=(SpreadSheetCellDAO[])value.get();
		// Extract SpreadsheetName from Key
		Text sheetNameText = new Text(key.toString());
		TextArrayWritable valueOutArrayWritable = new TextArrayWritable();
		Text[] valueOutTextArray = new Text[valueArray.length];
		// Create a simple ArrayWritable of Text
		 for (int i=0;i<valueArray.length;i++) {
			SpreadSheetCellDAO currentSpreadSheetCellDAO = valueArray[i];
			Text currentCellText = this.NULL;
			if (currentSpreadSheetCellDAO!=null) {
				currentCellText = new Text(currentSpreadSheetCellDAO.getFormattedValue());
			} 
				valueOutTextArray[i]=currentCellText;
		}
		valueOutArrayWritable.set(valueOutTextArray);
	    	context.write(sheetNameText, valueOutArrayWritable);
	   }
@Override
public void cleanup(Context context) {
} 
	    
}
	 
