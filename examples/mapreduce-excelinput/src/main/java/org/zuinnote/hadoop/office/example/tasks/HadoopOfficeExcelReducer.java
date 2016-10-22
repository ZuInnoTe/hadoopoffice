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
 * Simple Reducer for converting the rows to CSV rows
 */
package org.zuinnote.hadoop.office.example.tasks;

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/
import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import java.util.*;

import org.zuinnote.hadoop.office.example.tasks.util.TextArrayWritable;

public class HadoopOfficeExcelReducer extends MapReduceBase implements Reducer<Text, TextArrayWritable, NullWritable, Text> {
private static final String CSV_SEPARATOR=",";
private static final NullWritable EMPTYKEY = NullWritable.get();

   public void reduce(Text key, Iterator<TextArrayWritable> values, OutputCollector<NullWritable, Text> output, Reporter reporter)
     throws IOException {
       while (values.hasNext()) { // should be only called once
	   TextArrayWritable currentRow = values.next();
	   Writable[] currentRowTextArray = currentRow.get();
	   if (currentRowTextArray.length>0) {
	   	StringBuilder currentCSVRowSB=new StringBuilder();
	   	for (int i=0;i<currentRowTextArray.length;i++) {
			Text currentCell = (Text)currentRowTextArray[i];
			if (currentCell!=null) {
				currentCSVRowSB.append(currentCell.toString());
			} 
		    	currentCSVRowSB.append(this.CSV_SEPARATOR);
	   	}
	   	// remove last separator and add new line
	   	String currentCSVRowString = currentCSVRowSB.substring(0,currentCSVRowSB.length()-1)+"\n";
		// add new line
	   	output.collect(this.EMPTYKEY, new Text(currentCSVRowString));
	   }
       }
   }
}


