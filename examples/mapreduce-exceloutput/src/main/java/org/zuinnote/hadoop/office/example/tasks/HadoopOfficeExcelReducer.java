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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.util.*;

import org.zuinnote.hadoop.office.format.common.util.MSExcelUtil;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.dao.TextArrayWritable;
 
public class HadoopOfficeExcelReducer extends  Reducer<Text, TextArrayWritable, NullWritable, SpreadSheetCellDAO> {
private static final String CSV_SEPARATOR=",";
private static final NullWritable EMPTYKEY = NullWritable.get();
// since one reducer equals one file, we can assume one instance
private int currentRowNum=0;

   public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
     throws IOException, InterruptedException {
       for (TextArrayWritable currentRow: values) {  // should be only called once
	   String[] currentRowTextArray = currentRow.toStrings();
	   if (currentRowTextArray.length>0) {
		int currentColumnNum=0;
		for (String currentColumn: currentRowTextArray) { // for each column in the row
			String formattedValue=currentColumn;
			String comment="";
			String formula="";
			String address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum);
			String sheetName=key.toString();
			SpreadSheetCellDAO currentSCDAO = new SpreadSheetCellDAO(formattedValue,comment,formula,address,sheetName);
			context.write(this.EMPTYKEY, currentSCDAO);
			currentColumnNum++;
		}
		currentRowNum++;
	   }
       }
   }
}


