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
 * Simple Mapper reading 
 */
package org.zuinnote.hadoop.office.example.tasks;

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import org.zuinnote.hadoop.office.format.mapreduce.*;
import org.zuinnote.hadoop.office.format.common.dao.*;

import java.util.*;

import org.zuinnote.hadoop.office.format.common.dao.TextArrayWritable;

public  class HadoopOfficeExcelMap  extends Mapper<LongWritable, Text, Text, TextArrayWritable> {
private static final String CSV_SEPARATOR=",";


@Override
public void setup(Context context) throws IOException, InterruptedException {
}
@Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Convert a separated line into several substrings
		StringTokenizer myTokenizer = new StringTokenizer(value.toString(), HadoopOfficeExcelMap.CSV_SEPARATOR);
		Text[] textArray = new Text[myTokenizer.countTokens()];
		int i=0;
		 while (myTokenizer.hasMoreTokens()) {
			textArray[i++]=new Text(myTokenizer.nextToken());
  		}
		TextArrayWritable valueOutArrayWritable = new TextArrayWritable();
		valueOutArrayWritable.set(textArray);
	    	context.write(new Text("Sheet1"), valueOutArrayWritable);
	   }
@Override
public void cleanup(Context context) {
} 
	    
}
	 
