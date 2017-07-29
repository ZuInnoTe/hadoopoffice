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
 * Simple Driver for a map reduce job for converting CSV to Excel
 */
package org.zuinnote.hadoop.office.example.driver;

        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zuinnote.hadoop.office.example.tasks.HadoopOfficeExcelMap;
import org.zuinnote.hadoop.office.example.tasks.HadoopOfficeExcelReducer;
  
import org.zuinnote.hadoop.office.format.mapreduce.*;

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.dao.TextArrayWritable;
   
/**
* Author: Jörn Franke (zuinnote@gmail.com)
*
*/

public class CSV2ExcelDriver  extends Configured implements Tool {
	

public CSV2ExcelDriver() {
	// nothing here
}


public int run(String[] args) throws Exception {
	Job job = Job.getInstance(getConf(),"example-hadoopoffice-CSV2Excel-job");
    job.setJarByClass(CSV2ExcelDriver.class);     
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TextArrayWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(SpreadSheetCellDAO.class);
       
   job.setMapperClass(HadoopOfficeExcelMap.class);
   job.setReducerClass(HadoopOfficeExcelReducer.class);
       
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(ExcelFileOutputFormat.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   return job.waitForCompletion(true)?0:1;
}
        
 public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
   /** Set as an example some of the options to configure the HadoopOffice fileformat **/
     /** note this sets the locale to us-english, which means that numbers might be displayed differently then you expect. Change this to the locale of the Excel file **/
     conf.set("hadoopoffice.read.locale.bcp47","us");
     int res = ToolRunner.run(conf, new CSV2ExcelDriver(), args);
     System.exit(res);
 }
        
}
