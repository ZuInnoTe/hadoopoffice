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

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.zuinnote.hadoop.office.example.tasks.HadoopOfficeExcelMap;
import org.zuinnote.hadoop.office.example.tasks.HadoopOfficeExcelReducer;
  
import org.zuinnote.hadoop.office.format.common.*;
import org.zuinnote.hadoop.office.format.mapreduce.*;

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.dao.TextArrayWritable;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

public class CSV2ExcelDriver  {

       
        
 public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
   /** Set as an example some of the options to configure the HadoopOffice fileformat **/
  
     conf.set("hadoopoffice.read.locale.bcp47","de");
     Job job = Job.getInstance(conf,"example-hadoopoffice-CSV2Excel-job");
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
     System.exit(job.waitForCompletion(true)?0:1);
 }
        
}
