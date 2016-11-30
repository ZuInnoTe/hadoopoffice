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
 * Simple Driver for a map reduce job counting the number of rows / Excel sheet in input files
 */
package org.zuinnote.hadoop.office.example.driver;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.zuinnote.hadoop.office.example.tasks.HadoopOfficeExcelMap;
import org.zuinnote.hadoop.office.example.tasks.HadoopOfficeExcelReducer;
   
import org.zuinnote.hadoop.office.format.*;

import org.zuinnote.hadoop.office.example.tasks.util.TextArrayWritable;
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

public class ExcelRowDriver  {

       
        
 public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(ExcelRowDriver.class);
    conf.setJobName("example-hadoop-office-Excel2CSV-job");
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(TextArrayWritable.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
        
    conf.setMapperClass(HadoopOfficeExcelMap.class);
    conf.setReducerClass(HadoopOfficeExcelReducer.class);
        
    conf.setInputFormat(ExcelFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    /** Set as an example some of the options to configure the HadoopOffice fileformat **/
    conf.set("hadoopoffice.read.locale.bcp47","de");
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
 }
        
}
