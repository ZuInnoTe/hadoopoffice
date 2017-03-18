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

package org.zuinnote.hadoop.office.format.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.security.GeneralSecurityException;

import org.zuinnote.hadoop.office.format.common.parser.*;

public class ExcelFileInputFormat extends AbstractSpreadSheetDocumentFileInputFormat {

private static final Log LOG = LogFactory.getLog(ExcelFileInputFormat.class.getName());


public  RecordReader<Text,ArrayWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
/** Create reader **/
try {
		 // send configuration option to ms excel. The format of the Excel (old vs new) is detected automaitcally
 		job.set(AbstractSpreadSheetDocumentRecordReader.CONF_MIMETYPE,"ms-excel");
		return new ExcelRecordReader( (FileSplit) split,job,reporter);
	} catch (FormatNotUnderstoodException e) {
		// log
		LOG.error(e);
	} catch (GeneralSecurityException gse) {
		LOG.error(gse);
	}
return null;
}
	
public void configure (JobConf conf) {
		// not used
	} 

	

	/**
	 * Unfortunately, we cannot split Excel documents correctly. Apache POI/library requires full documents.
	 * Nevertheless, most of the time you have anyway small (smaller than default HDFS blocksize) Office documents that can be processed fast. 
	 * Hence, you should put them in Hadoop Archives (HAR) either uncompressed or compressed to reduce load on namenode.
	 *
	*/
  	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
  	}	


}
