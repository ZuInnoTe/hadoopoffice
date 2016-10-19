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

package org.zuinnote.hadoop.office.format;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.parser.*;

public class ExcelFileInputFormat extends AbstractTableDocumentFileInputFormat {

private static final Log LOG = LogFactory.getLog(ExcelFileInputFormat.class.getName());
public  RecordReader<Text,ArrayWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
/** Create reader **/
try {
		return new ExcelRecordReader( (FileSplit) split,job,reporter);
	} catch (FormatNotUnderstoodException e) {
		// log
		LOG.error(e);
	} 
return null;
}
	
	


}
