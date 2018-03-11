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
package org.zuinnote.hadoop.excel.hive.outputformat;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HivePassThroughRecordWriter;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.zuinnote.hadoop.office.format.mapred.ExcelRowFileOutputFormat;

/**
 * We use the HivePassThroughOutputFormat to reduce redundant code
 *
 */
public class HiveExcelRowFileOutputFormat extends ExcelRowFileOutputFormat implements HiveOutputFormat<NullWritable,ArrayWritable> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3914655814828214968L;

	public HiveExcelRowFileOutputFormat() {
		
	}

	@Override
	public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
			Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
			Progressable progress) throws IOException {
		FileSystem fs = finalOutPath.getFileSystem(jc);
		HiveExcelRowFileOutputFormat.setOutputPath(jc, finalOutPath);
	    RecordWriter<?, ?> recordWriter = this.getRecordWriter(fs, jc, null, progress);
	    return new HivePassThroughRecordWriter(recordWriter);
	}



}
