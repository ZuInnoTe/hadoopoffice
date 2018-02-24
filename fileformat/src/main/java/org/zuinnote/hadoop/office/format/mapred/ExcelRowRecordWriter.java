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
package org.zuinnote.hadoop.office.format.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;

/**
 * @author jornfranke
 *
 */
public class ExcelRowRecordWriter<NullWritable,ArrayWritable>  extends AbstractSpreadSheetDocumentRecordWriter<NullWritable,ArrayWritable>  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4760231449095057101L;
	private static final Log LOGRW = LogFactory.getLog(ExcelRowRecordWriter.class.getName());

	/*
	* Non-arg constructor for Serialization
	*
	*/

	public ExcelRowRecordWriter() {
			// only needed for serialization
	}

	/*
	* Instantiate the constructor of AbstractSpreadSheetDocumentRecordWriter
	*
	*/
	public ExcelRowRecordWriter(DataOutputStream out, String fileName, Configuration conf) throws InvalidWriterConfigurationException, IOException, OfficeWriterException  {
		super(out,fileName,conf);
		LOGRW.debug("Initialize ExcelRowRecordWriter");
	}

}
