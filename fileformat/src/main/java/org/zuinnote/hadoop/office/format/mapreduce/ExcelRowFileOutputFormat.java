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
package org.zuinnote.hadoop.office.format.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.HadoopUtil;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;


/**
 * @author jornfranke
 *
 */
public class ExcelRowFileOutputFormat extends AbstractSpreadSheetDocumentFileOutputFormat<ArrayWritable> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8620371832156924952L;
	private static final Log LOG = LogFactory.getLog(ExcelRowFileOutputFormat.class.getName());
	@Override
	public RecordWriter<NullWritable, ArrayWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
		// check if mimeType is set. If not assume new Excel format (.xlsx)
		Configuration conf=context.getConfiguration();
		String defaultConf=conf.get(HadoopOfficeWriteConfiguration.CONF_MIMETYPE,ExcelFileOutputFormat.DEFAULT_MIMETYPE);
		conf.set(HadoopOfficeWriteConfiguration.CONF_MIMETYPE,defaultConf);
		// add suffix	
		Path file = getDefaultWorkFile(context,ExcelFileOutputFormat.getSuffix(conf.get(HadoopOfficeWriteConfiguration.CONF_MIMETYPE)));


		 	try {
				return new ExcelRowRecordWriter<>(HadoopUtil.getDataOutputStream(conf,file,context,getCompressOutput(context),getOutputCompressorClass(context, ExcelFileOutputFormat.defaultCompressorClass)),file.getName(),conf);
			} catch (InvalidWriterConfigurationException | InvalidCellSpecificationException | FormatNotUnderstoodException
					| GeneralSecurityException | OfficeWriterException e) {
				LOG.error(e);
			}

		return null;
	}

}
