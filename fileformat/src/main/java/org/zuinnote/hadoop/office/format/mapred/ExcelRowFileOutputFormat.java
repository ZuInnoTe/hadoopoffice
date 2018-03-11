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

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.HadoopUtil;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;

/**
 * 
 *
 */
public class ExcelRowFileOutputFormat extends AbstractSpreadSheetDocumentFileOutputFormat<ArrayWritable>
		implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1241428157127154160L;
	private static final Log LOG = LogFactory.getLog(ExcelRowFileOutputFormat.class.getName());

	@Override
	public RecordWriter<NullWritable, ArrayWritable> getRecordWriter(FileSystem ignored, JobConf conf, String name,
			Progressable progress) throws IOException {
		// check if mimeType is set. If not assume new Excel format (.xlsx)

		String defaultConf = conf.get(HadoopOfficeWriteConfiguration.CONF_MIMETYPE,
				ExcelFileOutputFormat.DEFAULT_MIMETYPE);
		conf.set(HadoopOfficeWriteConfiguration.CONF_MIMETYPE, defaultConf);
		Path file;
		if (name!=null) {
			file = getTaskOutputPath(conf, name);
			// add suffix
			file = file.suffix(ExcelFileOutputFormat.getSuffix(conf.get(HadoopOfficeWriteConfiguration.CONF_MIMETYPE)));
		} else {
			file = getOutputPath(conf);
		}
		try {
			return new ExcelRowRecordWriter<>(
					HadoopUtil.getDataOutputStream(conf, file, progress, getCompressOutput(conf),
							getOutputCompressorClass(conf, ExcelFileOutputFormat.defaultCompressorClass)),
					file.getName(), conf);
		} catch (InvalidWriterConfigurationException | OfficeWriterException e) {
			LOG.error(e);
		}

		return null;
	}

}
