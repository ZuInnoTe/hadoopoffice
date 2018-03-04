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

package org.zuinnote.hadoop.office.format.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.Serializable;

import java.security.GeneralSecurityException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.HadoopUtil;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;
import org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException;

public class ExcelFileOutputFormat extends AbstractSpreadSheetDocumentFileOutputFormat<SpreadSheetCellDAO>implements Serializable {
/**
	 * 
	 */
	private static final long serialVersionUID = -1241096542392745880L;
private static final Log LOG = LogFactory.getLog(ExcelFileOutputFormat.class.getName());
public static final Class defaultCompressorClass = GzipCodec.class; 
public static final String DEFAULT_MIMETYPE="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
public static final String SUFFIX_OOXML = ".xlsx";
public static final String SUFFIX_OLDEXCEL = ".xls";


/*
* Returns a new record writer, if mimetype is not specified it is assumed that the new Excel format (.xlsx) should be used 
*
* @param context MR context
*
* @return Excel Record Writer
*
*/
@Override
public RecordWriter<NullWritable,SpreadSheetCellDAO> getRecordWriter(TaskAttemptContext context) throws IOException {
	// check if mimeType is set. If not assume new Excel format (.xlsx)
	Configuration conf=context.getConfiguration();
	String defaultConf=conf.get(HadoopOfficeWriteConfiguration.CONF_MIMETYPE,ExcelFileOutputFormat.DEFAULT_MIMETYPE);
	conf.set(HadoopOfficeWriteConfiguration.CONF_MIMETYPE,defaultConf);
	// add suffix	
	Path file = getDefaultWorkFile(context,ExcelFileOutputFormat.getSuffix(conf.get(HadoopOfficeWriteConfiguration.CONF_MIMETYPE)));


	 	try {
			return new ExcelRecordWriter<>(HadoopUtil.getDataOutputStream(conf,file,context,getCompressOutput(context),getOutputCompressorClass(context, ExcelFileOutputFormat.defaultCompressorClass)),file.getName(),conf);
		} catch (InvalidWriterConfigurationException | InvalidCellSpecificationException | FormatNotUnderstoodException
				| GeneralSecurityException | OfficeWriterException e) {
			LOG.error(e);
		}

	return null;
}

/*
* Determines file extension based on MimeType
*
* @param mimeType mimeType of the file
*
* @return file extension
*
*/
public static String getSuffix(String mimeType) {
	if (mimeType.contains("openxmlformats-officedocument.spreadsheetml")) {
		return ExcelFileOutputFormat.SUFFIX_OOXML;
	} else if (mimeType.contains("ms-excel")) {
		return ExcelFileOutputFormat.SUFFIX_OLDEXCEL;
	} 
	return ".unknown";
}



}
