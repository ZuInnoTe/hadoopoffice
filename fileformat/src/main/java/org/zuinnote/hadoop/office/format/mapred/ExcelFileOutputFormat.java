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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import java.security.GeneralSecurityException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.HadoopUtil;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException;

public class ExcelFileOutputFormat extends AbstractSpreadSheetDocumentFileOutputFormat implements Serializable {
private static final Log LOG = LogFactory.getLog(ExcelFileOutputFormat.class.getName());
public static final Class defaultCompressorClass = GzipCodec.class; 
public static final String DEFAULT_MIMETYPE=org.zuinnote.hadoop.office.format.mapreduce.ExcelFileOutputFormat.DEFAULT_MIMETYPE;
public static final String SUFFIX_OOXML = org.zuinnote.hadoop.office.format.mapreduce.ExcelFileOutputFormat.SUFFIX_OOXML;
public static final String SUFFIX_OLDEXCEL = org.zuinnote.hadoop.office.format.mapreduce.ExcelFileOutputFormat.SUFFIX_OLDEXCEL;


/*
* Returns a new record writer, if mimetype is not specified it is assumed that the new Excel format (.xlsx) should be used 
*
* @param ignored Filesystem - is determined from the configuration
* @param conf Job configuration
* @param name Name of the file
* @paramprogress progress
*
* @return Excel Record Writer
*
*/

public RecordWriter<NullWritable,SpreadSheetCellDAO> getRecordWriter(FileSystem ignored, JobConf conf, String name, Progressable progress) throws IOException {
	// check if mimeType is set. If not assume new Excel format (.xlsx)
	
	String defaultConf=conf.get(AbstractSpreadSheetDocumentRecordWriter.CONF_MIMETYPE,this.DEFAULT_MIMETYPE);
	conf.set(AbstractSpreadSheetDocumentRecordWriter.CONF_MIMETYPE,defaultConf);
	
	Path file = getTaskOutputPath(conf, name);
	// add suffix
	file=file.suffix(getSuffix(conf.get(AbstractSpreadSheetDocumentRecordWriter.CONF_MIMETYPE)));
	try {
	 	return new ExcelRecordWriter(HadoopUtil.getDataOutputStream(conf,file,progress,getCompressOutput(conf),getOutputCompressorClass(conf, this.defaultCompressorClass)),file.getName(),conf);
	} catch (InvalidWriterConfigurationException iwe) {
		LOG.error(iwe);
	} catch (InvalidCellSpecificationException icse) {
		LOG.error(icse);
	} catch (FormatNotUnderstoodException fnue) {
		LOG.error(fnue);
	} catch (GeneralSecurityException gse) {
		LOG.error(gse);
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
private String getSuffix(String mimeType) {
	if (mimeType.contains("openxmlformats-officedocument.spreadsheetml")) {
		return this.SUFFIX_OOXML;
	} else if (mimeType.contains("ms-excel")) {
		return this.SUFFIX_OLDEXCEL;
	} 
	return ".unknown";
}

}
