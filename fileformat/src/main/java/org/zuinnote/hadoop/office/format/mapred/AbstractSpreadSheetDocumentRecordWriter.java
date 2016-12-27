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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;

import java.util.Locale;
import java.util.Locale.Builder;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.HadoopFileReader;
import org.zuinnote.hadoop.office.format.common.OfficeWriter;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.writer.*;

/**
* This abstract writer implements some generic functionality for writing tables in various formats
*
*
*/

public abstract class AbstractSpreadSheetDocumentRecordWriter<NullWritable,SpreadSheetCellDAO> implements RecordWriter<NullWritable,SpreadSheetCellDAO> {
public static final Log LOG = LogFactory.getLog(AbstractSpreadSheetDocumentRecordWriter.class.getName());
public static final String CONF_MIMETYPE="hadoopoffice.write.mimeType";
public static final String CONF_LOCALE="hadoopoffice.write.locale.bcp47";
public static final String CONF_LINKEDWB="hadoopoffice.write.linkedworkbooks";
public static final String CONF_IGNOREMISSINGWB="hadoopoffice.write.ignoremissinglinkedworkbooks";
public static final String CONF_COMMENTAUTHOR="hadoopoffice.write.comment.author";
public static final String CONF_COMMENTWIDTH="hadoopoffice.write.comment.width";
public static final String CONF_COMMENTHEIGHT="hadoopoffice.write.comment.height";
public static final String DEFAULT_MIMETYPE="";
public static final String DEFAULT_LOCALE="";
public static final String DEFAULT_LINKEDWB="";
public static final boolean DEFAULT_IGNOREMISSINGLINKEDWB=false;
public static final String DEFAULT_AUTHOR="hadoopoffice";
public static final int DEFAULT_COMMENTWIDTH=1;
public static final int DEFAULT_COMMENTHEIGHT=3;
private String[] linkedWorkbooksName;
private JobConf conf;
private String fileName;
private Progressable progress;
private String mimeType;
private Locale locale;
private String localeStrBCP47;
private boolean ignoreMissingLinkedWorkbooks;
private String commentAuthor;
private int commentWidth;
private int commentHeight;
private OfficeWriter officeWriter;
private Map<String,InputStream> linkedWorkbooksMap;
private HadoopFileReader currentReader;


/**
* Creates an Abstract Record Writer for tables to various document formats
* 
* @param out OutputStream to which the tables should be written to
* @param fileName fileName (without path) of the file to be written
* @param conf Configuration:
 hadoopoffice.write.mimeType: Mimetype of the document
* hadoopoffice.write.locale: Locale of the document (e.g. needed for interpreting spreadsheets) in the BCP47 format (cf. https://tools.ietf.org/html/bcp47). If not specified then default system locale will be used.
* hadoopoffice.write.linkedworkbooks a []: separated list of existing linked workbooks. Example: [hdfs:///home/user/excel/linkedworkbook1.xls]:[hdfs:///home/user/excel/linkedworkbook2.xls]. Note: these workbooks are loaded during writing the current workbook. This means you may need a lot of memory on the node writing the file. Furthermore, you can only specify files and NOT directories.
* hadoopoffice.write.ignoremissinglinkedworkbooks: if you have specified linkedworkbooks then they are not read during writing. This implies also that the written document does NOT have a cached value. Value is ignored if you did not specify linkedworkbooks. Default: false. 
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case the writer could not be configured correctly
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException in case there are not enough information in SpreadSheetDAO to fill out cell
* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case of invalid format of linkeded workbooks
*
*/
public AbstractSpreadSheetDocumentRecordWriter(DataOutputStream out, String fileName, JobConf conf) throws IOException,InvalidWriterConfigurationException,InvalidCellSpecificationException,FormatNotUnderstoodException {
 	// parse configuration
     this.conf=conf;
     this.mimeType=conf.get(this.CONF_MIMETYPE,this.DEFAULT_MIMETYPE);
     this.localeStrBCP47=conf.get(this.CONF_LOCALE, this.DEFAULT_LOCALE);
     if (!("".equals(localeStrBCP47))) { // create locale
	this.locale=new Locale.Builder().setLanguageTag(this.localeStrBCP47).build();
      }
      this.fileName=fileName;
      this.commentAuthor=conf.get(this.CONF_COMMENTAUTHOR,this.DEFAULT_AUTHOR);
      this.commentWidth=conf.getInt(this.CONF_COMMENTWIDTH,this.DEFAULT_COMMENTWIDTH);
      this.commentHeight=conf.getInt(this.CONF_COMMENTHEIGHT,this.DEFAULT_COMMENTHEIGHT);
      String linkedWorkbooksStr=conf.get(this.CONF_LINKEDWB,this.DEFAULT_LINKEDWB);
      this.linkedWorkbooksName=parseLinkedWorkbooks(linkedWorkbooksStr);
      this.ignoreMissingLinkedWorkbooks=conf.getBoolean(this.CONF_IGNOREMISSINGWB,this.DEFAULT_IGNOREMISSINGLINKEDWB);
      // load linked workbooks as inputstreams
      this.currentReader= new HadoopFileReader(this.conf);
      this.linkedWorkbooksMap=loadLinkedWorkbooks(linkedWorkbooksName);
     // create OfficeWriter 
       this.officeWriter=new OfficeWriter(this.mimeType, this.locale, this.ignoreMissingLinkedWorkbooks,  this.fileName, this.commentAuthor,this.commentWidth,this.commentHeight);
      this.officeWriter.create(out,this.linkedWorkbooksMap);
}

/**
*
* Write SpreadSheetDAO into a table document. Note this does not necessarily mean it is already written in the OutputStream, but usually the in-memory representation.
* @param key is ignored
* @param value is a SpreadSheet Cell to be inserted into the table document
*
*/
@Override
public synchronized void write(NullWritable key, SpreadSheetCellDAO value) throws IOException {
	try {
		this.officeWriter.write(value);
	} catch (ObjectNotSupportedException onse) {
		LOG.error(onse);
	} catch (InvalidWriterConfigurationException iwce) {
		LOG.error(iwce);
	} catch (InvalidCellSpecificationException icse) {
		LOG.error(icse);
	}
}


/***
*
* This method closes the document and writes it into the OutputStream
*
*/
@Override
public synchronized void  close(Reporter reporter) throws IOException {
	try {
		this.officeWriter.finalizeWrite();
	} catch (InvalidWriterConfigurationException iwce) {
		LOG.error(iwce);
	} finally {
		if (this.currentReader!=null) {
			this.currentReader.close();
		}
	}
 }

/*
* Parses a string in the format [filename]:[filename2]:[filename3] into a String array of filenames
*
* @param linkedWorkbookString list of filename as one String
*
* @return String Array containing the filenames as separate Strings
*
**/
private String[] parseLinkedWorkbooks(String linkedWorkbooksString) {
	if ("".equals(linkedWorkbooksString)) {
		return null;
	}
	// first split by ]:[
	String[] tempSplit = linkedWorkbooksString.split("\\]:\\[");
	// 1st case just one filename remove first [ and last ]
	if (tempSplit.length==1) {
		tempSplit[0]=tempSplit[0].substring(1,tempSplit[0].length()-1);
	} else if (tempSplit.length>1) {
	// 2nd case two or more filenames
	// remove first [
		tempSplit[0]=tempSplit[0].substring(1,tempSplit[0].length());
	// remove last ]
		tempSplit[tempSplit.length-1]=tempSplit[tempSplit.length-1].substring(0,tempSplit[tempSplit.length-1].length()-1);
	}
	return tempSplit;
}



/*
* Loads linked workbooks as InputStreams
* 
* @param fileNames List of filenames (full URI/path) to load
*
* @return a Map of filenames (without path!) with associated InputStreams
*
* @throws java.io.IOException in case of issues loading a file
*
*/

private Map<String,InputStream> loadLinkedWorkbooks(String[] fileNames) throws IOException {
	HashMap<String,InputStream> result = new HashMap<String,InputStream>();
	if (fileNames==null) return result;
	for (String currentFile: fileNames) {
		Path currentPath=new Path(currentFile);
		InputStream currentInputStream = currentReader.openFile(currentPath);
		result.put(currentPath.getName(), currentInputStream); 
	}
	return result;
}

}
