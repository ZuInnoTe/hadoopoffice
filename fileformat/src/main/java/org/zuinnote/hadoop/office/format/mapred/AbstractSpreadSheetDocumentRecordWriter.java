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



import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


import org.zuinnote.hadoop.office.format.common.HadoopFileReader;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeWriter;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.writer.*;

/**
* This abstract writer implements some generic functionality for writing tables in various formats
*
*
*/



public abstract class AbstractSpreadSheetDocumentRecordWriter<NullWritable,SpreadSheetCellDAO> implements RecordWriter<NullWritable,SpreadSheetCellDAO> {
public static final Log LOG = LogFactory.getLog(AbstractSpreadSheetDocumentRecordWriter.class.getName());
private OfficeWriter officeWriter;
private Map<String,InputStream> linkedWorkbooksMap;
private HadoopFileReader currentReader;
private HadoopOfficeWriteConfiguration howc;


/*
* Non-arg constructor for Serialization
*
*/

public AbstractSpreadSheetDocumentRecordWriter() {
}


/**
* Creates an Abstract Record Writer for tables to various document formats
* 
* @param out OutputStream to which the tables should be written to
* @param fileName fileName (without path) of the file to be written
* @param conf Configuration to be parsed by HadoopOfficeWriteConfiguration
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case the writer could not be configured correctly
* @throws org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException in case of issues creating the initial document
*
*/
public AbstractSpreadSheetDocumentRecordWriter(DataOutputStream out, String fileName, Configuration conf) throws InvalidWriterConfigurationException, IOException, OfficeWriterException {
	// parse configuration
    this.howc=new HadoopOfficeWriteConfiguration(conf,fileName);
      // load linked workbooks as inputstreams
     this.currentReader= new HadoopFileReader(conf);
     this.linkedWorkbooksMap=this.currentReader.loadLinkedWorkbooks(this.howc.getLinkedWorkbooksName());
    // create OfficeWriter 
      this.officeWriter=new OfficeWriter(this.howc);
     this.officeWriter.create(out,this.linkedWorkbooksMap,this.howc.getLinkedWBCredentialMap()); 
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
		} catch (OfficeWriterException e) {
			LOG.error(e);
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
			this.officeWriter.close();
		} catch (OfficeWriterException e) {
			LOG.error(e);
		} finally {
			if (this.currentReader!=null) {
				this.currentReader.close();
			}
		}
 }






}
