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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;


import java.security.GeneralSecurityException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


import org.zuinnote.hadoop.office.format.common.HadoopFileReader;
import org.zuinnote.hadoop.office.format.common.HadoopKeyStoreManager;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeWriter;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.writer.*;

/**
* This abstract writer implements some generic functionality for writing tables in various formats
*
*
*/

public abstract class AbstractSpreadSheetDocumentRecordWriter<NullWritable,SpreadSheetCellDAO> extends RecordWriter<NullWritable,SpreadSheetCellDAO> {
public static final Log LOG = LogFactory.getLog(AbstractSpreadSheetDocumentRecordWriter.class.getName());

private OfficeWriter officeWriter;
private Map<String,InputStream> linkedWorkbooksMap;
private HadoopOfficeWriteConfiguration howc;
private HadoopFileReader currentReader;
private DataOutputStream out;



/*
* Non-arg constructor for Serialization
*
*/

public AbstractSpreadSheetDocumentRecordWriter() {
		// for serialization
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
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException in case there are not enough information in SpreadSheetDAO to fill out cell
* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case of invalid format of linkeded workbooks
* @throws java.security.GeneralSecurityException in case of encrypted linkedworkbooks that could not be decrypted
*
*/
public AbstractSpreadSheetDocumentRecordWriter(DataOutputStream out, String fileName, Configuration conf) throws IOException,InvalidWriterConfigurationException,InvalidCellSpecificationException, FormatNotUnderstoodException,GeneralSecurityException, OfficeWriterException {
 	this.out=out;
	// parse configuration
     this.howc=new HadoopOfficeWriteConfiguration(conf,fileName);
  
     this.readKeyStore(conf);
       // load linked workbooks as inputstreams
      this.currentReader= new HadoopFileReader(conf);
      this.linkedWorkbooksMap=this.currentReader.loadLinkedWorkbooks(this.howc.getLinkedWorkbooksName());
     // create OfficeWriter 
       this.officeWriter=new OfficeWriter(this.howc);
       InputStream templateInputStream=null;
       if ((this.howc.getTemplate()!=null) && (!"".equals(this.howc.getTemplate()))) {
    	   templateInputStream=this.currentReader.loadTemplate(this.howc.getTemplate());
       }
      this.officeWriter.create(out,this.linkedWorkbooksMap,this.howc.getLinkedWBCredentialMap(), templateInputStream);
}

/**
 * Reads the keystore to obtain credentials
 * 
 * @param conf Configuration provided by the Hadoop environment
 * @throws IOException 
 * @throws OfficeWriterException
 * 
 */
private void readKeyStore(Configuration conf) throws IOException, OfficeWriterException {
	if ((this.howc.getCryptKeystoreFile()!=null) && (!"".equals(this.howc.getCryptKeystoreFile()))) {
		LOG.info("Using keystore to obtain credentials instead of passwords");
		HadoopKeyStoreManager hksm = new HadoopKeyStoreManager(conf);
		try {
			hksm.openKeyStore(new Path(this.howc.getCryptKeystoreFile()), this.howc.getCryptKeystoreType(), this.howc.getCryptKeystorePassword());
			String password="";
			if ((this.howc.getCryptKeystoreAlias()!=null) && (!"".equals(this.howc.getCryptKeystoreAlias()))) {
				password=hksm.getPassword(this.howc.getCryptKeystoreAlias(), this.howc.getCryptKeystorePassword());
			} else {
				password=hksm.getPassword(this.howc.getFileName(), this.howc.getCryptKeystorePassword());
			}
			this.howc.setPassword(password);
		} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException | UnrecoverableEntryException | InvalidKeySpecException e) {
			LOG.error(e);
			throw new OfficeWriterException("Cannot read keystore to obtain credentials used to encrypt office documents "+e);
		}
		
	}
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
public synchronized void  close(TaskAttemptContext context) throws IOException {

		try {
			this.officeWriter.close();
		}  finally {
			if (this.out!=null) {
				this.out.close();
			}
			if (this.currentReader!=null) {
				this.currentReader.close();
			}
		}
	
	
 }






}
