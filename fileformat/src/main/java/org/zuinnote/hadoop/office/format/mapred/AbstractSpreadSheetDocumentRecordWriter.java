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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


import org.zuinnote.hadoop.office.format.common.HadoopFileReader;
import org.zuinnote.hadoop.office.format.common.HadoopKeyStoreManager;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeWriter;
import org.zuinnote.hadoop.office.format.common.writer.*;

/**
* This abstract writer implements some generic functionality for writing tables in various formats
*
*
*/



public abstract class AbstractSpreadSheetDocumentRecordWriter<NullWritable,K> implements RecordWriter<NullWritable,K> {
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
  
    this.readKeyStore(conf);
    this.readSigningKeyAndCertificate(conf);
      // load linked workbooks as inputstreams
     this.currentReader= new HadoopFileReader(conf);
     this.linkedWorkbooksMap=this.currentReader.loadLinkedWorkbooks(this.howc.getLinkedWorkbooksName());
    // create OfficeWriter 
      this.officeWriter=new OfficeWriter(this.howc);
      InputStream templateInputStream=null;
      if ((this.howc.getTemplate()!=null) && (!"".equals(this.howc.getTemplate()))) {
   	   templateInputStream=this.currentReader.loadTemplate(this.howc.getTemplate());
      }
     this.officeWriter.create(out,this.linkedWorkbooksMap,this.howc.getLinkedWBCredentialMap(),templateInputStream); 
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
			LOG.error("Cannopt read keystore. Exception: ",e);
			throw new OfficeWriterException("Cannot read keystore to obtain credentials used to encrypt office documents "+e);
		}
		
	}
}

/***
 * Reads the  (private) key and certificate from keystore to sign
 * 
 * @param conf
 * @throws OfficeWriterException
 * @throws IOException
 */
private void readSigningKeyAndCertificate(Configuration conf) throws OfficeWriterException, IOException {
	if ((this.howc.getSigKeystoreFile()!=null) && (!"".equals(this.howc.getSigKeystoreFile()))) {
		LOG.info("Signing document");
		if ((this.howc.getSigKeystoreAlias()==null) || ("".equals(this.howc.getSigKeystoreAlias()))) {
				LOG.error("Keystore alias for signature keystore not defined. Cannot sign document");
				throw new OfficeWriterException("Keystore alias for signature keystore not defined. Cannot sign document");
		}
		if ((this.howc.getSigKeystoreType()==null) || ("".equals(this.howc.getSigKeystoreType()))) {
			LOG.error("Keystore type for signature keystore not defined. Cannot sign document");
			throw new OfficeWriterException("Keystore type for signature keystore not defined. Cannot sign document");
		}
		LOG.info("Reading keystore");
		HadoopKeyStoreManager hksm = new HadoopKeyStoreManager(conf);
		try {
			hksm.openKeyStore(new Path(this.howc.getSigKeystoreFile()), this.howc.getSigKeystoreType(), this.howc.getSigKeystorePassword());
			this.howc.setSigKey(hksm.getPrivateKey(this.howc.getSigKeystoreAlias(), this.howc.getSigKeystorePassword()));
			this.howc.setSigCertificate((X509Certificate) hksm.getCertificate(this.howc.getSigKeystoreAlias()));
		} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException | UnrecoverableKeyException  e) {
			LOG.error("Cannopt read signing certificate. Exception: ",e);
			throw new OfficeWriterException("Cannot read keystore to obtain key and certificate for signing "+e);
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
public synchronized void write(NullWritable key, K value) throws IOException {
		try {
			if (value==null) {
				return;
			}
			if (value instanceof ArrayWritable) {
				ArrayWritable row = (ArrayWritable)value;
				Writable[] rowCellDAO = row.get();
				for (int i=0;i<rowCellDAO.length;i++) {
					this.officeWriter.write(rowCellDAO[i]);
				}
			} else {
				this.officeWriter.write(value);
			}
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
		}  finally {
			if (this.currentReader!=null) {
				this.currentReader.close();
			}
		}
 }






}
