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

import java.security.GeneralSecurityException;

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


import org.zuinnote.hadoop.office.format.common.HadoopUtil;
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
public static final String CONF_SECURITYPASSWORD="hadoopoffice.write.security.crypt.password";
public static final String CONF_SECURITYALGORITHM="hadoopoffice.write.security.crypt.encrypt.algorithm";
public static final String CONF_SECURITYMODE="hadoopoffice.write.security.crypt.encrypt.mode";
public static final String CONF_CHAINMODE="hadoopoffice.write.security.crypt.chain.mode";
public static final String CONF_HASHALGORITHM="hadoopoffice.write.security.crypt.hash.algorithm";
public static final String CONF_DECRYPTLINKEDWBBASE="hadoopoffice.write.security.crypt.linkedworkbooks.";
public static final String CONF_METADATA="hadoopoffice.write.metadata."; // base: all these properties (e.g. hadoopoffice.write.metadata.author) will be handed over to the corresponding writer
public static final String DEFAULT_MIMETYPE="";
public static final String DEFAULT_LOCALE="";
public static final String DEFAULT_LINKEDWB="";
public static final boolean DEFAULT_IGNOREMISSINGLINKEDWB=false;
public static final String DEFAULT_AUTHOR="hadoopoffice";
public static final int DEFAULT_COMMENTWIDTH=1;
public static final int DEFAULT_COMMENTHEIGHT=3;
public static final String DEFAULT_PASSWORD=null;
public static final String DEFAULT_ALGORITHM="aes256";
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
private String password;
private String encryptAlgorithm;
private String hashAlgorithm;
private String encryptMode;
private String chainMode;
private Map<String,String> linkedWBCredentialMap;
private Map<String,String> metadata;
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
* hadoopoffice.write.security.crypt.password: use password to encrypt the document. Note: There is no security check of strongness of password. This is up to the application developer.
* hadoopoffice.write.security.crypt.encrypt.algorithm: use the following algorithm to encrypt. Note that some writers do not support all algorithms and an exception will be thrown if the algorithm is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.hash.algorithm: use the following algorithm to hash. Note that some writers do not support all algorithms and an exception will be thrown if the algorithm is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.encrypt.mode: use the following mode to encrypt. Note that some writers do not support all modes and an exception will be thrown if the mode is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.chain.mode: use the following mode to chain. Note that some writers do not support all modes and an exception will be thrown if the mode is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.linkedworkbooks.*: if set then hadoopoffice will try to decrypt all the linked workbooks where a password has been specified. If no password is specified then it is assumed that the linked workbook is not encrypted. Example: Property key for file "linkedworkbook1.xlsx" is  "hadoopoffice.read.security.crypt.linkedworkbooks.linkedworkbook1.xslx". Value is the password. You must not include path or protocol information in the filename 
* hadoopoffice.write.metadata.*: Write metadata properties of the document. All properties belonging to the base (e.g. hadoopoffice.write.metadata.author for author) will be handed over to the corresponding writer. See writer documentation which properties are supported
*
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case the writer could not be configured correctly
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException in case there are not enough information in SpreadSheetDAO to fill out cell
* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case of invalid format of linkeded workbooks
* @throws java.security.GeneralSecurityException in case of encrypted linkedworkbooks that could not be decrypted
*
*/
public AbstractSpreadSheetDocumentRecordWriter(DataOutputStream out, String fileName, JobConf conf) throws IOException,InvalidWriterConfigurationException,InvalidCellSpecificationException,FormatNotUnderstoodException, GeneralSecurityException {
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
      this.linkedWorkbooksName=HadoopUtil.parseLinkedWorkbooks(linkedWorkbooksStr);
      this.ignoreMissingLinkedWorkbooks=conf.getBoolean(this.CONF_IGNOREMISSINGWB,this.DEFAULT_IGNOREMISSINGLINKEDWB);
      this.encryptAlgorithm=conf.get(this.CONF_SECURITYALGORITHM);
      this.password=conf.get(this.CONF_SECURITYPASSWORD);
      this.hashAlgorithm=conf.get(this.CONF_HASHALGORITHM);
      this.encryptMode=conf.get(this.CONF_SECURITYMODE);
      this.chainMode=conf.get(this.CONF_CHAINMODE);
      this.metadata=HadoopUtil.parsePropertiesFromBase(conf,this.CONF_METADATA);
      this.linkedWBCredentialMap=HadoopUtil.parsePropertiesFromBase(conf,this.CONF_DECRYPTLINKEDWBBASE);
      // load linked workbooks as inputstreams
      this.currentReader= new HadoopFileReader(this.conf);
      this.linkedWorkbooksMap=this.currentReader.loadLinkedWorkbooks(linkedWorkbooksName);
     // create OfficeWriter 
       this.officeWriter=new OfficeWriter(this.mimeType, this.locale, this.ignoreMissingLinkedWorkbooks,  this.fileName, this.commentAuthor,this.commentWidth,this.commentHeight,this.password,this.encryptAlgorithm,this.hashAlgorithm,this.encryptMode,this.chainMode,this.metadata);
      this.officeWriter.create(out,this.linkedWorkbooksMap,this.linkedWBCredentialMap);
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
	} catch (GeneralSecurityException gse) {
		LOG.error(gse);
	}
	finally {
		if (this.currentReader!=null) {
			this.currentReader.close();
		}
	}
 }






}
