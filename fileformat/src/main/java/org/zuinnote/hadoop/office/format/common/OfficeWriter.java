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

package org.zuinnote.hadoop.office.format.common;

import java.io.IOException;

import java.io.InputStream;
import java.io.OutputStream;

import java.security.GeneralSecurityException;

import java.util.Locale;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;

import org.zuinnote.hadoop.office.format.common.writer.MSExcelWriter;
import org.zuinnote.hadoop.office.format.common.writer.OfficeSpreadSheetWriterInterface;
import org.zuinnote.hadoop.office.format.common.writer.ObjectNotSupportedException;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException;
 

/*
*
* This class is responsible for writing content using libraries for processing office documents. It accepts an array (= a row) of objects to be stored 
*
*/

public class OfficeWriter {
private static final Log LOG = LogFactory.getLog(OfficeWriter.class.getName());
private Locale useLocale;
private boolean ignoreMissingLinkedWorkbooks;
private String fileName;
private String commentAuthor;
private int commentWidth;
private int commentHeight;
private OfficeSpreadSheetWriterInterface currentOfficeSpreadSheetWriter=null;


/**
*
* Creates a new writer for office documents given a mime type (cf. https://tika.apache.org/1.13/formats.html#Full_list_of_Supported_Formats)
*
* @param mimeType Mime Type of the office document
* @param useLocale Locale to be used to evaluate cells
* @param ignoreMissingLinkedWorkbooks if true then missing linked workbooks are ignored during writing, if false then missing linked workbooks are not ignored and need to be present
* @param fileName filename of the document without path
* @param commentAuthor default author for comments
* @param commentWidth default width for comments
* @param commentHeight default height for comments
* @param password password for encryption. Set to null for no encryption.
* @param encryptAlgorithm Encryption algorithm (support depends upon writer, see documentation there). It is strongly recommended to do an in-depth analysis which algorithm to select to provide optimal security
* @param hashAlgorithm Hash algorithm (support depends upon writer, see documentation there). It is strongly recommended to do an in-depth analysis which algorithm to select to provide optimal security
* @param encryptMode Encrypt mode (support depends upon writer, see documentation there). It is strongly recommended to do an in-depth analysis which algorithm to select to provide optimal security
* @param chainMode Chain mode (support depends upon writer, see documentation there). It is strongly recommended to do an in-depth analysis which algorithm to select to provide optimal security
* @param metadata to write with the document. Please consult documentation of the write to see which attribute names and values are supported
*
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case format is not supported or encryption algorithm is wrongly specified
*
*/

public OfficeWriter(String mimeType, Locale useLocale, boolean ignoreMissingLinkedWorkbooks, String fileName,String commentAuthor, int commentWidth, int commentHeight, String password, String encryptAlgorithm, String hashAlgorithm, String encryptMode, String chainMode, Map<String,String> metadata) throws InvalidWriterConfigurationException {
	LOG.debug("Initialize OfficeWriter");
	this.useLocale=useLocale;
	this.ignoreMissingLinkedWorkbooks=ignoreMissingLinkedWorkbooks;
	this.fileName=fileName;
	this.commentAuthor=commentAuthor;
	this.commentWidth=commentWidth;
	this.commentHeight=commentHeight;
	// check mimetype and create parser, this is based on some heuristics on the mimetype
	String writerFormat=getInternalWriterFormatFromMimeType(mimeType);
	if (MSExcelWriter.isSupportedFormat(writerFormat)) {
		currentOfficeSpreadSheetWriter=new MSExcelWriter(writerFormat,this.useLocale,this.ignoreMissingLinkedWorkbooks, this.fileName,this.commentAuthor,this.commentWidth,this.commentHeight,password,encryptAlgorithm,hashAlgorithm,encryptMode,chainMode,metadata);
	} else {
		throw new InvalidWriterConfigurationException("Error: Writer does not recognize format +\""+writerFormat+"\"");
	}
}


/**
* Creates a new office document
*
* @param oStream OutputStream where the Workbook should be written when calling finalizeWrite
* @param linkedWorkbooks linked workbooks that are already existing and linked to this document. Only if supported by the format
* @param linkedWorkbooksPasswords a map of passwords and linkedworkbooks. The key is the filename without path of the linkedworkbook and the value is the password
*
* @throws java.io.IOException if there is an issue with the OutputStream
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case no proper writer has been instantiated
* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case one of the linked workbooks has an invalid format
* 
*/

public void create(OutputStream oStream, Map<String,InputStream> linkedWorkbooks,Map<String,String> linkedWorkbooksPasswords) throws IOException, InvalidWriterConfigurationException,FormatNotUnderstoodException,GeneralSecurityException {
	if (this.currentOfficeSpreadSheetWriter!=null) {
		this.currentOfficeSpreadSheetWriter.create(oStream,linkedWorkbooks,linkedWorkbooksPasswords);
	} else {
		throw new InvalidWriterConfigurationException("No writer instantiated");
	}
}


/**
* Writes an object to the office document. Note the type of object highly depends on the underlying writer. E.g. for SpreadSheet-based writers it is of class SpreadSheetCellDAO
*
* @param o object 
*
* @throws org.zuinnote.hadoop.office.format.common.writer.ObjectNotSupportedException in case the underlying writer cannot handle this type of object
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case no proper writer has been instantiated
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException in case the specification of the cell is incorrect
*
*/
public void write(Object o) throws ObjectNotSupportedException,InvalidWriterConfigurationException,InvalidCellSpecificationException {
	
if (this.currentOfficeSpreadSheetWriter!=null) {
		this.currentOfficeSpreadSheetWriter.write(o);
	} else {
		throw new InvalidWriterConfigurationException("No writer instantiated");
	}
}


/**
* Writes the document in-memory representation to the OutputStream. Aferwards, it closes all related workbooks.
*
* @throws java.io.IOException in case of issues writing.
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case no proper writer has been instantiated
* @throws java.security.GeneralSecurityException in case of issues of writing encrypted documents
*
*
*/
public void finalizeWrite() throws IOException,InvalidWriterConfigurationException,GeneralSecurityException {
	
if (this.currentOfficeSpreadSheetWriter!=null) {
		this.currentOfficeSpreadSheetWriter.finalizeWrite();
	} else {
		throw new InvalidWriterConfigurationException("No writer instantiated");
	}
}


/***
*
* Identify the right format for the writer based on its MimeType
*
* @param mimeType MimeType of the office document
*
* @return format of the writer
*
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case the format is not recognized
*
*/
private String getInternalWriterFormatFromMimeType(String mimeType) throws InvalidWriterConfigurationException {
 // for MS Office it is based on https://blogs.msdn.microsoft.com/vsofficedeveloper/2008/05/08/office-2007-file-format-mime-types-for-http-content-streaming-2/
 if (mimeType.contains("ms-excel")) { 
	return MSExcelWriter.FORMAT_OLD;
} else if (mimeType.contains("openxmlformats-officedocument.spreadsheetml")) {
	return MSExcelWriter.FORMAT_OOXML;
} else {
	throw new InvalidWriterConfigurationException("Format \""+mimeType+"\" not recognized");
}
}


}



