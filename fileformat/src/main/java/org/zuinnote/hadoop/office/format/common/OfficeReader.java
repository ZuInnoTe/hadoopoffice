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
import java.io.BufferedInputStream;

import java.security.GeneralSecurityException;

import java.util.Map;
import java.util.Locale;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.parser.*;

/*
*
* This class is responsible for parsing content of InputStreams using Apache Tika and dependent libraries. Afterwards one can fetch the rows from the content.
* A row can be interpreted differently. It returns for each row or line an array of Objects containing the column data. You have to check the Object type by using instanceof or similar.
*
*/

public class OfficeReader {
private static final Log LOG = LogFactory.getLog(OfficeReader.class.getName());
private static final String FORMAT_EXCEL = "ms-excel";
private String[] sheetsArray=null;
private Locale locale=null;
private InputStream in=null;
private String mimeType; 
private boolean ignoreMissingLinkedWorkbooks;
private String fileName;
private String password;
private Map<String,String> metadataFilter;
private OfficeReaderParserInterface currentParser=null;

	/*
	* Creates a new OfficeReaderObject for a given content and Mime Type (cf. https://tika.apache.org/1.13/formats.html#Full_list_of_Supported_Formats)
	*
	* @param in InputStream holding the document
	* @param bufferSize size of Buffer (BufferedInputStream)
	* @param mimeType Mime Type of the office document
	* @param String sheets list of sheets to read (only for spreadsheet formats), sheetnames separated by :, empty string means read all sheets, sheets that are not found by name are logged as a warning
	* @param locale Locale to be used for interpreting content in spreadsheets and databases
	* @param ignoreMissingLinkedWorkbooks only for formats supporting references to other files. Ignores if these files are not available. 
	* @param fileName fileName of the file to read. Optional parameter and should not contain information about the directory.
	* @param password Password of this document (null if no password)
	* @param metadataFilter filter on metadata. The name is the metadata attribute name and the property is a filter. See the individual parser implementation what attributes are supported and if the filter, for example, supports regular expressions
	*/

	public OfficeReader(InputStream in, String mimeType, String sheets, Locale locale, boolean ignoreMissingLinkedWorkbooks, String fileName,String password, Map<String,String> metadataFilter) {
		LOG.debug("Initializing OfficeReader");
		this.in=in;
		if (mimeType==null) {
			this.mimeType="";
		} else {
			this.mimeType=mimeType;
		}
		if ((sheets!=null) && !("".equals(sheets))){
			this.sheetsArray=sheets.split(":");
		} else {
			this.sheetsArray=null;
		}
		this.locale=locale;
		this.ignoreMissingLinkedWorkbooks=ignoreMissingLinkedWorkbooks;
		this.fileName=fileName;
		this.password=password;
		this.metadataFilter=metadataFilter;
	}

	/**
	* Parses the input stream and generates an in-memory representation of the document. In most cases (depending on the parser) the full document needs to be represented in-memory.
	*
	* @throws java.io.IOException in case of errors reading from the InputStream
	* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case an invalid format is detected
	* @throws java.security.GeneralSecurityException in case of issues decrypting the document, if document is encrypted
	*
	*/
	public void parse() throws IOException, FormatNotUnderstoodException,GeneralSecurityException {
		// do content detection of document
			if (this.mimeType.contains(OfficeReader.FORMAT_EXCEL))	{
			// if it contains Excel then use MSExcelParser
				this.currentParser=new MSExcelParser(this.locale,this.sheetsArray,this.ignoreMissingLinkedWorkbooks, this.fileName,this.password,this.metadataFilter);
			} else {
			// if it cannot be detected throw an exception
				throw new FormatNotUnderstoodException("Format not understood");
			}
		// parse the inputStream
		currentParser.parse(this.in);
	}

	/**
	* Returns if the current document is filtered by a metadata filter
	*
	* @return true, if document applies to metadata filter, false if not
	*
	*/
	public boolean getFiltered() {
		if (this.currentParser!=null) {
			return this.currentParser.getFiltered();
		}
		return false;
	}


	/*
	* Returns the current parser
	*
	*/
	public OfficeReaderParserInterface getCurrentParser() {
		return this.currentParser;
	}

	/* 
	* Get next row
	*
	* @return column values for given row as Objects (use instanceof or similar to determine the type), null if no further rows exist
	* 
	*/

	public Object[] getNext() {
		if (currentParser==null) {
				return new Object[0];
		}
		return currentParser.getNext();
	}

	/**
	* Get the current row number
	*
	* @return current row number
	*
	*/

	public long getCurrentRow() {
			if (currentParser==null) {
				return 0;
			}
			return currentParser.getCurrentRow();
	}

	/*
	* Get name of the sheet that has been processed since the last call to getNext()
	*
	* @return sheet name
	*/
	public String getCurrentSheetName() {
			if (currentParser==null) {
				return null;
			}
			return currentParser.getCurrentSheetName();
	}

	/*
	* Closes the reader
	*
	*
	* @throws java.io.IOException in case of errors
	*/
	public void close() throws IOException {
		
		if (this.in!=null) {
			in.close();
		}
	}

}
