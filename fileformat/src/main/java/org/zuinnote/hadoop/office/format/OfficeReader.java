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

package org.zuinnote.hadoop.office.format;

import java.io.IOException;

import java.io.InputStream;
import java.io.BufferedInputStream;

import org.apache.tika.Tika;

import java.util.Locale;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.parser.*;

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
private BufferedInputStream bis=null;
private String mimeType; 
private Tika tika=null;
private OfficeReaderParserInterface currentParser=null;

	/*
	* Creates a new OfficeReaderObject for a given content and Mime Type (cf. https://tika.apache.org/1.13/formats.html#Full_list_of_Supported_Formats)
	*
	* @param in InputStream holding the document
	* @param bufferSize size of Buffer (BufferedInputStream)
	* @param mimeType Mime Type of the office document
	* @param String sheets list of sheets to read (only for spreadsheet formats), sheetnames separated by :, empty string means read all sheets, sheets that are not found by name are logged as a warning
	* @param locale Locale to be used for interpreting content in spreadsheets and databases
	*/

	public OfficeReader(InputStream in, int bufferSize, String mimeType, String sheets, Locale locale) {
		this.bis=new BufferedInputStream(in,bufferSize);
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
		
	}

	public void parse() throws IOException, FormatNotUnderstoodException {
		// do content detection of document
			if (this.mimeType.contains(OfficeReader.FORMAT_EXCEL))	{
			// if it contains Excel then use MSExcelParser
				currentParser=new MSExcelParser(this.locale,this.sheetsArray);
			} else {
			// if it cannot be detected throw an exception
				throw new FormatNotUnderstoodException("Format not understood");
			}
		// parse the inputStream
		currentParser.parse(this.bis);
	}

	/* 
	* Get next row
	*
	* @return column values for given row as Objects (use instanceof or similar to determine the type), null if no further rows exist
	* 
	*/

	public Object[] getNext() {
		if (currentParser==null) return null;
		return currentParser.getNext();
	}


	public long getCurrentRow() {
			if (currentParser==null) return 0;
			return currentParser.getCurrentRow();
	}

	public String getCurrentSheetName() {
			if (currentParser==null) return null;
			return currentParser.getCurrentSheetName();
	}

	public void close() throws IOException {
		
		if (this.bis!=null) {
			bis.close();
		}
	}

}
