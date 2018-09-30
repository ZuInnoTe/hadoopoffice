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

import java.security.GeneralSecurityException;


import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.parser.*;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.MSExcelLowFootprintParser;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.MSExcelParser;

/*
*
* This class is responsible for parsing content of InputStreams using Apache Tika and dependent libraries. Afterwards one can fetch the rows from the content.
* A row can be interpreted differently. It returns for each row or line an array of Objects containing the column data. You have to check the Object type by using instanceof or similar.
*
*/

public class OfficeReader {
private static final Log LOG = LogFactory.getLog(OfficeReader.class.getName());
private static final String FORMAT_EXCEL = "ms-excel";
private HadoopOfficeReadConfiguration hocr;
private InputStream in;
private String[] sheetsArray=null;

private OfficeReaderParserInterface currentParser=null;

	/*
	* Creates a new OfficeReaderObject for a given content and Mime Type (cf. https://tika.apache.org/1.13/formats.html#Full_list_of_Supported_Formats)
	*
	* @param in InputStream holding the document
	* @param hocr HadoopOfficeConfiguration object for reading files
	* 	
	*/

	public OfficeReader(InputStream in, HadoopOfficeReadConfiguration hocr) {
		LOG.debug("Initializing OfficeReader");
		this.in=in;
		this.hocr=hocr;
		if ((hocr.getSheets()!=null) && !("".equals(hocr.getSheets()))){
			this.sheetsArray=hocr.getSheets().split(":");
		} else {
			this.sheetsArray=null;
		}
	}

	/**
	* Parses the input stream and generates an in-memory representation of the document. In most cases (depending on the parser) the full document needs to be represented in-memory.
	*
	* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case an invalid format is detected
	*
	*/
	public void parse() throws FormatNotUnderstoodException {
		// do content detection of document
			if (this.hocr.getMimeType().contains(OfficeReader.FORMAT_EXCEL))	{
				// check if low footprint
				if (!this.hocr.getLowFootprint()) {
					LOG.info("Using standard API to parse Excel file");
					// if it contains Excel then use default MSExcelParser
					this.currentParser=new MSExcelParser(this.hocr, this.sheetsArray);
				} else {
					// use low footprint parser
					LOG.info("Using low footprint API to parse Excel file");
					this.currentParser=new MSExcelLowFootprintParser(this.hocr, this.sheetsArray);
				}
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
				return null;
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
