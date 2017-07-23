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


import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


import org.zuinnote.hadoop.office.format.common.writer.MSExcelWriter;
import org.zuinnote.hadoop.office.format.common.writer.OfficeSpreadSheetWriterInterface;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.MSExcelLowFootprintWriter;
 

/*
*
* This class is responsible for writing content using libraries for processing office documents. It accepts an array (= a row) of objects to be stored 
*
*/

public class OfficeWriter {
private static final String EX_NO_WRITER_INSTANTIATED = "No writer instantiated";
private static final Log LOG = LogFactory.getLog(OfficeWriter.class.getName());
private OfficeSpreadSheetWriterInterface currentOfficeSpreadSheetWriter=null;
private HadoopOfficeWriteConfiguration howc;
private OutputStream oStream;

/**
*
* Creates a new writer for office documents given a mime type (cf. https://tika.apache.org/1.13/formats.html#Full_list_of_Supported_Formats)
*
* @param howc HadoopOfficeWriteConfiguration
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case format is not supported or encryption algorithm is wrongly specified
*
*/

public OfficeWriter(HadoopOfficeWriteConfiguration howc) throws InvalidWriterConfigurationException {
	LOG.debug("Initialize OfficeWriter");
	this.howc=howc;
	// check mimetype and create parser, this is based on some heuristics on the mimetype
	String writerFormat=OfficeWriter.getInternalWriterFormatFromMimeType(this.howc.getMimeType());
	if (MSExcelWriter.isSupportedFormat(writerFormat)) {
		// check if it is low footprint
		if (!this.howc.getLowFootprint()) {
			currentOfficeSpreadSheetWriter=new MSExcelWriter(writerFormat,this.howc);
		} else { // low footprint 
			if (MSExcelWriter.FORMAT_OLD.equals(writerFormat)) {
				LOG.warn("Low footprint mode is only supported for new Excel format .xlsx. Continuing with standard writing mode");
				currentOfficeSpreadSheetWriter=new MSExcelWriter(writerFormat,this.howc);
			} else {
				LOG.info("Storing new Excel file ,xlsx in low footprint mode");
				currentOfficeSpreadSheetWriter=new MSExcelLowFootprintWriter(writerFormat,this.howc);
			}
		}
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
* @param template
* @throws org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException in case no proper writer has been instantiated or linked workbooks have invalid format
* 
*/

public void create(OutputStream oStream, Map<String,InputStream> linkedWorkbooks,Map<String,String> linkedWorkbooksPasswords, InputStream template) throws OfficeWriterException {
	this.oStream=oStream;
	if (this.currentOfficeSpreadSheetWriter!=null) {
		this.currentOfficeSpreadSheetWriter.create(oStream,linkedWorkbooks,linkedWorkbooksPasswords,template);
	} else {
		throw new OfficeWriterException(EX_NO_WRITER_INSTANTIATED);
	}
}


/**
* Writes an object to the office document. Note the type of object highly depends on the underlying writer. E.g. for SpreadSheet-based writers it is of class SpreadSheetCellDAO
*
* @param o object 
*
* @throws org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException in case the specification of the cell is incorrect
*
*/
public void write(Object o) throws OfficeWriterException {
	
if (this.currentOfficeSpreadSheetWriter!=null) {
		this.currentOfficeSpreadSheetWriter.write(o);
	} else {
		throw new OfficeWriterException(EX_NO_WRITER_INSTANTIATED);
	}
}


/**
* Writes the document in-memory representation to the OutputStream. Afterwards, it closes all related workbooks.
*
* @throws java.io.IOException in case it cannot properly close the outputstream
*
*
*/
public void close() throws IOException {
try {
if (this.currentOfficeSpreadSheetWriter!=null) {
		this.currentOfficeSpreadSheetWriter.close();
	} else {
		LOG.error(EX_NO_WRITER_INSTANTIATED);
	}
} finally {
	
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
private static String getInternalWriterFormatFromMimeType(String mimeType) throws InvalidWriterConfigurationException {
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



