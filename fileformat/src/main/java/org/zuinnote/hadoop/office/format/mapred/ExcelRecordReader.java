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

import java.io.IOException;

import java.security.GeneralSecurityException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.*;

/* ExcelRecordReader reads rows from Excel
*
* You can specify the following options:
* See AbstractTableDocumentRecordReader
*
*
*/


public class ExcelRecordReader extends AbstractSpreadSheetDocumentRecordReader<Text,ArrayWritable> {
private static final Log LOG = LogFactory.getLog(ExcelRecordReader.class.getName());
private FileSplit split;

public ExcelRecordReader(FileSplit split, JobConf job, Reporter reporter) throws IOException,FormatNotUnderstoodException,GeneralSecurityException {
 super(split,job,reporter);
 LOG.debug("Initialize ExcelRecordReader");
 this.split=split;
}

/**
*
* Create an empty key
*
* @return key
*/
@Override
public Text createKey() {	
	return new Text("");	
}

/**
*
* Create an empty value
*
* @return value
*/
@Override
public ArrayWritable createValue() {
	ArrayWritable newArrayWritable = new ArrayWritable(SpreadSheetCellDAO.class);
	newArrayWritable.set(new SpreadSheetCellDAO[0]);
	return newArrayWritable;
}



/**
*
* Read row from Office document. If document does not match a defined metadata filter then it returns no rows. If no metadata filter is defined or document matches metadata filter then it returns rows, if available in the document/selected sheet
*
* @return true if next more rows are available, false if not
*/
@Override
public boolean next(Text key, ArrayWritable value) throws IOException {
	if (!(this.getOfficeReader().getFiltered())) {
		return false;
	}
	Object[] objectArray = this.getOfficeReader().getNext();
	if (objectArray==null) {
		return false; // no more to read
	}
	SpreadSheetCellDAO[] cellRows = (SpreadSheetCellDAO[])objectArray;
	key.set("["+this.split.getPath().getName()+"]"+this.getOfficeReader().getCurrentSheetName()+"!A"+this.getOfficeReader().getCurrentRow());
	value.set(cellRows);
	return true;	
}



}
