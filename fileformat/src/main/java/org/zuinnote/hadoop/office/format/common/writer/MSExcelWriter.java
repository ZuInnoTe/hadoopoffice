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

package org.zuinnote.hadoop.office.format.common.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.WorkbookUtil;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.parser.MSExcelParser;

public class MSExcelWriter implements OfficeSpreadSheetWriterInterface {
public final static String FORMAT_OOXML = "ooxmlexcel";
public final static String FORMAT_OLD = "oldexcel";
public final static String[] VALID_FORMAT = {FORMAT_OOXML, FORMAT_OLD};
private static final Log LOG = LogFactory.getLog(MSExcelWriter.class.getName());
private final static String DEFAULT_FORMAT = VALID_FORMAT[0];
private String format=DEFAULT_FORMAT;
private OutputStream oStream;
private Workbook currentWorkbook;
private Locale useLocale;
private boolean ignoreMissingLinkedWorkbooks;
private String fileName;
private String commentAuthor;
private int commentWidth;
private int commentHeight;
private Map<String,InputStream> linkedWorkbooks;
private Map<String,FormulaEvaluator> linkedFormulaEvaluators;
private Map<String,Drawing> mappedDrawings;
private List<Workbook> listOfWorkbooks;
private FormulaEvaluator currentFormulaEvaluator;

/**
*
* Creates a new writer for MS Excel files
*
* @param excelFormat format of the Excel: ooxmlexcel: Excel 2007-2013 (.xlsx), oldexcel: Excel 2003 (.xls)
* @param useLocale Locale to be used to evaluate cells
* @param ignoreMissingLinkedWorkbooks if true then missing linked workbooks are ignored during writing, if false then missing linked workbooks are not ignored and need to be present
* @param fileName filename without path of the workbook
* @param commentAuthor default author for comments
* @param commentWidth width of comments in terms of number of columns
* @param commentHeight height of commments in terms of number of rows
*
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case the writer is not configured correctly 
*
*/

public MSExcelWriter(String excelFormat, Locale useLocale, boolean ignoreMissingLinkedWorkbooks, String fileName, String commentAuthor, int commentWidth, int commentHeight) throws InvalidWriterConfigurationException {
	boolean formatFound=isSupportedFormat(excelFormat);
	if (formatFound==false) {
		 LOG.error("Unknown Excel format: "+this.format);
		 throw new InvalidWriterConfigurationException("Unknown Excel format: "+this.format);
	}
	this.format=excelFormat;
	this.useLocale=useLocale;
	this.ignoreMissingLinkedWorkbooks=ignoreMissingLinkedWorkbooks;
	this.fileName=fileName;
	this.commentAuthor=commentAuthor;
	this.commentWidth=commentWidth;
	this.commentHeight=commentHeight;
}



/**
* Creates a new Excel workbook for the output stream. Note: You need to save it AFTER adding the cells via addSpreadSheetCell using the method finalizeWrite
*
* @param oStream OutputStream where the Workbook should be written when calling finalizeWrite
* @param linkedWorkbooks linked workbooks that are already existing and linked to this spreadsheet
*
* @throws java.io.IOException if there is an issue with the OutputStream
*
*/

public void create(OutputStream oStream, Map<String,InputStream> linkedWorkbooks) throws IOException,FormatNotUnderstoodException {
	this.oStream=oStream;
	// create a new Workbook either in old Excel or "new" Excel format
	if (this.format.equals(MSExcelWriter.FORMAT_OOXML)) {
		this.currentWorkbook=new XSSFWorkbook();
	} else if (this.format.equals(MSExcelWriter.FORMAT_OLD)) {
		this.currentWorkbook=new HSSFWorkbook();
	} 
	this.linkedWorkbooks=linkedWorkbooks;
	this.currentFormulaEvaluator=this.currentWorkbook.getCreationHelper().createFormulaEvaluator();
	this.linkedFormulaEvaluators=new HashMap<String,FormulaEvaluator>();
	this.linkedFormulaEvaluators.put(this.fileName,this.currentFormulaEvaluator);
	this.mappedDrawings=new HashMap<String,Drawing>();
	// add current workbook to list of linked workbooks
	this.listOfWorkbooks=new ArrayList<Workbook>();
	// parse linked workbooks
	try {
		for (String name: linkedWorkbooks.keySet()) {
			// parse linked workbook
			MSExcelParser currentLinkedWorkbookParser = new MSExcelParser(this.useLocale, null, this.ignoreMissingLinkedWorkbooks,name);
			currentLinkedWorkbookParser.parse(this.linkedWorkbooks.get(name));
			this.listOfWorkbooks.add(currentLinkedWorkbookParser.getCurrentWorkbook());
			this.linkedFormulaEvaluators.put(name,currentLinkedWorkbookParser.getCurrentFormulaEvaluator());
			this.currentWorkbook.linkExternalWorkbook(name,currentLinkedWorkbookParser.getCurrentWorkbook());
		}
					
	} finally {	// close linked workbook inputstreams
		for (InputStream currentIS: linkedWorkbooks.values()) {
			currentIS.close();
		}
	}
	LOG.debug("Size of linked formula evaluators map: "+linkedFormulaEvaluators.size());
	this.currentFormulaEvaluator.setupReferencedWorkbooks(linkedFormulaEvaluators);
}
	


/**
* Add a cell to the current Workbook
*
* @param newDAO cell to add. If it is already existing an exception will be thrown. Note that the sheet name is sanitized using  org.apache.poi.ss.util.WorkbookUtil.createSafeSheetName. The Cell address needs to be in A1 format. Either formula or formattedValue must be not null.
*
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidCellSpecificationException in case the cell cannot be added (e.g. invalid address, invalid formula, already existing etc.)
* @throws org.zuinnote.hadoop.office.format.common.writer.ObjectNotSupportedException in case the object is not of type SpreadSheetCellDAO
*
*/

public void write(Object newDAO) throws InvalidCellSpecificationException,ObjectNotSupportedException {
	if (!(newDAO instanceof SpreadSheetCellDAO)) {
		throw new ObjectNotSupportedException("Objects which are not of the class SpreadSheetCellDAO are not supported.");
	}
	SpreadSheetCellDAO sscd = (SpreadSheetCellDAO)newDAO;
	// check sheetname (needs to be there)
	if ((sscd.getSheetName()==null) || ("".equals(sscd.getSheetName()))) {
		throw new InvalidCellSpecificationException("Empy sheet name not allowed.");
	}
	// check celladdress (needs to be there)
	if ((sscd.getAddress()==null) || ("".equals(sscd.getAddress()))) {
		throw new InvalidCellSpecificationException("Empy cell address not allowed.");
	}
	// check that either formula or formatted value is filled
	if ((sscd.getFormula()==null) && (sscd.getFormattedValue()==null))  {
		throw new InvalidCellSpecificationException("Either formula or formattedValue needs to be specified for cell.");
	}
	String safeSheetName=WorkbookUtil.createSafeSheetName(sscd.getSheetName());
	Sheet currentSheet=this.currentWorkbook.getSheet(safeSheetName);
	if (currentSheet==null) {// create sheet if it does not exist yet
		currentSheet=this.currentWorkbook.createSheet(safeSheetName);
		if (safeSheetName.equals(sscd.getSheetName())==false) {
			LOG.warn("Sheetname modified from \""+sscd.getSheetName()+"\" to \""+safeSheetName+"\" to correspond to Excel conventions.");
		}
		// create drawing anchor (needed for comments...)
		this.mappedDrawings.put(safeSheetName,currentSheet.createDrawingPatriarch());
	}
	// check if cell exist
	CellAddress currentCA = new CellAddress(sscd.getAddress());
	Row currentRow = currentSheet.getRow(currentCA.getRow());
	if (currentRow==null) { // row does not exist? => create it
		currentRow=currentSheet.createRow(currentCA.getRow());
	}
	Cell currentCell = currentRow.getCell(currentCA.getColumn());
	if (currentCell!=null) { // cell already exists? => throw exception
		throw new InvalidCellSpecificationException("Cell already exists at "+currentCA);
	}
	// create cell
	currentCell=currentRow.createCell(currentCA.getColumn());		
	// set the values accordingly
	if ("".equals(sscd.getFormula())==false) { // if formula exists then use formula
		currentCell.setCellFormula(sscd.getFormula());
		
	} else {	
	// else use formattedValue
		currentCell.setCellValue(sscd.getFormattedValue());
	}
	// set comment
	if ((sscd.getComment()!=null) && ("".equals(sscd.getComment())==false)) {
		/** the following operations are necessary to create comments **/
		/** Define size of the comment window **/
		    ClientAnchor anchor = this.currentWorkbook.getCreationHelper().createClientAnchor();
    		    anchor.setCol1(currentCell.getColumnIndex());
    		    anchor.setCol2(currentCell.getColumnIndex()+this.commentWidth);
    		    anchor.setRow1(currentRow.getRowNum());
    		    anchor.setRow2(currentRow.getRowNum()+this.commentHeight);
		/** create comment **/
		    Comment currentComment = mappedDrawings.get(safeSheetName).createCellComment(anchor);
    		    currentComment.setString(this.currentWorkbook.getCreationHelper().createRichTextString(sscd.getComment()));
    		    currentComment.setAuthor(this.commentAuthor);
		    currentCell.setCellComment(currentComment);

	}
}

/**
* Writes the document in-memory representation to the OutputStream. Aferwards, it closes all related workbooks.
*
* @throws java.io.IOException in case of issues writing.
*
*
*/

public void finalizeWrite() throws IOException {
	try {
	if (this.oStream!=null) {
		this.currentWorkbook.write(this.oStream);
		this.oStream.close();
	}
	} finally {
	// close main workbook
	if (this.currentWorkbook!=null) {
		this.currentWorkbook.close();
	}
	// close linked workbooks
	 	for (Workbook currentWorkbook: this.listOfWorkbooks) {
			if (currentWorkbook!=null) {
				currentWorkbook.close();
			}
		}
	}
}


/**
* Checks if format is supported
*
* @param format String describing format
*
* @return true, if supported, false if not
*
*/
public static boolean isSupportedFormat(String format) {
	for (int i=0;i<MSExcelWriter.VALID_FORMAT.length;i++) {
		if (VALID_FORMAT[i].equals(format)==true) {
			return true;
		}
	}
return false;
}

}
