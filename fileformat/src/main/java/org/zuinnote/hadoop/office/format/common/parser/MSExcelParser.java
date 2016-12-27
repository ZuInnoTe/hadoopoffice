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

package org.zuinnote.hadoop.office.format.common.parser;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

import org.apache.poi.hssf.model.InternalWorkbook;
import org.apache.poi.hssf.usermodel.HSSFFormulaEvaluator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory; 
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.xssf.model.ExternalLinksTable;
import org.apache.poi.ss.util.CellRangeAddress;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.lang.reflect.InvocationTargetException;
import java.lang.NoSuchMethodException;
import java.lang.IllegalAccessException;
import java.lang.NoSuchFieldException;

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

/*
*
* This class is responsible for parsing Excel content in OOXML format and old excel format
*
*/

public class MSExcelParser implements OfficeReaderParserInterface {
private static final Log LOG = LogFactory.getLog(MSExcelParser.class.getName());
private FormulaEvaluator formulaEvaluator;
private InputStream in;
private DataFormatter useDataFormatter=null;
private String[] sheets=null;
private Workbook currentWorkbook=null;
private int currentSheet=0; // current sheet where we are
private int sheetsIndex=0; // current index of sheets, if specified
private int currentRow=0;
private String currentSheetName="";
private HashMap<String,FormulaEvaluator> addedFormulaEvaluators;
private ArrayList<Workbook> addedWorkbooks;
private Locale locale;
private boolean ignoreMissingLinkedWorkbooks;
private String fileName;

	/*
	* In the default case all sheets are parsed one after the other.
	*
	* @param useLocale Locale to use (if null then default locale will be used), see java.util.Locale
	* @param ignoreMissingLinkedWorkbooks ignore missing linked Workbooks
	* @param filename filename of the file to parse (without directory). Required for linked workbooks
	*
	*/

	public MSExcelParser(Locale useLocale,boolean ignoreMissingLinkedWorkbooks, String fileName) {
		this(useLocale,null,ignoreMissingLinkedWorkbooks,fileName);
	}

	/*
	*
	* Only process selected sheets (one after the other)
	*
	* @param useLocale Locale to use (if null then default locale will be used), see java.util.Locale
	* @param sheets Set of sheets to be read. Note in linked workbooks all sheets are read
	* @param ignoreMissingLinkedWorkbooks ignore missing linked Workbooks
	* @param filename filename of the file to parse (without directory). Required for linked workbooks
	*
	*/
	public MSExcelParser(Locale useLocale, String[] sheets,boolean ignoreMissingLinkedWorkbooks, String fileName) {
		this.sheets=sheets;
		this.locale=locale;
		if (useLocale==null)  {
			useDataFormatter=new DataFormatter(); // use default locale
		} else {
			useDataFormatter=new DataFormatter(useLocale);
		}
		this.ignoreMissingLinkedWorkbooks=ignoreMissingLinkedWorkbooks;
		this.fileName=fileName;
		this.addedFormulaEvaluators = new HashMap<String,FormulaEvaluator>();
		this.addedWorkbooks = new ArrayList<Workbook>();
	}

	/*
	*
	* Parses the given InputStream containing Excel data. The type of InputStream (e.g. FileInputStream, BufferedInputStream etc.) does not matter here, but it is recommended to use an appropriate
	* type to avoid performance issues. 
	*
	* @param in InputStream containing Excel data
	*
	* @throws java.io.IOException in case of issues reading from in
	* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case there are issues reading from the Excel file
	*
	*/
	@Override
	public void parse(InputStream in) throws IOException,FormatNotUnderstoodException {
		this.in=in;
		// read xls
		try {
			this.currentWorkbook=WorkbookFactory.create(in);
		} catch (InvalidFormatException e) {
			throw new FormatNotUnderstoodException(e.toString());
		}
		finally 
		{
			this.in.close();
			this.in=null;
		}
		 this.formulaEvaluator = this.currentWorkbook.getCreationHelper().createFormulaEvaluator();
		  // add the formulator evaluator of this file as well or we will see a strange Exception
		 this.addedFormulaEvaluators.put(this.fileName,this.formulaEvaluator);
		 this.formulaEvaluator.setIgnoreMissingWorkbooks(this.ignoreMissingLinkedWorkbooks);
		 this.currentRow=0;
		 if (this.sheets==null) {
			this.currentSheetName=this.currentWorkbook.getSheetAt(0).getSheetName();
		 } else if (sheets.length<1) {
			throw new FormatNotUnderstoodException("Error: no sheets selected");
		 } else  {
			this.currentSheetName=sheets[0];
		 }
	}

	/**
	* Adds a linked workbook that is referred from this workbook. If the filename is already in the list then it is not processed twice. Note that the inputStream is closed after parsing
	*
	* @param name fileName (without path) of the workbook
	* @param inputStream content of the linked workbook
	*
	* @return true if it has been added, false if it has been already added
	*
	* @throws java.io.IOException in case of issues during reading of the inputStream
	* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case there are issues reading from the Excel file
	*
	**/
	@Override
	public boolean addLinkedWorkbook(String name, InputStream inputStream) throws IOException,FormatNotUnderstoodException {
		// check if already added
		if (this.addedFormulaEvaluators.containsKey(name)==true) {
			return false;
		}
		LOG.debug("Start adding  \""+name+"\" to current workbook");
		// create new parser, select all sheets
		MSExcelParser linkedWBMSExcelParser = new MSExcelParser(this.locale,null,this.ignoreMissingLinkedWorkbooks,name);
		// parse workbook 
		linkedWBMSExcelParser.parse(inputStream);
		// add linked workbook
		this.addedWorkbooks.add(linkedWBMSExcelParser.getCurrentWorkbook());
		this.addedFormulaEvaluators.put(name,linkedWBMSExcelParser.getCurrentFormulaEvaluator());
		this.formulaEvaluator.setupReferencedWorkbooks(addedFormulaEvaluators);
	
		return true;
	}

	/**
	* Provides a list of filenames that contain workbooks that are linked with the current one. Officially supported only for new Excel format. For the old Excel format this is experimental
	*
	* @return list of filenames (without path) belonging to linked workbooks
	* 
	*/
	@Override
	public List<String> getLinkedWorkbooks() {
		ArrayList<String> result = new ArrayList<String>();
		if (this.currentWorkbook instanceof HSSFWorkbook) {
			try {
				// this is a hack to fetch linked workbooks in the Old Excel format
				// we use reflection to access private fields
				// might not work if internal structure of the class changes
				InternalWorkbook intWb = ((HSSFWorkbook)this.currentWorkbook).getInternalWorkbook();
				// method to fetch link table
				Method linkTableMethod = InternalWorkbook.class.getDeclaredMethod("getOrCreateLinkTable");
		        	linkTableMethod.setAccessible(true);
        			Object linkTable = linkTableMethod.invoke(intWb);
				// method to fetch external book and sheet name
        			Method externalBooksMethod = linkTable.getClass().getDeclaredMethod("getExternalBookAndSheetName", int.class);
        			externalBooksMethod.setAccessible(true);
				// now we need to browse through the table until we hit an array out of bounds
				int i = 0;
				try {
					while(true) {
						String[] externalBooks = (String[])externalBooksMethod.invoke(linkTable, i++);
						if ((externalBooks!=null) && (externalBooks.length>0)){
							result.add(externalBooks[0]);
						}
			        	}
				} catch  ( java.lang.reflect.InvocationTargetException e) {
           				 if ( !(e.getCause() instanceof java.lang.IndexOutOfBoundsException) ) {
                			throw e;
            				}
				}
        			
			} catch (NoSuchMethodException nsme) {
				LOG.error("Could not retrieve linked workbooks for old Excel format. Internal error: "+nsme.toString());
			}
			 catch (IllegalAccessException iae) {
				LOG.error("Could not retrieve linked workbooks for old Excel format. Internal error: "+iae.toString());
			}
			catch (InvocationTargetException ite) {
				LOG.error("Could not retrieve linked workbooks for old Excel format. Internal error: "+ite.toString());
			}
			
    
		} else if (this.currentWorkbook instanceof XSSFWorkbook) {
			// use its API
			for (ExternalLinksTable element: ((XSSFWorkbook)this.currentWorkbook).getExternalLinksTable()) {
				result.add(element.getLinkedFileName());
			}
		} else {
			LOG.warn("Cannot determine linked workbooks");
		}
		return result;
	}

	/**
	*
	* returns the current formula evaluator of the workbook
	*
	* @return Formulaevalutor of the workbook
	*
	*/

	public FormulaEvaluator getCurrentFormulaEvaluator() {
		return this.formulaEvaluator;
	}

	/*
	* returns the current workbook
	*
	* @return current workbook
	*
	*/

	public Workbook getCurrentWorkbook() {
		return this.currentWorkbook;
	}

	/* returns the current row number starting from 1
	*
	* @return current row number
	*
	*/
	@Override
	public long getCurrentRow() {
		return (long)this.currentRow;
	}


	/* returns the current sheet name
	*
	* @return current sheet name
	*
	*/
	@Override
	public String getCurrentSheetName() {
		return this.currentSheetName;
	}

	/*
	* Returns the next row in the set of sheets. If sheets==null then all available sheets are returned in the order as specified in the document. If sheets contains specific sheets then rows of the specific sheets are returned in order of the sheets specified.
	*
	* @return column array of SpreadSheetCellDAO (may contain nulls if cell is without content), null if no further rows exist
	* 
	*/
	@Override
	public Object[] getNext() {
		SpreadSheetCellDAO[] result=null;
		// all sheets?
		if (this.sheets==null) { //  go on with all sheets
				if (this.currentRow>this.currentWorkbook.getSheetAt(this.currentSheet).getLastRowNum()) { // end of row reached? => next sheet
					this.currentSheet++;
					this.currentRow=0;
					if (this.currentSheet>=this.currentWorkbook.getNumberOfSheets()) return result; // no more sheets available?
					this.currentSheetName=this.currentWorkbook.getSheetAt(this.currentSheet).getSheetName();
				}

		} else { // go on with specified sheets
			// go through sheets specified until one found
			boolean sheetFound=false;
			while((this.sheetsIndex!=this.sheets.length) && sheetFound==false) {
				if (this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex])==null) { // log only if sheet not found
					LOG.warn("Sheet \""+this.sheets[this.sheetsIndex]+"\" not found");
				} else { // sheet found, check number of rows
				   if (this.currentRow>this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex]).getLastRowNum()) {
					// reset rows
					this.currentRow=0;
				   } else { // we have a sheet where we still need to process rows
					this.currentSheet=this.currentWorkbook.getSheetIndex(this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex]));
					this.currentSheetName=this.currentWorkbook.getSheetAt(this.currentSheet).getSheetName();
					sheetFound=true;
					break;
				   }
				}
				this.sheetsIndex++;
			}
			if (this.sheetsIndex==this.sheets.length) return result; // all sheets processed
		}
		// read row from the sheet currently to be processed
		Sheet rSheet = this.currentWorkbook.getSheetAt(this.currentSheet);
		Row rRow = rSheet.getRow(this.currentRow);
		if (rRow==null) {
			this.currentRow++;
			return new SpreadSheetCellDAO[0]; // emtpy row
		}
		result = new SpreadSheetCellDAO[rRow.getLastCellNum()];
		for (int i=0;i<rRow.getLastCellNum();i++) {
			Cell currentCell=rRow.getCell(i);
			if (currentCell==null) {
				result[i]=null;
			} else {	
				String formattedValue=useDataFormatter.formatCellValue(currentCell,this.formulaEvaluator);
				String formula = "";
				if (currentCell.getCellTypeEnum()==CellType.FORMULA)  {
					formula = currentCell.getCellFormula();
				}
				Comment currentCellComment = currentCell.getCellComment();
				String comment = "";
				if (currentCellComment!=null) {
					comment = currentCellComment.getString().getString();
				}
				String address = currentCell.getAddress().toString();
				String sheetName = currentCell.getSheet().getSheetName();
				SpreadSheetCellDAO mySpreadSheetCellDAO = new SpreadSheetCellDAO(formattedValue,comment,formula,address,sheetName);
				result[i]=mySpreadSheetCellDAO;
			}
		}
		
		// increase rows
		this.currentRow++;
		return result;
	}


	/**
	* Close parser and linked workbooks
	*
	*/
	@Override
	public void close() throws IOException {
		if (this.in!=null) {
			in.close();
		}
		if (this.currentWorkbook!=null) {
			LOG.debug("Closing current Workbook \""+this.fileName+"\"");
			this.currentWorkbook.close();
		}
		for (Workbook addedWorkbook: this.addedWorkbooks) {
			
			addedWorkbook.close();
		}
	
	}


}
