/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.hadoop.office.format.common.parser.msexcel.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.EventWorkbookBuilder.SheetRecordCollectingListener;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingRowDummyRecord;
import org.apache.poi.hssf.model.HSSFFormulaParser;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.ExtendedFormatRecord;
import org.apache.poi.hssf.record.FormatRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RowRecord;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.util.msexcel.MSExcelUtil;

/**
 * @author jornfranke
 *
 */

/** Adapted the Apache POI HowTos 
 * https://poi.apache.org/spreadsheet/how-to.html
 * 
 * **/
public class HSSFEventParser implements HSSFListener {
	private static final Log LOG = LogFactory.getLog(HSSFEventParser.class.getName());
	private Map<Integer,List<SpreadSheetCellDAO[]>> spreadSheetCellDAOCache; 
	private List<String> sheetList;
	private Map<Integer,Boolean> sheetMap;
	private Map<Integer,Long> sheetSizeMap;
	private List<Integer> extendedRecordFormatIndexList;
	private Map<Integer,String> formatRecordIndexMap;
	private DataFormatter useDataFormatter;
	private int currentSheet;
	private String[] sheets;
	private long currentCellNum;
	private int cachedRowNum;
	private short cachedColumnNum;
	private SSTRecord currentSSTrecord;
	private SheetRecordCollectingListener workbookBuildingListener;
	private HSSFWorkbook stubWorkbook;

	public HSSFEventParser(List<String> sheetNameList,DataFormatter useDataFormatter, Map<Integer,List<SpreadSheetCellDAO[]>> spreadSheetCellDAOCache, String[] sheets) {
		this.spreadSheetCellDAOCache=spreadSheetCellDAOCache;
		this.sheets=sheets;
		this.currentCellNum=0L;
		this.cachedRowNum=0;
		this.cachedColumnNum=0;
		this.sheetList=new ArrayList<>();
		this.sheetMap=new HashMap<>();
		this.sheetSizeMap=new HashMap<>();
		this.currentSheet=0;
		this.extendedRecordFormatIndexList=new ArrayList<>();
		this.formatRecordIndexMap=new HashMap<>();
		this.sheetList=sheetNameList;
		this.useDataFormatter=useDataFormatter;
	}
	
	public void setSheetRecordCollectingListener(SheetRecordCollectingListener listener) {
		this.workbookBuildingListener=listener;
		
	}

	@Override
	public void processRecord(Record record) {
		switch (record.getSid()) // one should note that these do not arrive necessary in linear order. First all the sheets are processed. Then all the rows of the sheets
        {
            // the BOFRecord can represent either the beginning of a sheet or the workbook
            case BOFRecord.sid:
                BOFRecord bof = (BOFRecord) record;
                if (bof.getType() == bof.TYPE_WORKBOOK)
                {
                    // ignored
                	if ((stubWorkbook==null) && (workbookBuildingListener!=null)){
                		stubWorkbook= workbookBuildingListener.getStubHSSFWorkbook();
                	} else {
                		LOG.error("Cannot create stub network. Formula Strings cannot be parsed");
                	}
                } else if (bof.getType() == bof.TYPE_WORKSHEET)
                {
                    // ignored
                }
                break;
            case BoundSheetRecord.sid:
                BoundSheetRecord bsr = (BoundSheetRecord) record;
                String currentSheet=bsr.getSheetname();
                LOG.debug("Sheet found: "+currentSheet);
                if (this.sheets==null) { // no sheets filter
                	// ignore the filter
                	this.sheetMap.put(this.sheetList.size(), true);
                	this.sheetSizeMap.put(this.sheetList.size(), 0L);
                	this.sheetList.add(currentSheet);
                } else
                if (currentSheet!=null) { // sheets filter
                 	boolean found=false;
                    for(int i=0;i<this.sheets.length;i++) {
                    	if (currentSheet.equals(this.sheets[i])) {
                    		found=true;
                    		break;
                    	}
                    }
                    this.sheetMap.put(this.sheetList.size(), found);
                    this.sheetList.add(currentSheet);
                } 
                if (this.sheetMap.get(this.sheetList.size()-1)) { // create sheet
                	 this.spreadSheetCellDAOCache.put(this.sheetList.size()-1, new ArrayList<SpreadSheetCellDAO[]>());
                }
                break;
            case RowRecord.sid:
            	  RowRecord rowRec = (RowRecord) record;
	              LOG.debug("Row found. Number of Cells: "+rowRec.getLastCol());
	              if ((this.currentSheet==0) && (rowRec.getRowNumber()==0)) { // first sheet
	            	  // special handling first sheet
	            	  this.currentSheet++;
	            	  this.currentCellNum=0;
	              } else
	              if ((this.currentSheet>0) && (rowRec.getRowNumber()==0)) { // new sheet
	            	  LOG.debug("Sheet number : "+this.currentSheet+" total number of cells "+this.currentCellNum);
	            	  this.sheetSizeMap.put(this.currentSheet-1, this.currentCellNum);
	            	  this.currentSheet++; // start processing next sheet
	            	  this.currentCellNum=0;
	              }
	              // create row if this sheet is supposed to be parsed
	              if (this.sheetMap.get(this.currentSheet-1)) {
	            	  this.spreadSheetCellDAOCache.get(this.currentSheet-1).add(new SpreadSheetCellDAO[rowRec.getLastCol()]);
	              }
	              this.currentCellNum+=rowRec.getLastCol();
                break;
            case FormulaRecord.sid:
            	LOG.debug("Formula Record found");
            	// check if formula has a cached value
            	FormulaRecord formRec=(FormulaRecord) record;
            	
            	/** check if this one should be parsed **/
            	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
            		break;
            	}
            	/** **/
            	String formulaString = "";
            	if (this.stubWorkbook!=null) {
            		formulaString=HSSFFormulaParser.toFormulaString(stubWorkbook, formRec.getParsedExpression());
            	}
            	if (formRec.hasCachedResultString()) {
           
            		this.cachedColumnNum=formRec.getColumn();
            		this.cachedRowNum=formRec.getRow();
            	} else {
            		// try to read the result
            		if (formRec.getColumn()>=this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(formRec.getRow()).length) {
            			LOG.error("More cells in row than expected. Row number:"+(formRec.getRow())+"Column number: "+formRec.getColumn()+"row length "+this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(formRec.getRow()).length);
                    	
            		} else {

            			int formatIndex= this.extendedRecordFormatIndexList.get(formRec.getXFIndex());
            			String theNumber=this.useDataFormatter.formatRawCellContents(formRec.getValue(), formatIndex, this.formatRecordIndexMap.get(formatIndex));
            			this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(formRec.getRow())[formRec.getColumn()]=new SpreadSheetCellDAO(theNumber,"",formulaString,MSExcelUtil.getCellAddressA1Format(formRec.getRow(), formRec.getColumn()),this.sheetList.get(this.currentSheet-1));          			
            		}
            	}
            	break;
            case StringRecord.sid:  // read cached formula results, if available
            	LOG.debug("String Record found");
            	StringRecord strRec=(StringRecord) record;
            	/** check if this one should be parsed **/
               	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
            		break;
            	}
            	/** **/
              this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(this.cachedRowNum)[this.cachedColumnNum]=new SpreadSheetCellDAO(strRec.getString(),"","",MSExcelUtil.getCellAddressA1Format(this.cachedRowNum,this.cachedColumnNum),this.sheetList.get(this.currentSheet-1));          			
    	        
            	
            	break;
            case NumberRecord.sid: // read number result
            	LOG.debug("Number Record found");
            	
                NumberRecord numrec = (NumberRecord) record;
           
            
                /** check if this one should be parsed **/
               	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
            		break;
            	}
            	/** **/
            	// try to read the result
        		if (numrec.getColumn()>=this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(numrec.getRow()).length) {
        			LOG.error("More cells in row than expected. Row number:"+(numrec.getRow())+"Column number: "+numrec.getColumn()+"row length "+this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(numrec.getRow()).length);
        		} else {
        			// convert the number in the right format (can be date etc.)
        			int formatIndex= this.extendedRecordFormatIndexList.get(numrec.getXFIndex());
        			String theNumber=this.useDataFormatter.formatRawCellContents(numrec.getValue(), formatIndex, this.formatRecordIndexMap.get(formatIndex));
        			SpreadSheetCellDAO mySpreadSheetCellDAO =   new SpreadSheetCellDAO(theNumber,"","",MSExcelUtil.getCellAddressA1Format(numrec.getRow(),numrec.getColumn()),this.sheetList.get(this.currentSheet-1));

        			this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(numrec.getRow())[numrec.getColumn()]=mySpreadSheetCellDAO;          		
        		}
                break;
                // SSTRecords store a array of unique strings used in Excel. (one per sheet?)
            case SSTRecord.sid:
            	LOG.debug("SST record found");
          
                this.currentSSTrecord=(SSTRecord) record;
                break;
            case LabelSSTRecord.sid: // get the string out of unique string value table 
            	LOG.debug("Label found");
                LabelSSTRecord lrec = (LabelSSTRecord) record;
              	/** check if this one should be parsed **/
               	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
            		break;
            	}
            	/** **/
            	if (lrec.getColumn()>=this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(lrec.getRow()).length) {
            		LOG.error("More cells in row than expected. Row number:"+(lrec.getRow())+"Column number: "+lrec.getColumn()+"row length "+this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(lrec.getRow()).length);
                	
        		} else {
        			if ((lrec.getSSTIndex()<0) || (lrec.getSSTIndex()>=this.currentSSTrecord.getNumUniqueStrings())) {
        				LOG.error("Invalid SST record index. Cell ignored");
        			} else {
        				   this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(lrec.getRow())[lrec.getColumn()]=new SpreadSheetCellDAO(this.currentSSTrecord.getString(lrec.getSSTIndex()).getString(),"","",MSExcelUtil.getCellAddressA1Format(lrec.getRow(),lrec.getColumn()),this.sheetList.get(this.currentSheet-1));          		
        	            	
        				
        			}
        		}
                break;
            case ExtendedFormatRecord.sid:
            	LOG.debug("Found extended format record");
            	ExtendedFormatRecord nfir = (ExtendedFormatRecord)record;
            	this.extendedRecordFormatIndexList.add((int)nfir.getFormatIndex());
               	
            	
            	break;
            case FormatRecord.sid:
            	LOG.debug("Found format record");
            	FormatRecord frec = (FormatRecord)record;
            	this.formatRecordIndexMap.put(frec.getIndexCode(),frec.getFormatString());
            	
            	break;
            	
          default:
        	  //LOG.debug("Ignored record: "+record.getSid());
        	  break;    
        }
		if (record instanceof MissingRowDummyRecord) { // this is an empty row in the Excel
			MissingRowDummyRecord emptyRow = (MissingRowDummyRecord)record;
			 LOG.debug("Detected Empty row");
              if ((this.currentSheet==0) && (emptyRow.getRowNumber()==0)) { // first sheet
            	  // special handling first sheet
            	  this.currentSheet++;
            	  this.currentCellNum=0;
              } else
              if ((this.currentSheet>0) && (emptyRow.getRowNumber()==0)) { // new sheet
            	  LOG.debug("Sheet number : "+this.currentSheet+" total number of cells "+this.currentCellNum);
            	  this.sheetSizeMap.put(this.currentSheet-1, this.currentCellNum);
            	  this.currentSheet++; // start processing next sheet
            	  this.currentCellNum=0;
              }
              // create empty row if this sheet is supposed to be parsed
              if (this.sheetMap.get(this.currentSheet-1)) {
            	  this.spreadSheetCellDAOCache.get(this.currentSheet-1).add(new SpreadSheetCellDAO[0]);
              }
		}
		
		
	}
	
}
