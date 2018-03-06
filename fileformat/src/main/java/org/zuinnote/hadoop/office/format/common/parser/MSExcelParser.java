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

import java.io.InputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;


import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.poi.hssf.model.InternalWorkbook;
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
import org.apache.poi.xssf.model.ExternalLinksTable;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.POIXMLProperties;
import org.apache.poi.hpsf.SummaryInformation;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.dsig.SignatureConfig;
import org.apache.poi.poifs.crypt.dsig.SignatureInfo;
import org.apache.poi.poifs.crypt.dsig.SignatureInfo.SignaturePart;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.lang.reflect.Method;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.lang.reflect.InvocationTargetException;

import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.util.CertificateChainVerificationUtil;

/*
*
* This class is responsible for parsing Excel content in OOXML format and old excel format
*
*/

public class MSExcelParser implements OfficeReaderParserInterface {
private static final String MIMETYPE_EXCEL_BINARY_ID = "ms-excel.sheet.binary"; // application/vnd.ms-excel.sheet.binary.macroEnabled.12
private static final String MATCH_ALL = "matchAll";
private static final String NOT_MATCHING = "Not matching: ";
private static final String COULD_NOT_RETRIEVE_LINKED_WORKBOOKS_FOR_OLD_EXCEL_FORMAT = "Could not retrieve linked workbooks for old Excel format.";
private static final Log LOG = LogFactory.getLog(MSExcelParser.class.getName());
public static final String DATE_FORMAT = "hh:mm:ss dd.MM.yyyy";
public static final int MAX_LINKEDWB_OLDEXCEL=100;
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

private boolean filtered=false;
private HadoopOfficeReadConfiguration hocr;
private String[] header;
private int currentSkipLine=0;
private boolean currentSheetSkipped=false;
	/*
	* In the default case all sheets are parsed one after the other.
	* @param hocr HadoopOffice configuration for reading files:
	* locale to use (if null then default locale will be used), see java.util.Locale
	* filename Filename of the document
	* ignoreMissingLinkedWorkbooks ignore missing linked Workbooks
	* password Password of this document (null if no password)
	* metadataFilter filter on metadata. The name is the metadata attribute name and the property is a filter which contains a regular expression. Currently the following are supported for .xlsx documents: category,contentstatus, contenttype,created,creator,description,identifier,keywords,lastmodifiedbyuser,lastprinted,modified,revision,subject,title. Additionally all custom.* are defined as custom properties. Example custom.myproperty. Finally, matchAll can be set to true (all metadata needs to be matched), or false (at least one of the metadata item needs to match).
 Currently the following are supported for .xls documents: applicationname,author,charcount, comments, createdatetime,edittime,keywords,lastauthor,lastprinted,lastsavedatetime,pagecount,revnumber,security,subject,template,title,wordcount. Finally, matchAll can be set to true (all metadata needs to be matched), or false (at least one of the metadata item needs to match).
	*
	*/



	public MSExcelParser(HadoopOfficeReadConfiguration hocr) {
		this(hocr, null);
	}

	/*
	*
	* Only process selected sheets (one after the other)
	*
	* @param hocr HadoopOffice configuration for reading files:
	* locale to use (if null then default locale will be used), see java.util.Locale
	* ignoreMissingLinkedWorkbooks ignore missing linked Workbooks
	* password Password of this document (null if no password)
	* metadataFilter filter on metadata. The name is the metadata attribute name and the property is a filter which contains a regular expression. Currently the following are supported for .xlsx documents: category,contentstatus, contenttype,created,creator,description,identifier,keywords,lastmodifiedbyuser,lastprinted,modified,revision,subject,title. Additionally all custom.* are defined as custom properties. Example custom.myproperty. Finally, matchAll can be set to true (all metadata needs to be matched), or false (at least one of the metadata item needs to match).
 Currently the following are supported for .xls documents: applicationname,author,charcount, comments, createdatetime,edittime,keywords,lastauthor,lastprinted,lastsavedatetime,pagecount,revnumber,security,subject,template,title,wordcount. Finally, matchAll can be set to true (all metadata needs to be matched), or false (at least one of the metadata item needs to match).
	* @param sheets selecrted sheets
	*
	*/
	public MSExcelParser(HadoopOfficeReadConfiguration hocr, String[] sheets) {
		this.sheets=sheets;
		this.hocr=hocr;
		if (hocr.getLocale()==null)  {
			useDataFormatter=new DataFormatter(); // use default locale
		} else {
			useDataFormatter=new DataFormatter(hocr.getLocale());
		}
		
		this.addedFormulaEvaluators = new HashMap<>();
		this.addedWorkbooks = new ArrayList<>();
	}

	/*
	*
	* Parses the given InputStream containing Excel data. The type of InputStream (e.g. FileInputStream, BufferedInputStream etc.) does not matter here, but it is recommended to use an appropriate
	* type to avoid performance issues. 
	*
	* @param in InputStream containing Excel data
	*
	* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case there are issues reading from the Excel file, e.g. wrong password or unknown format
	*
	*/
	@Override
	public void parse(InputStream in) throws FormatNotUnderstoodException {
		// read xls
	
			try {
				this.currentWorkbook=WorkbookFactory.create(in,this.hocr.getPassword());
			} catch (EncryptedDocumentException | InvalidFormatException | IOException e) {
				LOG.error(e);
				throw new FormatNotUnderstoodException(e.toString());
			} finally 
			{
				if (this.in!=null) {
					try {
						this.in.close();
					} catch (IOException e) {
						LOG.error(e);
					}
				}
			}
			// check if signature should be verified
			if (this.hocr.getVerifySignature()) {
				LOG.info("Verifying signature of document");
				if (!(this.currentWorkbook instanceof XSSFWorkbook)) {
					throw new FormatNotUnderstoodException("Can only verify signatures for files using the OOXML (.xlsx) format");
				} else {
					//
					OPCPackage pgk = ((XSSFWorkbook)this.currentWorkbook).getPackage();
					SignatureConfig sic = new SignatureConfig();
					sic.setOpcPackage(pgk);
					SignatureInfo si = new SignatureInfo();
					si.setSignatureConfig(sic);
					if (!si.verifySignature()) {
						throw new FormatNotUnderstoodException("Cannot verify signature of OOXML (.xlsx) file: "+this.hocr.getFileName());
					} else {
						LOG.info("Successfully verifed first part signature of OXXML (.xlsx) file: "+this.hocr.getFileName());
					}
					 Iterator<SignaturePart> spIter = si.getSignatureParts().iterator();
					 while (spIter.hasNext()) {
						 SignaturePart currentSP = spIter.next();
						 if (!(currentSP.validate())) {
							 throw new FormatNotUnderstoodException("Could not validate all signature parts for file: "+this.hocr.getFileName());
						 } else {
							 X509Certificate currentCertificate = currentSP.getSigner();
							 try {
								if ((this.hocr.getX509CertificateChain().size()>0) && (!CertificateChainVerificationUtil.verifyCertificateChain(currentCertificate, this.hocr.getX509CertificateChain()))) {
									 throw new FormatNotUnderstoodException("Could not validate signature part for principal \""+currentCertificate.getSubjectX500Principal().getName()+"\" : "+this.hocr.getFileName());
								 }
							} catch (CertificateException | NoSuchAlgorithmException | NoSuchProviderException
									| InvalidAlgorithmParameterException e) {
								LOG.error("Could not validate signature part for principal \""+currentCertificate.getSubjectX500Principal().getName()+"\" : "+this.hocr.getFileName(), e);
								 throw new FormatNotUnderstoodException("Could not validate signature part for principal \""+currentCertificate.getSubjectX500Principal().getName()+"\" : "+this.hocr.getFileName());
									
							}
						 }
					 }
					 LOG.info("Successfully verifed all signatures of OXXML (.xlsx) file: "+this.hocr.getFileName());
				}
			}
		// formulaEvaluator
		 this.formulaEvaluator = this.currentWorkbook.getCreationHelper().createFormulaEvaluator();
		  // add the formulator evaluator of this file as well or we will see a strange Exception
		 this.addedFormulaEvaluators.put(this.hocr.getFileName(),this.formulaEvaluator);
		 this.formulaEvaluator.setIgnoreMissingWorkbooks(this.hocr.getIgnoreMissingLinkedWorkbooks());
		 this.filtered=this.checkFiltered();
		 this.currentRow=0;
		 if (this.sheets==null) {
			this.currentSheetName=this.currentWorkbook.getSheetAt(0).getSheetName();
		 } else if (sheets.length<1) {
			throw new FormatNotUnderstoodException("Error: no sheets selected");
		 } else  {
			this.currentSheetName=sheets[0];
		 }
		 // check header
		 if (this.hocr.getReadHeader()) {
			 LOG.debug("Reading header...");
			 Object[] firstRow = this.getNext();
			 if (firstRow!=null) {
				 this.header=new String[firstRow.length];
				 for (int i=0;i<firstRow.length;i++) {
					 this.header[i]=((SpreadSheetCellDAO)firstRow[i]).getFormattedValue();
				 }
			 } else {
				 this.header=new String[0];
			 }
		 }
		 // check skipping of additional lines
		 this.currentRow+=this.hocr.getSkipLines();
		 this.currentSheetSkipped=true;
	}

	/**
	* Adds a linked workbook that is referred from this workbook. If the filename is already in the list then it is not processed twice. Note that the inputStream is closed after parsing
	*
	* @param name fileName (without path) of the workbook
	* @param inputStream content of the linked workbook
	* @param password if document is encrypted, null if not encrypted
	*
	* @return true if it has been added, false if it has been already added
	*
	* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case there are issues reading from the Excel file
	*
	**/
	@Override
	public boolean addLinkedWorkbook(String name, InputStream inputStream, String password) throws FormatNotUnderstoodException {
		// check if already added
		if (this.addedFormulaEvaluators.containsKey(name)) {
			return false;
		}
		LOG.debug("Start adding  \""+name+"\" to current workbook");
		// create new parser, select all sheets, no linkedworkbookpasswords,no metadatafilter
		HadoopOfficeReadConfiguration linkedWBHOCR = new HadoopOfficeReadConfiguration();
		linkedWBHOCR.setLocale(this.hocr.getLocale());
		linkedWBHOCR.setSheets(null);
		linkedWBHOCR.setIgnoreMissingLinkedWorkbooks(this.hocr.getIgnoreMissingLinkedWorkbooks());
		linkedWBHOCR.setFileName(name);
		linkedWBHOCR.setPassword(password);
		linkedWBHOCR.setMetaDataFilter(null);
		MSExcelParser linkedWBMSExcelParser = new MSExcelParser(linkedWBHOCR,null);
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
		List<String> result = new ArrayList<>();
		if (this.currentWorkbook instanceof HSSFWorkbook) {
			result = getLinkedWorkbooksHSSF();
    
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
	
	private List<String> getLinkedWorkbooksHSSF() {
		List<String> result = new ArrayList<>();
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
				while(i<MSExcelParser.MAX_LINKEDWB_OLDEXCEL) {
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
			LOG.error(COULD_NOT_RETRIEVE_LINKED_WORKBOOKS_FOR_OLD_EXCEL_FORMAT);
			LOG.error(nsme);
		}
		 catch (IllegalAccessException iae) {
			LOG.error(COULD_NOT_RETRIEVE_LINKED_WORKBOOKS_FOR_OLD_EXCEL_FORMAT);
			LOG.error(iae);
		}
		catch (InvocationTargetException ite) {
			LOG.error(COULD_NOT_RETRIEVE_LINKED_WORKBOOKS_FOR_OLD_EXCEL_FORMAT);
			LOG.error(ite);
		}
		return result;
	}

	/**
	* Check if document matches to a metadata filter
	*
	* @return true, if document matches metadata filter, false if not
	*
	*/
	@Override
	public boolean getFiltered() {
		return this.filtered;
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
			if (!nextAllSheets()) {
				return result;
			}
		} else { // go on with specified sheets
			if (!nextSpecificSheets()) {
				return result;
			}
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
	
	private boolean nextAllSheets() {
		while (this.currentRow>this.currentWorkbook.getSheetAt(this.currentSheet).getLastRowNum()) { // end of row reached? => next sheet
			this.currentSheet++;
			this.currentRow=0;
			// check if we need to skip header
			if (this.hocr.getIgnoreHeaderInAllSheets()) {
				this.currentRow++;
			}
			// check if we need to skip lines
			if (this.hocr.getSkipLinesAllSheets()) {
				this.currentRow+=this.hocr.getSkipLines();
			}
			if (this.currentSheet>=this.currentWorkbook.getNumberOfSheets()) {
				return false; // no more sheets available?
			}
			this.currentSheetName=this.currentWorkbook.getSheetAt(this.currentSheet).getSheetName();
		}
		return true;
	}
	
	private boolean nextSpecificSheets() {
		// go through sheets specified until one found
					while (this.sheetsIndex!=this.sheets.length) {
						if (this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex])==null) { // log only if sheet not found
							LOG.warn("Sheet \""+this.sheets[this.sheetsIndex]+"\" not found");
						} else { // sheet found, check number of rows
						   if (this.currentRow<=this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex]).getLastRowNum()) {
						 // we have a sheet where we still need to process rows
							this.currentSheet=this.currentWorkbook.getSheetIndex(this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex]));
							this.currentSheetName=this.currentWorkbook.getSheetAt(this.currentSheet).getSheetName();
							break;
						   }
						}
						while (this.currentRow>this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex]).getLastRowNum()) {
							this.sheetsIndex++;
							this.currentRow=0;
							// check if we need to skip header
							if (this.hocr.getIgnoreHeaderInAllSheets()) {
								this.currentRow++;
							}
							// check if we need to skip lines
							if (this.hocr.getSkipLinesAllSheets()) {
								this.currentRow+=this.hocr.getSkipLines();
							}
						}
					}
					if (this.sheetsIndex>=this.sheets.length) {
						return false; // all sheets processed
					}
					return true;
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
			LOG.debug("Closing current Workbook \""+this.hocr.getFileName()+"\"");
			this.currentWorkbook.close();
		}
		for (Workbook addedWorkbook: this.addedWorkbooks) {
			
			addedWorkbook.close();
		}
	
	}


	/*
	* Check if document matches the metadata filters
	*
	* @return true, if it matches, false if not
	*
	*/
	private boolean checkFiltered() {	
		if ((this.hocr.getMetaDataFilter()==null) || (this.hocr.getMetaDataFilter().size()==0)) { // if no filter is defined it does match by definition
			return true;
		}
		if (this.currentWorkbook instanceof XSSFWorkbook) {
			return checkFilteredXSSF();
		} else if (this.currentWorkbook instanceof HSSFWorkbook) {
			return checkFilteredHSSF();
		} else	{
			LOG.error("Unknown workbook format. Cannot check if document matches metadata filter");
			return false;
		}
	}

	/*
	* Check if document matches the metadata filters for XSSF documents. 
	*
	* @return true, if it matches, false if not
	*
	*/
	private boolean checkFilteredXSSF() {
		XSSFWorkbook currentXSSFWorkbook = (XSSFWorkbook) this.currentWorkbook;
     		POIXMLProperties props = currentXSSFWorkbook.getProperties();
		SimpleDateFormat format = new SimpleDateFormat(MSExcelParser.DATE_FORMAT); 
		// check for each defined property
		// check if we need to match all
		boolean matchAll=true;
		boolean matchFull=true;
		boolean matchOnce=false;
		if (this.hocr.getMetaDataFilter().get(MATCH_ALL)!=null) {
			if ("true".equalsIgnoreCase(this.hocr.getMetaDataFilter().get(MATCH_ALL))) {
				matchAll=true;
				LOG.info("matching all metadata properties");
			} else if ("false".equalsIgnoreCase(this.hocr.getMetaDataFilter().get(MATCH_ALL)))	{
				matchAll=false;
				LOG.info("matching at least one metadata property");
			} else {
				LOG.error("Metadata property matchAll not defined correctly. Assuming that at only least one attribute needs to match");
			}
		}
		// check core properties
		String corePropertyName;
		POIXMLProperties.CoreProperties coreProp=props.getCoreProperties();
		corePropertyName="category";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getCategory();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
					LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="contentstatus";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getContentStatus();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="contenttype";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getContentType();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="created";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				Date corePropStr=coreProp.getCreated();
				if ((corePropStr!=null) && (format.format(corePropStr).matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+format.format(corePropStr)+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
						LOG.debug(corePropertyName);
				}
		}
		corePropertyName="creator";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getCreator();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="description";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getDescription();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="identifier";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getIdentifier();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="keywords";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getKeywords();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="lastmodifiedbyuser";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getLastModifiedByUser();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="lastprinted";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				Date corePropStr=coreProp.getLastPrinted();
				if ((corePropStr!=null) && (format.format(corePropStr).matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+format.format(corePropStr)+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
						LOG.debug(corePropertyName);
				}
		}
		corePropertyName="modified";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				Date corePropStr=coreProp.getModified();
				if ((corePropStr!=null) && (format.format(corePropStr).matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
						LOG.debug(corePropertyName);
				}
		}
		corePropertyName="revision";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getRevision();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="subject";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getSubject();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		corePropertyName="title";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
				String corePropStr=coreProp.getTitle();
				if ((corePropStr!=null) && (corePropStr.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
					matchOnce=true;
				 } else {
						matchFull=false;
						LOG.debug(NOT_MATCHING+corePropStr+":"+this.hocr.getMetaDataFilter().get(corePropertyName));
				}
		}
		// check for custom properties
		POIXMLProperties.CustomProperties custProp = props.getCustomProperties();
		for (Map.Entry<String,String> entry: this.hocr.getMetaDataFilter().entrySet()) {
			if (entry.getKey().startsWith("custom.")) {
				String strippedKey=entry.getKey().substring("custom.".length());
				if (strippedKey.length()>0) {
					String valueMatch=entry.getValue();
					if (valueMatch!=null) {
						
						if ((custProp.getProperty(strippedKey)!=null) && (custProp.getProperty(strippedKey).getName()!=null)&& (custProp.getProperty(strippedKey).getLpwstr().matches(valueMatch))) {
		matchOnce=true;
	} else {
		matchFull=false;
	}
					}
				}
			}
		}		
		
		if (!(matchAll)) {
			return  matchOnce;
		} else {
			return matchFull;
		}
	}
	
	/*
	* Check if document matches the metadata filters for HSSF documents
	*
	* @return true, if it matches, false if not
	*
	*/
	private boolean checkFilteredHSSF() {
		HSSFWorkbook currentHSSFWorkbook = (HSSFWorkbook) this.currentWorkbook;
		SummaryInformation summaryInfo = currentHSSFWorkbook.getSummaryInformation(); 
		boolean matchAll=true;

		boolean matchFull=true;
		boolean matchOnce=false;
		if (this.hocr.getMetaDataFilter().get(MATCH_ALL)!=null) {
			if ("true".equalsIgnoreCase(this.hocr.getMetaDataFilter().get(MATCH_ALL))) {
				matchAll=true;
			} else if ("false".equalsIgnoreCase(this.hocr.getMetaDataFilter().get(MATCH_ALL))) {
				matchAll=false;
			} else {
				LOG.error("Metadata property matchAll not defined correctly. Assuming that at only least one attribute needs to match");
			}
		}
		String corePropertyName;
		corePropertyName="applicationname";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getApplicationName();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="author";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getAuthor();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="charcount";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			int coreProp=summaryInfo.getCharCount();
			if (String.valueOf(coreProp).matches(this.hocr.getMetaDataFilter().get(corePropertyName))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="comments";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getComments();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="createddatetime";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			Date coreProp=summaryInfo.getCreateDateTime();
			if ((coreProp!=null) && (coreProp.toString().matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="edittime";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			long coreProp=summaryInfo.getEditTime();
			if (String.valueOf(coreProp).matches(this.hocr.getMetaDataFilter().get(corePropertyName))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="keywords";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getKeywords();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="lastauthor";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getLastAuthor();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="lastprinted";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			Date coreProp=summaryInfo.getLastPrinted();
			if ((coreProp!=null) && (coreProp.toString().matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="lastsavedatetime";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			Date coreProp=summaryInfo.getLastSaveDateTime();
			if ((coreProp!=null) && (coreProp.toString().matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="pagecount";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			int coreProp=summaryInfo.getPageCount();
			if (String.valueOf(coreProp).matches(this.hocr.getMetaDataFilter().get(corePropertyName))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="revnumber";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getRevNumber();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="security";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			int coreProp=summaryInfo.getSecurity();
			if (String.valueOf(coreProp).matches(this.hocr.getMetaDataFilter().get(corePropertyName))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="subject";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getSubject();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="template";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getTemplate();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="title";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			String coreProp=summaryInfo.getTitle();
			if ((coreProp!=null) && (coreProp.matches(this.hocr.getMetaDataFilter().get(corePropertyName)))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
		corePropertyName="wordcount";
		if (this.hocr.getMetaDataFilter().get(corePropertyName)!=null) {
			int coreProp=summaryInfo.getWordCount();
			if (String.valueOf(coreProp).matches(this.hocr.getMetaDataFilter().get(corePropertyName))) {
				matchOnce=true;
			} else {
				matchFull=false;
			}
		}
	
		if (!(matchAll)) {
			return  matchOnce;
		} else {
			return matchFull;
		}
	}

	@Override
	public void setCurrentRow(long row) {
		this.currentRow=(int) row;
		
	}

	@Override
	public void setCurrentSheet(long sheet) {
		this.currentSheet=(int) sheet;
		
	}

	@Override
	public long getCurrentSheet() {
		return this.currentSheet;
	}

	@Override
	public String[] getHeader() {
		return this.header;
	}

}
