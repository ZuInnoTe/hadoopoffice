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
import java.util.Date;

import java.text.ParseException;
import java.text.Format;
import java.text.SimpleDateFormat;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;


import java.security.GeneralSecurityException;

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
import org.apache.poi.poifs.filesystem.OPOIFSFileSystem;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.poifs.crypt.HashAlgorithm;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.POIXMLProperties;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.openxml4j.util.Nullable;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.parser.MSExcelParser;

public class MSExcelWriter implements OfficeSpreadSheetWriterInterface {
public final static String FORMAT_OOXML = "ooxmlexcel";
public final static String FORMAT_OLD = "oldexcel";
protected final static String[] VALID_FORMAT = {FORMAT_OOXML, FORMAT_OLD};
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
private Map<String,String> linkedWorkbooksPasswords;
private Map<String,String> metadata;
private String password;
private CipherAlgorithm encryptAlgorithmCipher;
private HashAlgorithm hashAlgorithmCipher;
private EncryptionMode encryptionModeCipher;
private ChainingMode chainModeCipher;

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
* @param password Password of this document (null if no password)
* @param encryptAlgorithm algorithm use for encryption. It is recommended to carefully choose the algorithm. Supported for .xlsx: aes128,aes192,aes256,des,des3,des3_112,rc2,rc4,rsa. Support for .xls: rc4
* @param hashAlgorithm Hash algorithm,  Supported for .xslx: sha512, sha384, sha256, sha224, whirlpool, sha1, ripemd160,ripemd128,  md5, md4, md2,none. Ignored for .xls
* @param encryptMode Encrypt mode, Supported for .xlsx: binaryRC4,standard,agile,cryptoAPI. Ignored for .xls
* @param chainMode Chain mode, Supported for .xlsx: cbc, cfb, ecb.  Ignored for .xls 
* @param metadata properties as metadata.  Currently the following are supported for .xlsx documents: category,contentstatus, contenttype,created (date),creator,description,identifier,keywords,lastmodifiedbyuser,lastprinted (date),modified (date),revision,subject,title. Everything refering to a date needs to be in the format (EEE MMM dd hh:mm:ss zzz yyyy) see http://docs.oracle.com/javase/7/docs/api/java/util/Date.html#toString() Additionally all custom.* are defined as custom properties. Example custom.myproperty. 
 Currently the following are supported for .xls documents: applicationname,author,charcount, comments, createdatetime,edittime,keywords,lastauthor,lastprinted,lastsavedatetime,pagecount,revnumber,security,subject,template,title,wordcount
*
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case the writer is not configured correctly 
*
*/

public MSExcelWriter(String excelFormat, Locale useLocale, boolean ignoreMissingLinkedWorkbooks, String fileName, String commentAuthor, int commentWidth, int commentHeight,String password,String encryptAlgorithm,  String hashAlgorithm, String encryptMode, String chainMode, Map<String,String> metadata) throws InvalidWriterConfigurationException {
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
	this.password=password;
	this.encryptAlgorithmCipher=getAlgorithmCipher(encryptAlgorithm);
	this.hashAlgorithmCipher=getHashAlgorithm(hashAlgorithm);
	this.encryptionModeCipher=getEncryptionModeCipher(encryptMode);
	this.chainModeCipher=getChainMode(chainMode);
	this.metadata=metadata;
}



/**
* Creates a new Excel workbook for the output stream. Note: You need to save it AFTER adding the cells via addSpreadSheetCell using the method finalizeWrite
*
* @param oStream OutputStream where the Workbook should be written when calling finalizeWrite
* @param linkedWorkbooks linked workbooks that are already existing and linked to this spreadsheet
* @param linkedWorkbooksPasswords a map of passwords and linkedworkbooks. The key is the filename without path of the linkedworkbook and the value is the password
*
* @throws java.io.IOException if there is an issue with the OutputStream
*
*/

public void create(OutputStream oStream, Map<String,InputStream> linkedWorkbooks,Map<String,String> linkedWorkbooksPasswords) throws IOException,FormatNotUnderstoodException, GeneralSecurityException {
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
			MSExcelParser currentLinkedWorkbookParser = new MSExcelParser(this.useLocale, null, this.ignoreMissingLinkedWorkbooks,name,linkedWorkbooksPasswords.get(name),null);
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
* @throws java.security.GeneralSecurityException in case of issues encrypting
*
*
*/

public void finalizeWrite() throws IOException, GeneralSecurityException {
	try {
	// prepare metadata
	prepareMetaData();
	// write
	if (this.oStream!=null) {
		if (this.password==null) { // no encryption
			this.currentWorkbook.write(this.oStream);
			this.oStream.close();
		} else {	// encryption
			if (this.currentWorkbook instanceof HSSFWorkbook) { // old Excel format
				Biff8EncryptionKey.setCurrentUserPassword(this.password);
				this.currentWorkbook.write(this.oStream);
				this.oStream.close();
			} else if (this.currentWorkbook instanceof XSSFWorkbook) {
				if (this.encryptAlgorithmCipher==null) {
					LOG.error("No encryption algorithm specified");
				} else
				if (this.hashAlgorithmCipher==null) {
					LOG.error("No hash algorithm specified");
				} else
				if (this.encryptionModeCipher==null) {
					LOG.error("No encryption mode specified");
				} else
				if (this.chainModeCipher==null) {
					LOG.error("No chain mode specified");
				} else {
					OPOIFSFileSystem documentFileSystem = new OPOIFSFileSystem();
					EncryptionInfo info = new EncryptionInfo(this.encryptionModeCipher, this.encryptAlgorithmCipher, this.hashAlgorithmCipher, -1, -1, this.chainModeCipher);
					Encryptor enc = info.getEncryptor();
					enc.confirmPassword(this.password);
					OPCPackage opc = ((XSSFWorkbook)this.currentWorkbook).getPackage();
					OutputStream os = enc.getDataStream(documentFileSystem);
					opc.save(os);
					opc.close();
					documentFileSystem.writeFilesystem(this.oStream);
					this.oStream.close();
				}
			} else {
				LOG.error("Could not write encrypted workbook, because type of workbook is unknown");
			}
		}
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

/**
* Returns the CipherAlgorithm object matching the String.
*
* @param encryptAlgorithm encryption algorithm
*
*@return CipherAlgorithm object corresponding to encryption algorithm. Null if does not correspond to any algorithm.
*
*
*/

private static CipherAlgorithm getAlgorithmCipher(String encryptAlgorithm) {
	if (encryptAlgorithm==null) return null;
	switch (encryptAlgorithm) {
		case "aes128": return CipherAlgorithm.aes128;
		case "aes192": return CipherAlgorithm.aes192;
		case "aes256": return CipherAlgorithm.aes256;
		case "des": return CipherAlgorithm.des;
		case "des3": return CipherAlgorithm.des3;
		case "des3_112": return CipherAlgorithm.des3_112;
		case "rc2": return CipherAlgorithm.rc2;
		case "rc4": return CipherAlgorithm.rc4;
		case "rsa": return CipherAlgorithm.rsa;
	}
	return null;
}

/**
* Returns the HashAlgorithm object matching the String.
*
* @param hashAlgorithm hash algorithm
*
*@return HashAlgorithm object corresponding to hash algorithm. Null if does not correspond to any algorithm.
*
*
*/
private static HashAlgorithm getHashAlgorithm(String hashAlgorithm) {
	if (hashAlgorithm==null) return null;
	switch (hashAlgorithm) {
		case "md2": return HashAlgorithm.md2;
		case "md4": return HashAlgorithm.md4;
		case "md5": return HashAlgorithm.md5;
		case "none": return HashAlgorithm.none;
		case "ripemd128": return HashAlgorithm.ripemd128;
		case "ripemd160": return HashAlgorithm.ripemd160;
		case "sha1": return HashAlgorithm.sha1;
		case "sha224": return HashAlgorithm.sha224;
		case "sha256": return HashAlgorithm.sha256;
		case "sha384": return HashAlgorithm.sha384;
		case "sha512": return HashAlgorithm.sha512;
		case "whirlpool": return HashAlgorithm.whirlpool;
	}
	return null;
}



/**
* Returns the EncryptionMode object matching the String.
*
* @param encryptionMode encryption mode
*
*@return EncryptionMode object corresponding to encryption mode. Null if does not correspond to any mode.
*
*
*/


private static EncryptionMode getEncryptionModeCipher(String encryptionMode) {
	if (encryptionMode==null) return null;
	switch (encryptionMode) {
		case "agile": return EncryptionMode.agile;
		case "binaryRC4": return EncryptionMode.binaryRC4;
		case "cryptoAPI": return EncryptionMode.cryptoAPI;
		case "standard": return EncryptionMode.standard;
		//case "xor": return EncryptionMode.xor; // does not seem to be supported anymore
	}
	return null;
}


/**
* Returns the ChainMode object matching the String.
*
* @param chainMode chain mode
*
*@return ChainMode object corresponding to chain mode. Null if does not correspond to any mode.
*
*
*/


private static ChainingMode getChainMode(String chainMode) {
	if (chainMode==null) return null;
	switch (chainMode) {
		case "cbc": return ChainingMode.cbc;
		case "cfb": return ChainingMode.cfb;
		case "ecb": return ChainingMode.ecb;

	}
	return null;
}


/**
* writes metadata into document
*
*
*/

private void prepareMetaData() {
	if (this.currentWorkbook instanceof HSSFWorkbook) {
		prepareHSSFMetaData();
	} else if (this.currentWorkbook instanceof HSSFWorkbook) {
		prepareXSSFMetaData();
	} else {
		LOG.error("Unknown workbook type. Cannot write metadata.");
	}
}

/**
*
* Write metadata into HSSF document
*
*/
private void prepareHSSFMetaData() {
	HSSFWorkbook currentHSSFWorkbook = (HSSFWorkbook) this.currentWorkbook;
	SummaryInformation summaryInfo = currentHSSFWorkbook.getSummaryInformation(); 
	SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy"); // this is the format of the toString method of date used in the parser. Just to be consistent...http://docs.oracle.com/javase/7/docs/api/java/util/Date.html#toString()
	for (String currentKey: this.metadata.keySet()) {
		// process general properties
		try {
		switch(currentKey) {
			case "applicationame": summaryInfo.setApplicationName(this.metadata.get(currentKey)); break;
			case "author": summaryInfo.setAuthor(this.metadata.get(currentKey)); break;
			case "charcount": summaryInfo.setCharCount(Integer.parseInt(this.metadata.get(currentKey))); break;
			case "comments": summaryInfo.setComments(this.metadata.get(currentKey)); break;
			case "createdatetime": summaryInfo.setCreateDateTime(format.parse(this.metadata.get(currentKey))); break;
			case "edittime": summaryInfo.setEditTime(Long.parseLong(this.metadata.get(currentKey))); break;
			case "keywords": summaryInfo.setKeywords(this.metadata.get(currentKey)); break;
			case "lastauthor": summaryInfo.setLastAuthor(this.metadata.get(currentKey)); break;
			case "lastprinted": summaryInfo.setLastPrinted(format.parse(this.metadata.get(currentKey))); break;	
			case "lastsavedatetime": summaryInfo.setLastSaveDateTime(format.parse(this.metadata.get(currentKey))); break;
			case "pagecount": summaryInfo.setPageCount(Integer.parseInt(this.metadata.get(currentKey))); break;
			case "revnumber": summaryInfo.setRevNumber(this.metadata.get(currentKey)); break;
			case "security": summaryInfo.setSecurity(Integer.parseInt(this.metadata.get(currentKey))); break;
			case "subject": summaryInfo.setSubject(this.metadata.get(currentKey)); break;
			case "template": summaryInfo.setTemplate(this.metadata.get(currentKey)); break;	
			case "title": summaryInfo.setTitle(this.metadata.get(currentKey)); break;
			case "wordcount": summaryInfo.setWordCount(Integer.parseInt(this.metadata.get(currentKey))); break;
			default: LOG.warn("Unknown metadata key: "+currentKey); break;	
		} 
		} catch (ParseException pe) {
			LOG.error(pe);
		}

	}
}

/**
*
* Write metadata into XSSF document
*
*/

private void prepareXSSFMetaData() {
 	XSSFWorkbook currentXSSFWorkbook = (XSSFWorkbook) this.currentWorkbook;
        POIXMLProperties props = currentXSSFWorkbook.getProperties();
	POIXMLProperties.CoreProperties coreProp=props.getCoreProperties();
	POIXMLProperties.CustomProperties custProp = props.getCustomProperties();
	SimpleDateFormat format = new SimpleDateFormat(MSExcelParser.DATE_FORMAT); 
	for (String currentKey: this.metadata.keySet()) {
		// process general properties
		try {
		switch(currentKey) {
			case "category": coreProp.setCategory(this.metadata.get(currentKey)); break;
			case "contentstatus": coreProp.setContentStatus(this.metadata.get(currentKey)); break;
			case "contenttype": coreProp.setContentType(this.metadata.get(currentKey)); break;
			case "created": coreProp.setCreated(new Nullable<Date>(format.parse(this.metadata.get(currentKey)))); break;
			case "creator": coreProp.setCreator(this.metadata.get(currentKey)); break;
			case "description": coreProp.setDescription(this.metadata.get(currentKey)); break;
			case "identifier": coreProp.setIdentifier(this.metadata.get(currentKey)); break;
			case "keywords": coreProp.setKeywords(this.metadata.get(currentKey)); break;
			case "lastmodifiedbyuser": coreProp.setLastModifiedByUser(this.metadata.get(currentKey)); break;
			case "lastprinted": coreProp.setLastPrinted(new Nullable<Date>(format.parse(this.metadata.get(currentKey)))); break;
			case "modified": coreProp.setLastPrinted(new Nullable<Date>(format.parse(this.metadata.get(currentKey)))); break;
			case "revision": coreProp.setRevision(this.metadata.get(currentKey)); break;
			case "subject": coreProp.setSubjectProperty(this.metadata.get(currentKey)); break;
			case "title": coreProp.setTitle(this.metadata.get(currentKey)); break;
		}
		// process custom properties
		if (currentKey.startsWith("custom.")==true) {
			String strippedKey=currentKey.substring("custom.".length());
			if (strippedKey.length()>0) {
				custProp.addProperty(strippedKey,this.metadata.get(currentKey));
			}
		} else {
			LOG.warn("Unknown metadata key: "+currentKey);
		}
		} catch (ParseException pe) {
			LOG.error(pe);
		}
	}
}

}
