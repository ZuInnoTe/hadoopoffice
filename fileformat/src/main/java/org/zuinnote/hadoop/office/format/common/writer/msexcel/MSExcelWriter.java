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

package org.zuinnote.hadoop.office.format.common.writer.msexcel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.xml.crypto.MarshalException;
import javax.xml.crypto.dsig.XMLSignatureException;

import java.util.HashMap;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;


import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ooxml.POIXMLProperties;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.poifs.crypt.HashAlgorithm;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.hpsf.SummaryInformation;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.MSExcelParser;
import org.zuinnote.hadoop.office.format.common.util.msexcel.MSExcelOOXMLSignUtil;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeSpreadSheetWriterInterface;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;

public class MSExcelWriter implements OfficeSpreadSheetWriterInterface {
public static final String FORMAT_OOXML = "ooxmlexcel";
public static final String FORMAT_OLD = "oldexcel";
protected static final String[] VALID_FORMAT = {FORMAT_OOXML, FORMAT_OLD};
private static final Log LOG = LogFactory.getLog(MSExcelWriter.class.getName());
private static final String DEFAULT_FORMAT = VALID_FORMAT[0];
private String format=DEFAULT_FORMAT;
private OutputStream oStream;
private Workbook currentWorkbook;
private Map<String,Drawing> mappedDrawings;
private List<Workbook> listOfWorkbooks;
private POIFSFileSystem ooxmlDocumentFileSystem;
private HadoopOfficeWriteConfiguration howc;
private CipherAlgorithm encryptAlgorithmCipher;
private HashAlgorithm hashAlgorithmCipher;
private EncryptionMode encryptionModeCipher;
private ChainingMode chainModeCipher;
private boolean hasTemplate;
private MSExcelOOXMLSignUtil signUtil;



/**
*
* Creates a new writer for MS Excel files
*
* @param excelFormat format of the Excel: ooxmlexcel: Excel 2007-2013 (.xlsx), oldexcel: Excel 2003 (.xls)
* @param howc HadoopOfficeWriteConfiguration
* locale Locale to be used to evaluate cells
* ignoreMissingLinkedWorkbooks if true then missing linked workbooks are ignored during writing, if false then missing linked workbooks are not ignored and need to be present
* fileName filename without path of the workbook
* commentAuthor default author for comments
* commentWidth width of comments in terms of number of columns
* commentHeight height of commments in terms of number of rows
* password Password of this document (null if no password)
* encryptAlgorithm algorithm use for encryption. It is recommended to carefully choose the algorithm. Supported for .xlsx: aes128,aes192,aes256,des,des3,des3_112,rc2,rc4,rsa. Support for .xls: rc4
* hashAlgorithm Hash algorithm,  Supported for .xslx: sha512, sha384, sha256, sha224, whirlpool, sha1, ripemd160,ripemd128,  md5, md4, md2,none. Ignored for .xls
* encryptMode Encrypt mode, Supported for .xlsx: binaryRC4,standard,agile,cryptoAPI. Ignored for .xls
* chainMode Chain mode, Supported for .xlsx: cbc, cfb, ecb.  Ignored for .xls 
* metadata properties as metadata.  Currently the following are supported for .xlsx documents: category,contentstatus, contenttype,created (date),creator,description,identifier,keywords,lastmodifiedbyuser,lastprinted (date),modified (date),revision,subject,title. Everything refering to a date needs to be in the format (EEE MMM dd hh:mm:ss zzz yyyy) see http://docs.oracle.com/javase/7/docs/api/java/util/Date.html#toString() Additionally all custom.* are defined as custom properties. Example custom.myproperty. 
 Currently the following are supported for .xls documents: applicationname,author,charcount, comments, createdatetime,edittime,keywords,lastauthor,lastprinted,lastsavedatetime,pagecount,revnumber,security,subject,template,title,wordcount
*
* @throws org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException in case the writer is not configured correctly 
*
*/

public MSExcelWriter(String excelFormat, HadoopOfficeWriteConfiguration howc) throws InvalidWriterConfigurationException {
	boolean formatFound=isSupportedFormat(excelFormat);
	if (!(formatFound)) {
		 LOG.error("Unknown Excel format: "+this.format);
		 throw new InvalidWriterConfigurationException("Unknown Excel format: "+this.format);
	}
	this.format=excelFormat;
	this.howc=howc;
	this.encryptAlgorithmCipher=getAlgorithmCipher(this.howc.getEncryptAlgorithm());
	this.hashAlgorithmCipher=getHashAlgorithm(this.howc.getHashAlgorithm());
	this.encryptionModeCipher=getEncryptionModeCipher(this.howc.getEncryptMode());
	this.chainModeCipher=getChainMode(this.howc.getChainMode());
	this.hasTemplate=false;

}



/**
* Creates a new Excel workbook for the output stream. Note: You need to save it AFTER adding the cells via addSpreadSheetCell using the method finalizeWrite
*
* @param oStream OutputStream where the Workbook should be written when calling finalizeWrite
* @param linkedWorkbooks linked workbooks that are already existing and linked to this spreadsheet
* @param linkedWorkbooksPasswords a map of passwords and linkedworkbooks. The key is the filename without path of the linkedworkbook and the value is the password
* @param template a template that should be use as base for the document to write, null if there is no template
* @throws org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException in  case of errors writing to the outputstream
*
*/
@Override
public void create(OutputStream oStream, Map<String,InputStream> linkedWorkbooks,Map<String,String> linkedWorkbooksPasswords, InputStream template) throws OfficeWriterException {
	this.oStream=oStream;
	// check if we should load a workbook from template
	if(template!=null){
		LOG.info("Loading template: "+this.howc.getTemplate());
		if (this.howc.getLowFootprint()) {
			LOG.warn("Low footprint mode is not supported with templates. Continuing normal mode");
		}
		this.hasTemplate=true;
		HadoopOfficeReadConfiguration currentTemplateHOCR = new HadoopOfficeReadConfiguration();
		currentTemplateHOCR.setLocale(this.howc.getLocale());
		currentTemplateHOCR.setSheets(null);
		currentTemplateHOCR.setIgnoreMissingLinkedWorkbooks(this.howc.getIgnoreMissingLinkedWorkbooks());
		currentTemplateHOCR.setMetaDataFilter(null);
		currentTemplateHOCR.setPassword(this.howc.getTemplatePassword());
		MSExcelParser currentTemplateParser = new MSExcelParser(currentTemplateHOCR,null);
		try {
			currentTemplateParser.parse(template);
		} catch (FormatNotUnderstoodException e) {
			LOG.error(e);
			LOG.error("Cannot read template");
			throw new OfficeWriterException(e.toString());
		}
		this.currentWorkbook=currentTemplateParser.getCurrentWorkbook();
	} else {
		// create a new Workbook either in old Excel or "new" Excel format
		if (this.format.equals(MSExcelWriter.FORMAT_OOXML)) {
			this.currentWorkbook=new XSSFWorkbook();
			this.ooxmlDocumentFileSystem = new POIFSFileSystem();					
		} else if (this.format.equals(MSExcelWriter.FORMAT_OLD)) {
			if (this.howc.getLowFootprint()) {
				LOG.warn("Low footprint mode writing is not supported for old Excel files (.xls). Continuing normal mode");
			}
			this.currentWorkbook=new HSSFWorkbook();
			((HSSFWorkbook)this.currentWorkbook).createInformationProperties();
		} 
	}
	FormulaEvaluator currentFormulaEvaluator=this.currentWorkbook.getCreationHelper().createFormulaEvaluator();
	HashMap<String,FormulaEvaluator> linkedFormulaEvaluators=new HashMap<>();
	linkedFormulaEvaluators.put(this.howc.getFileName(),currentFormulaEvaluator);
	this.mappedDrawings=new HashMap<>();
	// add current workbook to list of linked workbooks
	this.listOfWorkbooks=new ArrayList<>();
	// parse linked workbooks
	try {
		for (Map.Entry<String,InputStream> entry: linkedWorkbooks.entrySet()) {
			// parse linked workbook
			HadoopOfficeReadConfiguration currentLinkedWBHOCR = new HadoopOfficeReadConfiguration();
			currentLinkedWBHOCR.setLocale(this.howc.getLocale());
			currentLinkedWBHOCR.setSheets(null);
			currentLinkedWBHOCR.setIgnoreMissingLinkedWorkbooks(this.howc.getIgnoreMissingLinkedWorkbooks());
			currentLinkedWBHOCR.setFileName(entry.getKey());
			currentLinkedWBHOCR.setPassword(linkedWorkbooksPasswords.get(entry.getKey()));
			currentLinkedWBHOCR.setMetaDataFilter(null);
			MSExcelParser currentLinkedWorkbookParser = new MSExcelParser(currentLinkedWBHOCR,null);
			try {
				currentLinkedWorkbookParser.parse(entry.getValue());
			} catch (FormatNotUnderstoodException e) {
				LOG.error(e);
				throw new OfficeWriterException(e.toString());
			}
			this.listOfWorkbooks.add(currentLinkedWorkbookParser.getCurrentWorkbook());
			linkedFormulaEvaluators.put(entry.getKey(),currentLinkedWorkbookParser.getCurrentFormulaEvaluator());
			this.currentWorkbook.linkExternalWorkbook(entry.getKey(),currentLinkedWorkbookParser.getCurrentWorkbook());
		}
					
	} finally {	// close linked workbook inputstreams
		for (InputStream currentIS: linkedWorkbooks.values()) {
			try {
				currentIS.close();
			} catch (IOException e) {
				LOG.error(e);
			}
		}
	}
	LOG.debug("Size of linked formula evaluators map: "+linkedFormulaEvaluators.size());
	currentFormulaEvaluator.setupReferencedWorkbooks(linkedFormulaEvaluators);
	if (this.howc.getSigKey()!=null) { // use temporary files for signatures
		try {
			this.signUtil= new MSExcelOOXMLSignUtil(this.oStream);
		} catch (IOException e) {
			LOG.error("Cannot create sign utilities "+e);
			throw new OfficeWriterException(e.toString());
		}
	}
}
	


/**
* Add a cell to the current Workbook
*
* @param newDAO cell to add. If it is already existing an exception will be thrown. Note that the sheet name is sanitized using  org.apache.poi.ss.util.WorkbookUtil.createSafeSheetName. The Cell address needs to be in A1 format. Either formula or formattedValue must be not null.
*
*/
@Override
public void write(Object newDAO) throws OfficeWriterException {
	if (newDAO!=null) {
		SpreadSheetCellDAO sscd = checkSpreadSheetCellDAO(newDAO);
		String safeSheetName=WorkbookUtil.createSafeSheetName(sscd.getSheetName());
		Sheet currentSheet=this.currentWorkbook.getSheet(safeSheetName);
		if (currentSheet==null) {// create sheet if it does not exist yet
			currentSheet=this.currentWorkbook.createSheet(safeSheetName);
			if (!(safeSheetName.equals(sscd.getSheetName()))) {
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
		if ((currentCell!=null) && (this.hasTemplate==false)) { // cell already exists and no template loaded ? => throw exception
			throw new OfficeWriterException("Invalid cell specification: cell already exists at "+currentCA);
		}
		// create cell if no template is loaded or cell not available in template
		if ((this.hasTemplate==false) || (currentCell==null)) {
			currentCell=currentRow.createCell(currentCA.getColumn());		
		}
		// set the values accordingly
		if (!("".equals(sscd.getFormula()))) { // if formula exists then use formula
	
			currentCell.setCellFormula(sscd.getFormula());
			
		} else {	
		// else use formattedValue
			currentCell.setCellValue(sscd.getFormattedValue());

		}
		// set comment
		if ((sscd.getComment()!=null) && (!("".equals(sscd.getComment())))) {
			/** the following operations are necessary to create comments **/
			/** Define size of the comment window **/
			    ClientAnchor anchor = this.currentWorkbook.getCreationHelper().createClientAnchor();
	    		    anchor.setCol1(currentCell.getColumnIndex());
	    		    anchor.setCol2(currentCell.getColumnIndex()+this.howc.getCommentWidth());
	    		    anchor.setRow1(currentRow.getRowNum());
	    		    anchor.setRow2(currentRow.getRowNum()+this.howc.getCommentHeight());
			/** create comment **/
			    Comment currentComment = mappedDrawings.get(safeSheetName).createCellComment(anchor);
	    		    currentComment.setString(this.currentWorkbook.getCreationHelper().createRichTextString(sscd.getComment()));
	    		    currentComment.setAuthor(this.howc.getCommentAuthor());
			    currentCell.setCellComment(currentComment);
	
		}
	}
}

/***
 * Verifies that an object is a well-formed SpreadSheetCellDAO
 * 
 * 
 * @param newDAO obejct
 * @return SpreadsheetCellDAO if wellformed, otherwise null
 */

public static SpreadSheetCellDAO checkSpreadSheetCellDAO(Object newDAO) throws OfficeWriterException {
	if (!(newDAO instanceof SpreadSheetCellDAO)) {
		throw new OfficeWriterException("Objects which are not of the class SpreadSheetCellDAO are not supported for writing.");
	}
	SpreadSheetCellDAO sscd = (SpreadSheetCellDAO)newDAO;
	// check sheetname (needs to be there)
	if ((sscd.getSheetName()==null) || ("".equals(sscd.getSheetName()))) {
		throw new OfficeWriterException("Invalid cell specification: empy sheet name not allowed.");
	}
	// check celladdress (needs to be there)
	if ((sscd.getAddress()==null) || ("".equals(sscd.getAddress()))) {
		throw new OfficeWriterException("Invalid cell specification: empy cell address not allowed.");
	}
	// check that either formula or formatted value is filled
	if ((sscd.getFormula()==null) && (sscd.getFormattedValue()==null))  {
		throw new OfficeWriterException("Invalid cell specification: either formula or formattedValue needs to be specified for cell.");
	}
	return sscd;
}


/**
* Writes the document in-memory representation to the OutputStream. Afterwards, it closes all related workbooks.
*
* @throws java.io.IOException in case of issues writing 
*
*
*/
@Override
public void close() throws IOException {
	try {
		// prepare metadata
		prepareMetaData();
		
		// write
		if (this.oStream!=null) {
			if (this.howc.getPassword()==null) { // no encryption
				finalizeWriteNotEncrypted();
			} else 	// encryption
				if (this.currentWorkbook instanceof HSSFWorkbook) { // old Excel format
					finalizeWriteEncryptedHSSF();
				} else if (this.currentWorkbook instanceof XSSFWorkbook) {
					finalizeWriteEncryptedXSSF();
					}
				 else {
					LOG.error("Could not write encrypted workbook, because type of workbook is unknown");
				}

			}
		} finally {

			// close filesystems
			if (this.ooxmlDocumentFileSystem!=null)  {
				 ooxmlDocumentFileSystem.close();
			}

		// close main workbook
		if (this.currentWorkbook!=null) {
			this.currentWorkbook.close();
		}
		
		// close linked workbooks
		 	for (Workbook currentWorkbookItem: this.listOfWorkbooks) {
				if (currentWorkbookItem!=null) {
					currentWorkbookItem.close();
				}
			}
		}
	try {
	// do we need to sign => sign
	if (this.signUtil!=null) {
		// sign
		LOG.info("Signing document \""+this.howc.getFileName()+"\"");
		if (this.howc.getSigCertificate()==null) {
			LOG.error("Cannot sign document \""+this.howc.getFileName()+"\". No certificate for key provided");
		} else if (!(this.currentWorkbook instanceof XSSFWorkbook)){
			LOG.warn("Signing of docuemnts in old Excel format not supported for \""+this.howc.getFileName()+"\"");
		}else {
		try {
				ArrayList<X509Certificate> certList = new ArrayList<>();
				certList.add(this.howc.getSigCertificate());
				this.signUtil.sign(this.howc.getSigKey(), certList, this.howc.getPassword(), MSExcelWriter.getHashAlgorithm(this.howc.getSigHash()));
		} catch (XMLSignatureException|MarshalException|IOException|FormatNotUnderstoodException e) {
			LOG.error("Cannot sign document \""+this.howc.getFileName()+"\" "+e);
		}
			
		}
		
	}
	} finally {
		if (this.signUtil!=null) {
			this.signUtil.close();
		}
	}
}



private void finalizeWriteNotEncrypted() throws IOException {
	try {
		if ((this.signUtil!=null) && (this.currentWorkbook instanceof XSSFWorkbook)) {
			this.currentWorkbook.write(this.signUtil.getTempOutputStream()); // write to temporary file to sign it afterwards
		} else {
			this.currentWorkbook.write(this.oStream);
		}
	} finally {
		if ((this.oStream!=null) && (this.signUtil==null)) {
			this.oStream.close();
		}

	}
	
	
}

private void finalizeWriteEncryptedHSSF() throws IOException {
	LOG.debug("encrypting HSSFWorkbook");
	Biff8EncryptionKey.setCurrentUserPassword(this.howc.getPassword());
	try {
		this.currentWorkbook.write(this.oStream);
	} finally {
		Biff8EncryptionKey.setCurrentUserPassword(null);
		if (this.oStream!=null) {
			this.oStream.close();
		}
	}
}

private void finalizeWriteEncryptedXSSF() throws IOException{
	if (this.encryptAlgorithmCipher==null) {
		LOG.error("No encryption algorithm specified");
		return;
	} else
	if (this.hashAlgorithmCipher==null) {
		LOG.error("No hash algorithm specified");
		return;
	} else
	if (this.encryptionModeCipher==null) {
		LOG.error("No encryption mode specified");
		return;
	} else
	if (this.chainModeCipher==null) {
		LOG.error("No chain mode specified");
		return;
	} 
		OutputStream os = null;
		try {
			EncryptionInfo info = new EncryptionInfo(this.encryptionModeCipher, this.encryptAlgorithmCipher, this.hashAlgorithmCipher, -1, -1, this.chainModeCipher);
			Encryptor enc = info.getEncryptor();
			enc.confirmPassword(this.howc.getPassword());
			
			try {
				os = enc.getDataStream(ooxmlDocumentFileSystem);
				if (os!=null) {
					this.currentWorkbook.write(os);
				}
				if (os!=null) {
					os.close();
				}
			} catch (GeneralSecurityException e) {
				LOG.error(e);
			} 

			OutputStream theOS=this.oStream;
			if (this.signUtil!=null) {
				theOS= this.signUtil.getTempOutputStream();
				
			} 
			ooxmlDocumentFileSystem.writeFilesystem(theOS);
			
		} finally {
			
		 if ((this.oStream!=null) && (this.signUtil==null)) { // if we need to sign it we close it later after signing
			 this.oStream.close();
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
		if (VALID_FORMAT[i].equals(format)) {
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

public static CipherAlgorithm getAlgorithmCipher(String encryptAlgorithm) {
	if (encryptAlgorithm==null) { 
		return null;
	}
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
		default:
			LOG.error("Uknown encryption algorithm: \""+encryptAlgorithm+"\"");
			break;
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
public static HashAlgorithm getHashAlgorithm(String hashAlgorithm) {
	if (hashAlgorithm==null) {
		return null;
	}
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
		default:
			LOG.error("Uknown hash algorithm: \""+hashAlgorithm+"\"");
			break;
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


public static EncryptionMode getEncryptionModeCipher(String encryptionMode) {
	if (encryptionMode==null) {
		return null;
	}
	switch (encryptionMode) {
		case "agile": return EncryptionMode.agile;
		case "binaryRC4": return EncryptionMode.binaryRC4;
		case "cryptoAPI": return EncryptionMode.cryptoAPI;
		case "standard": return EncryptionMode.standard;
		default:
			LOG.error("Uknown enncryption mode \""+encryptionMode+"\"");
			break;
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


public static ChainingMode getChainMode(String chainMode) {
	if (chainMode==null) {
		return null;
	}
	switch (chainMode) {
		case "cbc": return ChainingMode.cbc;
		case "cfb": return ChainingMode.cfb;
		case "ecb": return ChainingMode.ecb;
		default:
			LOG.error("Uknown chainmode: \""+chainMode+"\"");
			break;

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
	} else if (this.currentWorkbook instanceof XSSFWorkbook) {
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
	if (summaryInfo==null) {
		currentHSSFWorkbook.createInformationProperties();
		 summaryInfo = currentHSSFWorkbook.getSummaryInformation(); 
	}
	SimpleDateFormat formatSDF = new SimpleDateFormat(MSExcelParser.DATE_FORMAT); 
	for (Map.Entry<String,String> entry: this.howc.getMetadata().entrySet()) {
		// process general properties
		try {
		switch(entry.getKey()) {
			case "applicationname": 
				summaryInfo.setApplicationName(entry.getValue()); 
				break;
			case "author": 
				summaryInfo.setAuthor(entry.getValue()); 
				break;
			case "charcount": 
				summaryInfo.setCharCount(Integer.parseInt(entry.getValue())); 
				break;
			case "comments": 
				summaryInfo.setComments(entry.getValue()); 
				break;
			case "createdatetime": 
				summaryInfo.setCreateDateTime(formatSDF.parse(entry.getValue())); 
				break;
			case "edittime": 
				summaryInfo.setEditTime(Long.parseLong(entry.getValue())); 
				break;
			case "keywords": 
				summaryInfo.setKeywords(entry.getValue()); 
				break;
			case "lastauthor": 
				summaryInfo.setLastAuthor(entry.getValue()); 
				break;
			case "lastprinted": 
				summaryInfo.setLastPrinted(formatSDF.parse(entry.getValue())); 
				break;	
			case "lastsavedatetime": 
				summaryInfo.setLastSaveDateTime(formatSDF.parse(entry.getValue())); 
				break;
			case "pagecount": 
				summaryInfo.setPageCount(Integer.parseInt(entry.getValue())); 
				break;
			case "revnumber": 
				summaryInfo.setRevNumber(entry.getValue()); 
				break;
			case "security": 
				summaryInfo.setSecurity(Integer.parseInt(entry.getValue())); 
				break;
			case "subject": 
				summaryInfo.setSubject(entry.getValue()); 
				break;
			case "template": 
				summaryInfo.setTemplate(entry.getValue()); 
				break;	
			case "title": 
				summaryInfo.setTitle(entry.getValue()); 
				break;
			case "wordcount": 
				summaryInfo.setWordCount(Integer.parseInt(entry.getValue())); 
				break;
			default: 
				LOG.warn("Unknown metadata key: "+entry.getKey()); 
				break;	
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
	SimpleDateFormat formatSDF = new SimpleDateFormat(MSExcelParser.DATE_FORMAT); 
	for (Map.Entry<String,String> entry: this.howc.getMetadata().entrySet()) {
		// process general properties
		boolean attribMatch=false;
		try {
		switch(entry.getKey()) {
			case "category": 
				coreProp.setCategory(entry.getValue()); 
				attribMatch=true; 
				break;
			case "contentstatus": 
				coreProp.setContentStatus(entry.getValue()); 
				attribMatch=true; 
				break;
			case "contenttype": 
				coreProp.setContentType(entry.getValue());
				attribMatch=true; 
				break;
			case "created": 
				coreProp.setCreated(Optional.of(formatSDF.parse(entry.getValue()))); 
				attribMatch=true; 
				break;
			case "creator": 
				coreProp.setCreator(entry.getValue()); 
				attribMatch=true; 
				break;
			case "description": 
				coreProp.setDescription(entry.getValue()); 
				attribMatch=true; 
				break;
			case "identifier": 
				coreProp.setIdentifier(entry.getValue()); 
				attribMatch=true; 
				break;
			case "keywords": 
				coreProp.setKeywords(entry.getValue());
				attribMatch=true; 
				break;
			case "lastmodifiedbyuser": 
				coreProp.setLastModifiedByUser(entry.getValue()); 
				attribMatch=true; 
				break;
			case "lastprinted": 
				coreProp.setLastPrinted(Optional.of(formatSDF.parse(entry.getValue())));
				attribMatch=true; 
				break;
			case "modified": 
				coreProp.setModified(Optional.of(formatSDF.parse(entry.getValue()))); 
				attribMatch=true; 
				break;
			case "revision":
				coreProp.setRevision(entry.getValue()); 
				attribMatch=true; 
				break;
			case "subject": 
				coreProp.setSubjectProperty(entry.getValue()); 
				attribMatch=true; 
				break;
			case "title": 
				coreProp.setTitle(entry.getValue());
				attribMatch=true; 
				break;
			default: // later we check if custom properties need to be added 
				break;
		} 
		if (!(attribMatch)) {
		// process custom properties
		processCustomProperties(entry.getKey(), entry.getValue(), custProp);
		}
		} catch (ParseException pe) {
			LOG.error(pe);
		}
}
}

private void processCustomProperties(String key, String value, POIXMLProperties.CustomProperties custProp) {
	if (key.startsWith("custom.")) {
		String strippedKey=key.substring("custom.".length());
		if (strippedKey.length()>0) {
			custProp.addProperty(strippedKey,value);
		}
	} else {
		LOG.warn("Unknown metadata key: "+key);
	}
}
	

}
