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
package org.zuinnote.hadoop.office.format.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;

import javax.xml.crypto.MarshalException;
import javax.xml.crypto.dsig.XMLSignatureException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.crypt.dsig.SignatureConfig;
import org.apache.poi.poifs.crypt.dsig.SignatureInfo;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.util.TempFile;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;

/**
 * This class stores the OOXML Excel file to a temporary output folder and the rereads it to sign it and store it in the final output stream (usually HDFS or any other compatible filesystem, such as S3)
 *
 */
public class MSExcelOOXMLSignUtil {
	private static final Log LOG = LogFactory.getLog(MSExcelOOXMLSignUtil.class.getName());
	private OutputStream finalOutputStream;
	private File tempSignFile;
	private FileOutputStream tempSignFileOS;
	
	
	public MSExcelOOXMLSignUtil(OutputStream finalOutputStream) throws IOException {
		this.finalOutputStream=finalOutputStream;
		LOG.info("Creating tempfile for signing");
		// create temporary outoputfile
		this.tempSignFile=TempFile.createTempFile("hadooffice-poi-temp-data-sign",".tmp");
		this.tempSignFileOS=new FileOutputStream(this.tempSignFile);
	}
	
	/***
	 * Temporary outputstream to be used to write content
	 * 
	 * @return temporary output stream
	 */
	public OutputStream getTempOutputStream() {
		return this.tempSignFileOS;
	}
	
	/** 
	 * Signs the Excel OOXML file and writes it to the final outputstream
	 * 
	 * @param privateKey private Key for signing
	 * @param x509 Certificate for private Key for signing
	 * @param password optional password for encryption, if used
	 * 
	 * @throws MarshalException 
	 * @throws XMLSignatureException 
	 * @throws IOException 
	 * @throws FormatNotUnderstoodException 
	 */
	public void sign(Key privateKey, X509Certificate x509, String password) throws XMLSignatureException, MarshalException, IOException, FormatNotUnderstoodException {
		if (this.tempSignFileOS!=null) { // close it we sign only a closed temporary file
			this.tempSignFileOS.close();
		}
		SignatureConfig sc = new SignatureConfig();
		sc.setKey((PrivateKey)privateKey);
		sc.setSigningCertificateChain(Collections.singletonList(x509));
		FileInputStream tempSignFileIS = null;
		try {
			
			InputStream tmpFileInputStream = new FileInputStream(this.tempSignFile);
			if (password==null) {
				this.signUnencryptedOpcPackage(tmpFileInputStream, sc);
			} else {
				this.signEncryptedPackage(tmpFileInputStream, sc, password);
			}
			
		} catch (InvalidFormatException | IOException e) {
			LOG.error(e);
		} finally {
			if (this.finalOutputStream!=null) {
				this.finalOutputStream.close();
			}
			if (tempSignFileIS!=null) {
				tempSignFileIS.close();
			}
		}
	}
	
	private void signUnencryptedOpcPackage(InputStream tmpFileInputStream, SignatureConfig sc) throws InvalidFormatException, IOException, XMLSignatureException, MarshalException {
		OPCPackage	pkg = OPCPackage.open(tmpFileInputStream);
		sc.setOpcPackage(pkg);
		SignatureInfo si = new SignatureInfo();
		si.setSignatureConfig(sc);
		si.confirmSignature();
		pkg.save(this.finalOutputStream);
		pkg.close();
	}
	
	private void signEncryptedPackage(InputStream tmpFileInputStream, SignatureConfig sc, String password) throws IOException, InvalidFormatException, FormatNotUnderstoodException, XMLSignatureException, MarshalException {
	
		NPOIFSFileSystem poifsTemp = new NPOIFSFileSystem(tmpFileInputStream);
		EncryptionInfo info = new EncryptionInfo(poifsTemp);
		Decryptor d = Decryptor.getInstance(info);
		try {
			if (!d.verifyPassword(password)) {
				throw new FormatNotUnderstoodException("Error: Cannot decrypt new Excel file (.xlsx) for signing. Invalid password");
			}
			// signing
			OPCPackage pkg = OPCPackage.open(d.getDataStream(poifsTemp));
			sc.setOpcPackage(pkg);
			SignatureInfo si = new SignatureInfo();
			si.setSignatureConfig(sc);
			si.confirmSignature();
			// encrypt again
			Encryptor enc = info.getEncryptor();
			enc.confirmPassword(password);
			NPOIFSFileSystem poifs = new NPOIFSFileSystem();
			OutputStream os = enc.getDataStream(poifs);
			pkg.save(os);
			pkg.close();
			if (os!=null) {
				os.close();
			}
			poifs.writeFilesystem(this.finalOutputStream);
			if (poifs!=null) {
				poifs.close();
			}
			if (poifsTemp!=null) {
				poifsTemp.close();
			}
		} catch (GeneralSecurityException e) {
			
			LOG.error(e);
			throw new FormatNotUnderstoodException("Error: Cannot decrypt new Excel file (.xlsx)  for signing.");
		}
	}

	/**
	 *   Cleans up tempfolder
	 * @throws IOException 
	 */
	public void close() throws IOException {
		try {
			if (this.tempSignFileOS!=null) {
				this.tempSignFileOS.close();
			}
		} finally {
			if (!this.tempSignFile.delete()) {
				LOG.warn("Could not delete temporary files used for signing");
			}
		}
	}
	

}
