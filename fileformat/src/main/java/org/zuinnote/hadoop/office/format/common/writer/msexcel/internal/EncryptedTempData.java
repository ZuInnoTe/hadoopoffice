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


package org.zuinnote.hadoop.office.format.common.writer.msexcel.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.poifs.crypt.CryptoFunctions;
import org.apache.poi.util.TempFile;


/**
 * @author jornfranke
 *
 */
public class EncryptedTempData {
	private static final Log LOG = LogFactory.getLog(EncryptedTempData.class.getName());
	private CipherAlgorithm ca;
	private ChainingMode cm;
	private Cipher ciEncrypt;
	private Cipher ciDecrypt;
	private File tempFile;
	
	public EncryptedTempData(CipherAlgorithm ca, ChainingMode cm) throws IOException {
		// generate random key for temnporary files
		if (ca!=null) {
					SecureRandom sr = new SecureRandom();
					byte[] iv = new byte[ca.blockSize];
					byte[] key = new byte[ca.defaultKeySize/8];
					sr.nextBytes(iv);
					sr.nextBytes(key);
					SecretKeySpec skeySpec = new SecretKeySpec(key,ca.jceId);
					this.ca = ca;
					this.cm = cm;
					if (this.cm.jceId.equals(ChainingMode.ecb.jceId)) { // does not work with Crpyto Functions since it does not require IV
						this.cm=ChainingMode.cbc;
					}
					this.ciEncrypt=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.ENCRYPT_MODE,"PKCS5Padding");
					this.ciDecrypt=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.DECRYPT_MODE,"PKCS5Padding");
		}
				this.tempFile=TempFile.createTempFile("hadooffice-poi-temp-data",".tmp");
	}
	
	public OutputStream getOutputStream() throws FileNotFoundException {
		if (this.ciEncrypt!=null) { // encrypted tempdata
			LOG.debug("Returning encrypted OutputStream for "+this.tempFile.getAbsolutePath());
			
			return new CipherOutputStream(new FileOutputStream(this.tempFile),this.ciEncrypt);
		}
		return new FileOutputStream(this.tempFile);
	}
	
	public InputStream getInputStream() throws FileNotFoundException {
		if (this.ciDecrypt!=null) { // decrypt tempdata
			LOG.debug("Returning decrypted InputStream for "+this.tempFile.getAbsolutePath());
			LOG.debug("Size of temp file: "+this.tempFile.length());
			return new CipherInputStream(new FileInputStream(this.tempFile),this.ciDecrypt);
		}
		return new FileInputStream(this.tempFile);
	}
	
	public void dispose() {
		if (!this.tempFile.delete()) {
			LOG.warn("Could not delete temporary files");
		}
	}
}