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
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.Enumeration;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.openxml4j.util.ZipEntrySource;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.poifs.crypt.CryptoFunctions;
import org.apache.poi.util.TempFile;

/**
 * @author jornfranke
 *
 */

public class EncryptedZipEntrySource implements ZipEntrySource {
	private static final Log LOG = LogFactory.getLog(EncryptedZipEntrySource.class.getName());
	private ZipFile zipFile;
	private CipherAlgorithm ca;
	private ChainingMode cm;
	private Cipher ciEncoder;
	private Cipher ciDecoder;
	private File tmpFile;
	private boolean closed;

	public EncryptedZipEntrySource( CipherAlgorithm ca, ChainingMode cm) throws  IOException {
		// generate random key for temporary files
		if (ca!=null) { // encrypted files
			SecureRandom sr = new SecureRandom();
			byte[] iv = new byte[ca.blockSize];
			byte[] key = new byte[ca.defaultKeySize/8];
			sr.nextBytes(iv);
			sr.nextBytes(key);
			SecretKeySpec skeySpec = new SecretKeySpec(key,ca.jceId);
			this.ca = ca;
			this.cm = cm;
			this.ciEncoder=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.ENCRYPT_MODE,"PKCS5Padding");
			this.ciDecoder=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.DECRYPT_MODE,"PKCS5Padding");
		}
		
		
		this.closed=false;
	
	}
	

	public void setInputStream(InputStream is) throws IOException {
		this.tmpFile = TempFile.createTempFile("hadoopoffice-protected", ".zip");
		
		ZipArchiveInputStream zis = new ZipArchiveInputStream(is);
		FileOutputStream fos = new FileOutputStream(tmpFile);
		ZipArchiveOutputStream zos = new ZipArchiveOutputStream(fos);
		ZipArchiveEntry ze;
		while ((ze = (ZipArchiveEntry) zis.getNextEntry()) !=null) {
			// rewrite zip entries to match the size of the encrypted data (with padding)
			ZipArchiveEntry zeNew = new ZipArchiveEntry(ze.getName());
			zeNew.setComment(ze.getComment());
			zeNew.setExtra(ze.getExtra());
			zeNew.setTime(ze.getTime());
			zos.putArchiveEntry(zeNew);
			FilterOutputStream fos2 = new FilterOutputStream(zos) {
				// do not close underlyzing ZipOutputStream
				@Override
				public void close() {}
			};
			OutputStream nos;
			if (this.ciEncoder!=null) { // encrypt if needed
				nos= new CipherOutputStream(fos2,this.ciEncoder);
			} else { // do not encrypt
				nos = fos2;
			}
			IOUtils.copy(zis, nos);
			nos.close();
			if (fos2!=null) {
				fos2.close();
			}
			zos.closeArchiveEntry();
		
		}
		zos.close();
		fos.close();
		zis.close();
		IOUtils.closeQuietly(is);
		this.zipFile=new ZipFile(this.tmpFile);
		
	}


	@Override
	public void close() throws IOException {
		if (!this.closed) {
			this.zipFile.close();
			if (this.tmpFile!=null) {
				
				if (!this.tmpFile.delete()) {
					LOG.warn("Could not delete temporary files");
				}
			}
		}
		this.closed=true;
	}

	@Override
	public boolean isClosed() {
		
		return this.closed;
	}

	@Override
	public ZipArchiveEntry getEntry(String path) {
		return this.zipFile.getEntry(path);
	}

	
	@Override
	public Enumeration<? extends ZipArchiveEntry> getEntries() {
	
		return this.zipFile.getEntries();
	}
	
	@Override
	public InputStream getInputStream(ZipArchiveEntry entry) throws IOException {
		InputStream is = this.zipFile.getInputStream(entry);
		if (this.ciDecoder!=null) {
			return new CipherInputStream(is,this.ciDecoder);
		}
		return is;
	}
	
}