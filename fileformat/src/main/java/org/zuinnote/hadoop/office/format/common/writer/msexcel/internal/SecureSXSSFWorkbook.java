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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.poi.openxml4j.util.ZipEntrySource;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.zuinnote.hadoop.office.format.common.writer.msexcel.internal.EncryptedZipEntrySource;

/**
 * 
 * This class is inspired by https://bz.apache.org/bugzilla/show_bug.cgi?id=60321 to create - in case of encrypted excel - also use encrypted and compressed temporary files
 *
 * The underlying ideas of the examples have been enhanced, e.g. temporary data is encrypted with the same algorithm as the output data. The examples uses only AES128, which may not be sufficient for all cases of confidential data
 * 
 */
public class SecureSXSSFWorkbook extends SXSSFWorkbook {
	private CipherAlgorithm ca;
	private ChainingMode cm;
	
	public SecureSXSSFWorkbook(int cacherows, CipherAlgorithm ca, ChainingMode cm) {
		super(cacherows);
		setCompressTempFiles(true);
		this.ca=ca;
		this.cm=cm;
	}
	
	@Override
	public void write(OutputStream stream) throws IOException {
		
		this.flushSheets();
		EncryptedTempData tempData = new EncryptedTempData(this.ca,this.cm);
		ZipEntrySource source = null;
		
			OutputStream os = tempData.getOutputStream();
			try {
				getXSSFWorkbook().write(os);
			} finally {
				IOUtils.closeQuietly(os);
			}
			// provide ZipEntrySoruce to poi to decrypt on the fly (if necessary)
			source = new EncryptedZipEntrySource(this.ca,this.cm);
			((EncryptedZipEntrySource)source).setInputStream(tempData.getInputStream());
			injectData(source,stream);
	
			tempData.dispose();
	
		
	}
}