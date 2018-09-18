/**
* Copyright 2018 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.poifs.crypt.CryptoFunctions;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.util.StaxHelper;
import org.apache.poi.util.TempFile;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.internal.cache.LRUCache;

/**
 * This class implements on top of Apache POI SharedStrings table a disk-based
 * gzip-compressed cached encrypted SharedStringsTable that does not require to
 * keep the full SharedStringsTable in-memory (in case of large ones). It is
 * only for the new XML-based Excel format The StaX (instead of the SAX) parser
 * is used for efficient low memory/low CPU parsing of string tables. This class
 * is based on the source code of the Apache POI library, ie we inspired from
 * SharedStringsTable and added functionality for encryption, compression and
 * off-memory storage. Note: The memory foot print is not only related to cache
 * size, but we need to store also the position of each string in the file. If
 * one has a lot of entries, this could means one million or more position in
 * memory. E.g. one position has a size of 8 byte in memory. One million entries
 * have 8 Million bytes in memory implying 8 Megabyte of additional memory is
 * needed.
 * 
 * The implementation assumes that the SST will be accessed sequentially in most
 * cases (due to the large number of entries)
 * 
 * Note that encryption and compression significantly reduce performance due to
 * the lack of random access in those files. You will need to define a suitable
 * cache size for those
 * 
 */
public class EncryptedCachedDiskStringsTable extends SharedStringsTable implements AutoCloseable {
	private static final Log LOG = LogFactory.getLog(EncryptedCachedDiskStringsTable.class.getName());
	public static final int compressBufferSize = 1 * 1024 * 1024;
	public static final String encoding = "UTF-8";
	private List<Long> stringPositionInFileList;
	private Map<Integer, String> cache;
	private int cacheSize;
	private File tempFile;
	private long tempFileSize;
	private InputStream in;
	private InputStream originalIS;
	private CipherAlgorithm ca;
	private ChainingMode cm;
	private Cipher ciEncrypt;
	private Cipher ciDecrypt;
	private boolean compressTempFile;
	private long currentPos;
	private int currentItem;
	private int count;
	private RandomAccessFile tempRAF = null;

	/***
	 * Create a new encrypted cached string table
	 * 
	 * @param part             package part with Shared String Table
	 * @param cacheSize        cache = -1 means all is in memory, cache = 0 means
	 *                         nothing is in memory, positive means only that
	 *                         fractions is kept in-memory
	 * @param compressTempFile true, if temporary file storage for shared string
	 *                         table should be gzip compressed, false if not
	 * @param                  ca, cipher algorithm leave it null for disabling
	 *                         encryption (not recommended if source document is
	 *                         encrypted)
	 * @param                  cm, chaining mode, only need to be specified if
	 *                         cipher algorithm is specified
	 * @throws IOException
	 */

	public EncryptedCachedDiskStringsTable(PackagePart part, int cacheSize, boolean compressTempFile,
			CipherAlgorithm ca, ChainingMode cm) throws IOException {
		this.cacheSize = cacheSize;
		this.count=0;
		if (this.cacheSize > 0) {

			this.cache = new LRUCache<>(((int) Math.ceil(this.cacheSize / 0.75)) + 1); // based on recommendations of
																						// the Javadoc of HashMap
			this.stringPositionInFileList = new ArrayList<>(this.cacheSize);
		} else {
			this.cache = new LRUCache<>();
			this.stringPositionInFileList = new ArrayList<>();
		}
		this.stringPositionInFileList = new ArrayList<>();
		this.compressTempFile = compressTempFile;
		this.tempFile = TempFile.createTempFile("hadooffice-poi-temp-sst", ".tmp");
		this.tempFileSize = 0L;
		// generate random key for temnporary files
		if (ca != null) {
			SecureRandom sr = new SecureRandom();
			byte[] iv = new byte[ca.blockSize];
			byte[] key = new byte[ca.defaultKeySize / 8];
			sr.nextBytes(iv);
			sr.nextBytes(key);
			SecretKeySpec skeySpec = new SecretKeySpec(key, ca.jceId);
			this.ca = ca;
			this.cm = cm;
			if (this.cm.jceId.equals(ChainingMode.ecb.jceId)) { // does not work with Crpyto Functions since it does not require IV
				this.cm=ChainingMode.cbc;
			}
			this.ciEncrypt = CryptoFunctions.getCipher(skeySpec, this.ca, this.cm, iv, Cipher.ENCRYPT_MODE, "PKCS5Padding");
			this.ciDecrypt = CryptoFunctions.getCipher(skeySpec, this.ca, this.cm, iv, Cipher.DECRYPT_MODE, "PKCS5Padding");
		}
		this.originalIS = part.getInputStream();
		this.readFrom(this.originalIS);
	}

	/**
	 * Reads from the original Excel document the string table and puts it into a
	 * compressed and encrypted file.
	 * 
	 * @param is
	 * @throws java.io.IOException
	 */

	@Override
	public void readFrom(InputStream is) throws IOException {
		this.currentItem = 0;
		// read from source and write into tempfile
		// open temp file depending on options compressed/encrypted
		OutputStream tempOS = null;
		if ((this.ca != null) || (this.compressTempFile)) {
			tempOS = new FileOutputStream(this.tempFile);
			if (this.ca != null) { // encrypt file if configured
				tempOS = new CipherOutputStream(tempOS, this.ciEncrypt);
			}
			if (this.compressTempFile) { // compress file if configured
				tempOS = new GZIPOutputStream(tempOS, EncryptedCachedDiskStringsTable.compressBufferSize);
			}
		} else { // not encrypted and not compressed: configure a random access file for
					// writing/reading = highest performance
			this.tempRAF = new RandomAccessFile(this.tempFile, "rw");
		}
		// read from source
		// use Stax event reader
		XMLEventReader xer = null;
		try {
			xer = StaxHelper.newXMLInputFactory().createXMLEventReader(this.originalIS);
			while (xer.hasNext()) {
				XMLEvent xe = xer.nextEvent();
				// check if it is a string item (entry in string table)
				if (xe.isStartElement() && xe.asStartElement().getName().getLocalPart().equalsIgnoreCase("si")) {
					String siText = this.parseSIText(xer);
					// add string to temp file
					this.addString(siText, tempOS);
					this.count++;
				}
			}
			// close tempfile
			// make tempFile available as a reader
		} catch (XMLStreamException e) {
			LOG.error("Cannot read original SharedStringTable from document. Exception " + e);
			throw new IOException(e);
		} catch (FormatNotUnderstoodException e) {
			LOG.error("Cannot read properly SharedStringTable from document. Exception " + e);
			throw new IOException(e);
		} finally {
			// close temporary Stream (tempfile should be deleted using the close method of
			// this class and not here)
			if (tempOS != null) {
				tempOS.close();
			}
		}
		// open the input stream towards the temp file
		this.accessTempFile(0L);

	}

	/**
	 * Get an item from the shared string table
	 * 
	 * @param idx index of the entry
	 * @return entry
	 * 
	 */
	@Override
	public RichTextString getItemAt(int idx) {
		try {
			return new XSSFRichTextString(this.getString(idx));
		} catch (IOException e) {
			LOG.error("Cannot read from temporary shared String table. Exception: " + e);
		}
		return new XSSFRichTextString("");
	}

	/**
	 * Closes the temporary inputstream and deletes the temp file
	 * 
	 * @throws IOException
	 * 
	 */
	@Override
	public void close() throws IOException {
		if (this.in != null) {
			this.in.close();
		}
		if (!this.tempFile.delete()) {
			LOG.warn("Cannot delete tempFile: " + this.tempFile.getAbsolutePath());
			throw new IOException("Cannot delete tempFile: " + this.tempFile.getAbsolutePath());
		}
		;
	}

	/**
	 * Adds a string to the table on disk
	 * 
	 * @param str string to store
	 * @param os  OutputStream to use to write it
	 * @throws IOException
	 */
	private void addString(String str, OutputStream os) throws IOException {
		if (this.cacheSize >= 0) { // add to disk
			byte[] strbytes = str.getBytes(EncryptedCachedDiskStringsTable.encoding);
			byte[] sizeOfStr = ByteBuffer.allocate(4).putInt(strbytes.length).array();
			this.stringPositionInFileList.add(this.tempFileSize);
			if (os != null) {
				os.write(sizeOfStr);
				os.write(strbytes);
			} else { // we can write to the random access file
				FileChannel fc = this.tempRAF.getChannel().position(this.tempFileSize);
				fc.write(ByteBuffer.wrap(sizeOfStr));
				fc.write(ByteBuffer.wrap(strbytes));
			}
			this.tempFileSize += sizeOfStr.length + strbytes.length;
		}
		if (this.cacheSize < 0) { // put it into cache
			this.cache.put(this.currentItem, str);
			this.currentItem++;
		} else if ((this.cacheSize > 0) && (this.currentItem < this.cacheSize)) { // put the first items already into
																					// cache
			this.cache.put(this.currentItem, str);
			this.currentItem++;
		}
	}

	/**
	 * Parses a string item from a SST in XML format
	 * 
	 * @param xer XMLEventReader from which to read the next string item
	 * @throws XMLStreamException           in case the string item cannot be
	 *                                      correctly read from the XML file
	 * @throws FormatNotUnderstoodException in case a string item cannot be
	 *                                      identified in the shared string table
	 *                                      (e.g. unknown type)
	 */
	private String parseSIText(XMLEventReader xer) throws XMLStreamException, FormatNotUnderstoodException {
		String result = "";
		XMLEvent xe;
		while ((xe = xer.nextTag()).isStartElement()) {
			String elementName = xe.asStartElement().getName().getLocalPart().toUpperCase();
			switch (elementName) {
			case "T": // normal text
				result = xer.getElementText();
				break;
			case "R": // rich text (returned as normal text)
				result = this.parseSIRichText(xer);
				break;
			case "RPH": // phonetic (ignored)
			case "PHONETICPR": // phonetic properties (ignored)
				this.skipXMLElementHierarchy(xer);
				break;
			default:
				LOG.error("Unknown string item in shared string table: " + elementName);
				throw new FormatNotUnderstoodException("Unknown string item in shared string table: " + elementName);

			}
		}
		return result;
	}

	/**
	 * Parses a rich text item of a shared string table and returns the unformatted
	 * text
	 * 
	 * @param xer
	 * @return unformatted text of rich text item
	 * @throws FormatNotUnderstoodException
	 * @throws XMLStreamException
	 */
	private String parseSIRichText(XMLEventReader xer) throws XMLStreamException, FormatNotUnderstoodException {
		String result = "";
		XMLEvent xe;
		while ((xe = xer.nextTag()).isStartElement()) {
			String elementName = xe.asStartElement().getName().getLocalPart().toUpperCase();
			switch (elementName) {
			case "T": // normal text
				result = xer.getElementText();
				break;
			case "RPR": // run properties (ignored)
			default:
				LOG.error("Unknown rich text string item in shared string table: " + elementName);
				throw new FormatNotUnderstoodException(
						"Unknown rich text string item in shared string table: " + elementName);

			}
		}

		return result;
	}

	/**
	 * 
	 * Gets a String from cache or underlying encrypted/compressed file
	 * 
	 * @param index
	 * @return
	 * @throws IOException
	 */

	private String getString(int index) throws IOException {
		// check if it is in cache?
		if (this.cache.containsKey(index)) {
			return this.cache.get(index);
		}
		// if not we have to read it from the file
		long itemPosition = this.stringPositionInFileList.get(index);
		String result = null;
		if (this.tempRAF == null) {
			this.accessTempFile(itemPosition);
			byte[] readSize = new byte[4];
			this.in.read(readSize);
			int sizeOfString = ByteBuffer.wrap(readSize).getInt();
			byte[] strbytes = new byte[sizeOfString];
			this.in.read(strbytes);
			this.currentPos += readSize.length + strbytes.length;
			result = new String(strbytes, EncryptedCachedDiskStringsTable.encoding);
		} else {
			FileChannel fc = this.tempRAF.getChannel().position(itemPosition);
			ByteBuffer bb = ByteBuffer.allocate(4);
			// read size of String
			fc.read(bb);
			bb.flip();
			int sizeOfStr = bb.getInt();
			// read string
			bb = ByteBuffer.allocate(sizeOfStr);
			fc.read(bb);
			bb.flip();
			result = new String(bb.array(), EncryptedCachedDiskStringsTable.encoding);
		}
		if (this.cacheSize != 0) {
			this.cache.put(index, result);
		}
		return result;
	}

	/**
	 * 
	 * Skips over an arbitrary deep hierarchy of XML tags
	 * 
	 * @param xer XMLEventReader from which the tags should be skipped
	 * @throws XMLStreamException
	 */
	private void skipXMLElementHierarchy(XMLEventReader xer) throws XMLStreamException {
		while (xer.nextTag().isStartElement()) {
			skipXMLElementHierarchy(xer);
		}
	}

	/***
	 * Simulates random access to the tempfile even if not supported due to
	 * encryption and/or compression
	 * 
	 * @param position
	 * @throws IOException
	 */
	private void accessTempFile(long position) throws IOException {
		if ((position == 0L) || (position < this.currentPos)) { // in those cases we have to read from scratch
			this.in = new FileInputStream(this.tempFile);
			if (this.ca != null) {
				// decrypt
				this.in = new CipherInputStream(this.in, this.ciDecrypt);
			}
			if (this.compressTempFile) { // decompress it
				this.in = new GZIPInputStream(this.in, EncryptedCachedDiskStringsTable.compressBufferSize);
			} else { // configure a buffer for reading it
				this.in = new BufferedInputStream(this.in, EncryptedCachedDiskStringsTable.compressBufferSize);
			}
			this.currentPos = 0L;
		} else if (position > this.currentPos) {
			this.in.skip(position - this.currentPos);
			this.currentPos = position; // attention needs to be updated after read!
		}

	}

	/**
	 * @return the count
	 */
	@Override
	public int getCount() {
		return count;
	}


}
