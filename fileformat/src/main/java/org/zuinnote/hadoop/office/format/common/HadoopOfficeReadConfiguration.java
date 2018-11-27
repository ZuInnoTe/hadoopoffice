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
package org.zuinnote.hadoop.office.format.common;

import java.io.Serializable;
import java.security.cert.PKIXParameters;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

/**
 * read the configuration for reading office files from a Hadoop configuration
 * 
 * @author Jörn Franke (zuinnote@gmail.com)
 *
 */
public class HadoopOfficeReadConfiguration implements Serializable {
	/**
		 * 
		 */
	private static final long serialVersionUID = 8028549445862365699L;
	public static final String PREFIX_UNKNOWN_COL = "Col"; // only for reading headers, if a column does not have a name
	public static final String CONF_MIMETYPE = "hadoopoffice.read.mimeType";
	public static final String CONF_SHEETS = "hadoopoffice.read.sheets";
	public static final String CONF_LOCALE = "hadoopoffice.read.locale.bcp47";
	public static final String CONF_LINKEDWB = "hadoopoffice.read.linkedworkbooks";
	public static final String CONF_LINKEDWB_LOCATION = "hadoopoffice.read.linkedworkbooks.location";
	public static final String CONF_IGNOREMISSINGWB = "hadoopoffice.read.ignoremissinglinkedworkbooks";
	public static final String CONF_DECRYPT = "hadoopoffice.read.security.crypt.password";
	public static final String CONF_DECRYPTLINKEDWBBASE = "hadoopoffice.read.security.crypt.linkedworkbooks.";
	public static final String CONF_FILTERMETADATA = "hadoopoffice.read.filter.metadata."; // base: all these properties
																							// (e.g.
																							// hadoopoffice.read.filter.metadata.author)
																							// will be handed over to
																							// the corresponding reader
																							// which does the filtering!
	public static final String CONF_LOWFOOTPRINT = "hadoopoffice.read.lowFootprint";
	public static final String CONF_LOWFOOTPRINT_PARSER = "hadoopoffice.read.lowFootprint.parser";
	public static final String CONF_LOWFOOTPRINT_STAX_CACHE = "hadoopoffice.read.lowFootprint.stax.sst.cache";
	public static final String CONF_LOWFOOTPRINT_STAX_COMPRESS = "hadoopoffice.read.lowFootprint.stax.sst.compress";
	public static final String CONF_CRYKEYSTOREFILE = "hadoopoffice.read.security.crypt.credential.keystore.file";
	public static final String CONF_CRYKEYSTORETYPE = "hadoopoffice.read.security.crypt.credential.keystore.type";
	public static final String CONF_CRYKEYSTOREPW = "hadoopoffice.read.security.crypt.credential.keystore.password";
	public static final String CONF_CRYKEYSTOREALIAS = "hadoopoffice.read.security.crypt.credential.keystore.alias";
	public static final String CONF_VERIFYSIGNATURE = "hadoopoffice.read.security.sign.verifysignature";
	public static final String CONF_SIGTRUSTFILE = "hadoopoffice.read.security.sign.truststore.file";
	public static final String CONF_SIGTRUSTTYPE = "hadoopoffice.read.security.sign.truststore.type";
	public static final String CONF_SIGTRUSTPW = "hadoopoffice.read.security.sign.truststore.password";
	public static final String CONF_READHEADER = "hadoopoffice.read.header.read";
	public static final String CONF_HEADERIGNOREHEADERALLSHEETS = "hadoopoffice.read.header.skipheaderinallsheets";
	public static final String CONF_SKIPLINES = "hadoopoffice.read.sheet.skiplines.num";
	public static final String CONF_SKIPLINESALLSHEETS = "hadoopoffice.read.sheet.skiplines.allsheets";
	public static final String CONF_COLUMNNAMESREGEX = "hadoopoffice.read.header.column.names.regex";
	public static final String CONF_COLUMNNAMESREPLACE = "hadoopoffice.read.header.column.names.replace";
	public static final String CONF_EMULATECSV = "hadoopoffice.read.emulateCSV";
	
	public static final String CONF_SIMPLEDATEFORMAT = "hadoopoffice.read.simple.dateFormat";

	public static final String CONF_SIMPLEDATEPATTERN = "hadoopoffice.read.simple.datePattern";

	public static final String CONF_SIMPLEDATETIMEFORMAT = "hadoopoffice.read.simple.dateTimeFormat";

	public static final String CONF_SIMPLEDATETIMEPATTERN = "hadoopoffice.read.simple.dateTimePattern";
	
	public static final String CONF_SIMPLEDECIMALFORMAT = "hadoopoffice.read.simple.decimalFormat";
	
	
	
	
	public static final String OPTION_LOWFOOTPRINT_PARSER_STAX = "stax"; // STAX parser uses pull mode meaning a real improvement for reading XML-based spreadsheet formats in low footprint mode
	public static final String OPTION_LOWFOOTPRINT_PARSER_SAX = "sax"; // SAX parser uses push mode meaning despite low footprint mode a significant amount of data needs to be kept in memory
	
	public static final String DEFAULT_MIMETYPE = "";
	public static final String DEFAULT_LOCALE = "";
	public static final String DEFAULT_SHEETS = "";
	public static final boolean DEFAULT_LINKEDWB = false;
	public static final String DEFAULT_LINKEDWB_LOCATION = "";
	public static final boolean DEFAULT_IGNOREMISSINGLINKEDWB = false;

	public static final boolean DEFAULT_LOWFOOTPRINT = false;

	public static final String DEFAULT_CRYKEYSTOREFILE = "";
	public static final String DEFAULT_LOWFOOTPRINT_PARSER = HadoopOfficeReadConfiguration.OPTION_LOWFOOTPRINT_PARSER_STAX;
	public static final int DEFAULT_LOWFOOTPRINT_STAX_CACHE = 10000;
	public static final boolean DEFAULT_LOWFOOTPRINT_STAX_COMPRESS = false;
	
	public static final String DEFAULT_CRYKEYSTORETYPE = "JCEKS";
	public static final String DEFAULT_CRYKEYSTOREPW = "";
	public static final String DEFAULT_CRYKEYSTOREALIAS = "";

	public static final boolean DEFAULT_VERIFYSIGNATURE = false;

	public static final String DEFAULT_SIGTRUSTFILE = "";
	public static final String DEFAULT_SIGTRUSTTYPE = "JKS";
	public static final String DEFAULT_SIGTRUSTPW = "";
	
	
	
	public static final boolean DEFAULT_READHEADER = false;
	public static final boolean DEFAULT_HEADERIGNOREHEADERALLSHEETS = false;
	public static final Integer DEFAULT_SKIPLINES = 0;
	public static final boolean DEFAULT_SKIPLINESALLSHEETS = false;
	
	public static final String DEFAULT_COLUMNNAMESREGEX = "";
	public static final String DEFAULT_COLUMNNAMESREPLACE = "";

	
	public static final String DEFAULT_SIMPLEDATEFORMAT = "US";

	public static final String DEFAULT_SIMPLEDATEPATTERN = "";

	public static final String DEFAULT_SIMPLEDATETIMEFORMAT = "US";

	public static final String DEFAULT_SIMPLEDATETIMEPATTERN = "";
	
	public static final String DEFAULT_SIMPLEDECIMALFORMAT = "";
	public static final boolean DEFAULT_EMULATECSV = false;
	
	
	
	private String fileName;
	private String mimeType = null;
	private String localeStrBCP47 = null;
	private String sheets = null;
	private Locale locale = null;
	private boolean readLinkedWorkbooks = false;
	private String linkedWorkbookLocation;
	private boolean ignoreMissingLinkedWorkbooks = false;
	private String password = null;
	private Map<String, String> metadataFilter;
	private Map<String, String> linkedWBCredentialMap;
	private boolean lowFootprint;
	private String lowFootprintParser;
	private String cryptKeystoreFile;
	private String cryptKeystoreType;
	private String cryptKeystorePassword;
	private String cryptKeystoreAlias;
	private boolean verifySignature;
	private String sigTruststoreFile;
	private String sigTruststoreType;
	private String sigTruststorePassword;
	private Set<X509Certificate> x509CertificateChain;
	private boolean readHeader;
	private boolean ignoreHeaderInAllSheets;
    private int skipLines;
    private boolean skipLinesAllSheets;
	private String columnNameRegex;
	private String columnNameReplace;
	private SimpleDateFormat simpleDateFormat;
	private SimpleDateFormat simpleDateTimeFormat;
	private DecimalFormat simpleDecimalFormat;
	private int sstCacheSize;
	private boolean compressSST;
	private boolean emulateCSV;
	/*
	 * Create an empty configuration
	 * 
	 */
	public HadoopOfficeReadConfiguration() {
		// create an empty configuration
		this.mimeType = HadoopOfficeReadConfiguration.DEFAULT_MIMETYPE;
		this.sheets = HadoopOfficeReadConfiguration.DEFAULT_SHEETS;
		this.localeStrBCP47 = HadoopOfficeReadConfiguration.DEFAULT_LOCALE;
		if (!("".equals(localeStrBCP47))) { // create locale
			this.locale = new Locale.Builder().setLanguageTag(this.localeStrBCP47).build();
		} else {
			this.locale = Locale.getDefault();
		}
		this.readLinkedWorkbooks = HadoopOfficeReadConfiguration.DEFAULT_LINKEDWB;
		this.ignoreMissingLinkedWorkbooks = HadoopOfficeReadConfiguration.DEFAULT_IGNOREMISSINGLINKEDWB;
		this.setLinkedWorkbookLocation(HadoopOfficeReadConfiguration.DEFAULT_LINKEDWB_LOCATION);
		this.password = null; // null if no password is set

		this.setCryptKeystoreFile(HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTOREFILE);
		this.setCryptKeystoreType(HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTORETYPE);
		this.setCryptKeystorePassword(HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTOREPW);
		this.setCryptKeystoreAlias(HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTOREALIAS);

		this.setVerifySignature(HadoopOfficeReadConfiguration.DEFAULT_VERIFYSIGNATURE);

		this.setSigTruststoreFile(HadoopOfficeReadConfiguration.DEFAULT_SIGTRUSTFILE);
		this.setSigTruststoreType(HadoopOfficeReadConfiguration.DEFAULT_SIGTRUSTTYPE);
		this.setSigTruststorePassword(HadoopOfficeReadConfiguration.DEFAULT_SIGTRUSTPW);
		this.setReadHeader(HadoopOfficeReadConfiguration.DEFAULT_READHEADER);
		this.setIgnoreHeaderInAllSheets(HadoopOfficeReadConfiguration.DEFAULT_HEADERIGNOREHEADERALLSHEETS);
		this.setSkipLines(HadoopOfficeReadConfiguration.DEFAULT_SKIPLINES);
	    this.setSkipLinesAllSheets(HadoopOfficeReadConfiguration.DEFAULT_SKIPLINESALLSHEETS);
	    this.setColumnNameRegex(HadoopOfficeReadConfiguration.DEFAULT_COLUMNNAMESREGEX);
	    this.setColumnNameReplace(HadoopOfficeReadConfiguration.DEFAULT_COLUMNNAMESREPLACE);
		this.lowFootprint = HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT;

		this.setLowFootprintParser(HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT_PARSER);
		this.setEmulateCSV(HadoopOfficeReadConfiguration.DEFAULT_EMULATECSV);
		// set date for simple format
		Locale dateLocale = Locale.getDefault();
		if (!("".equals(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATEFORMAT))) { // create locale
			dateLocale = new Locale.Builder().setLanguageTag(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATEFORMAT).build();
		}
		this.setSimpleDateFormat((SimpleDateFormat) DateFormat.getDateInstance(DateFormat.SHORT, dateLocale));
		// set dateTime for simple format
		Locale dateTimeLocale = Locale.getDefault();
		if (!("".equals(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATETIMEFORMAT))) { // create locale
			dateTimeLocale = new Locale.Builder().setLanguageTag(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATETIMEFORMAT).build();
		}
		this.setSimpleDateTimeFormat((SimpleDateFormat) DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, dateTimeLocale));
		// check if should set pattern for date instead of locale
		if (!"".equals(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATEPATTERN)) {
			this.setSimpleDateFormat(new SimpleDateFormat(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATEPATTERN));
		}
		if (!"".equals(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATETIMEPATTERN)) {
			this.setSimpleDateTimeFormat(new SimpleDateFormat(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATETIMEPATTERN));
		}
		// set decimal for simple format
		Locale decimallocale = Locale.getDefault();
		if (!"".equals(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDECIMALFORMAT)) {
			decimallocale = new Locale.Builder().setLanguageTag(HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDECIMALFORMAT).build();
		}
		this.setSimpleDecimalFormat((DecimalFormat) NumberFormat.getInstance(decimallocale));
		
		this.setX509CertificateChain(new HashSet<>());
		this.setSstCacheSize(HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT_STAX_CACHE);
		this.setCompressSST(HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT_STAX_COMPRESS);
	
	}

	/**
	 * Reasd HadoopOffice configuration from Hadoop configuration
	 * 
	 * @param conf <ul>
	 *            <li> hadoopoffice.read.mimeType: Mimetype of the document </li>
	 *            <li> hadoopoffice.read.locale: Locale of the document (e.g. needed for
	 *            interpreting spreadsheets) in the BCP47 format (cf.
	 *            https://tools.ietf.org/html/bcp47). If not specified then default
	 *            system locale will be used. </li>
	 *            <li> hadoopoffice.read.sheets: A ":"
	 *            separated list of sheets to be read. If not specified then all
	 *            sheets will be read one after the other</li>
	 *            <li> hadoopoffice.read.linkedworkbooks: true if linkedworkbooks should
	 *            be fetched. They must be in the same folder as the main workbook.
	 *            Linked Workbooks will be processed together with the main workbook
	 *            on one node and thus it should be avoided to have a lot of linked
	 *            workbooks. It does only read the linked workbooks that are
	 *            directly linked to the main workbook. Default: false</li>
	 *            <li> hadoopoffice.read.ignoremissinglinkedworkbooks: true if missing
	 *            linked workbooks should be ignored. Default: false</li>
	 *            <li> hadoopoffice.read.linkedworkbooks.location (as of 1.2.3): location of linked workbooks (only one folder). If set to empty String then the same folder as the main excel file is used. Default: set to emptyString</li>
	 *            <li> hadoopoffice.read.security.crypt.password: if set then</li>
	 *            <li> hadoopoffice will try to decrypt the file</li>
	 *            <li> hadoopoffice.read.security.crypt.linkedworkbooks.*: if set then
	 *            hadoopoffice will try to decrypt all the linked workbooks where a
	 *            password has been specified. If no password is specified then it
	 *            is assumed that the linked workbook is not encrypted. Example:
	 *            Property key for file "linkedworkbook1.xlsx" is
	 *            "hadoopoffice.read.security.crypt.linkedworkbooks.linkedworkbook1.xslx".
	 *            Value is the password. You must not include path or protocol
	 *            information in the filename </li>
	 *            <li> hadoopoffice.read.filter.metadata:
	 *            filters documents according to metadata. For example,
	 *            hadoopoffice.read.filter.metadata.author will filter by author and
	 *            the filter defined as value. Filtering is done by the parser and
	 *            it is recommended that it supports regular expression for
	 *            filtering, but this is up to the parser!</li>
	 *            <li> hadoopoffice.read.lowfootprint: uses low memory/cpu footprint for
	 *            reading documents. Note: In this mode certain features are not
	 *            available, such as reading formulas. Default: false
	 *            hadoopoffice.read.security.crypt.credential.keystore.file:
	 *            keystore file that is used to store credentials, such as
	 *            passwords, for reading secured office documents. Note that the
	 *            alias in the keystore needs to correspond to the filename (without
	 *            the path)</li>
	 *            <li> hadoopoffice.read.lowfootprint.parser: Only valid for new Excel files. Parser to be used for low footprint: stax or sax. SAX consumes more memory, but can be faster in case of encrypted files. Default: stax</li>
	 *            <li> hadoopoffice.read.lowFootprint.stax.sst.cache: if stax parser is used a cache size can be defined for the so-called sharedstringtable (an Excel concept where it stores all unique strings, this can save space, but is not very memory efficient for large files). The cache can be -1 (everything in-memory), 0 (nothing in memory), n{@literal >}0 (n entries in cache). All that does not fit in the cache will be swapped to disk and read from disk when needed. This might be slow (especially if source file is encrypted, because the sst table is stored in this case on disk as well encrypted). Generally the strategy should be that if you have a lot of entries repeating at various positions in the document then you should have a rather large cache in-memory. If you have entries that appear in a sequential manner and ideally do not repeat then you can have a smaller cache. You may need to experiment in case of large Excel files if you want to save memory. Alternatively, provide enough memory and put everything in-memory (can be potentially large!)
	 *            <li> hadoopoffice.read.lowFootprint.stax.sst.compress: compress (gzip) swapped Excel sst items to disk (if stax parser is used). true if should be compressed, false if not. Note: Compression can significantly reduce performance. Default:False
	 *            <li> hadoopoffice.read.security.crypt.credential.keystore.alias: alias
	 *            for the password if different from filename</li>
	 *            <li> hadoopoffice.read.security.crypt.credential.keystore.type:
	 *            keystore type. Default: JCEKS</li>
	 *            <li> hadoopoffice.read.security.crypt.credential.keystore.password:
	 *            keystore password: password of the keystore</li>
	 *            <li>hadoopoffice.read.security.sign.verifysignature: verify digital
	 *            signature, true if it should be verfied, false if not. Default:
	 *            false. Requires to add bc libaries to your dependencies (use
	 *            latest version and upgrade regularly!). Note: The public key is
	 *            included in the document itself and Excel (similarly to POI) does
	 *            only verify if the signature belongs to the supplied public key.
	 *            The link between the public key and a real identity (person) is
	 *            part of other processes.</li>
	 *            <li>hadoopoffice.read.security.sign.truststore.file: file to trust store to validate certification chain (if none is defined then certification chain is not validated) </li>
	 *            <li>hadoopoffice.read.security.sign.truststore.type: format of truststore to validate certification chain. Default: JCEKS</li>
	 *            <li> hadoopoffice.read.security.sign.truststore.password: password for truststore to validate certification chain</li>
	 *            <li>hadoopoffice.read.header.read: if true then the next row of the first sheet is interpreted as header and ignored for subsequent reads (you can get the header row content by calling reader.getOfficeReader().getCurrentParser().getHeader()). Default false</li>
	 *            <li>hadoopoffice.read.header.skipheaderinallsheets: if true then the next row in all sheet is interpreted as header and ignored. Default false.</li>
	 *            <li>hadoopoffice.read.sheet.skiplines.num:number of rows to be skipped of the first Excel sheet. Default: 0. Note: this applies before reading the header. E.g. if the header row is in the 5th line then you can skip the previous 4 lines. </li>
	 *            <li>hadoopoffice.read.sheet.skiplines.allsheets: skip number of rows configured using the previous option in all sheets.</li>
	 *            <li>hadoopoffice.read.header.column.names.regex: regex to identify header names to be replaced (e.g. all dots in header names). Default: "" (no replacement)</li>
	 *            <li>hadoopoffice.read.header.column.names.replace: string to replace all occurrences identified by hadoopoffice.read.header.column.names.regex in header names (e.g. replace by -). Default: "" (removal of identified occurrences)</li>
	 *            <li>hadoopoffice.read.simple.dateFormat: applies only to HadoopOffice components that use the Converter to convert SpreadSheetCellDAOs into simple Java objects.  Describes the date format to interpret dates using the BCP47 notation. Note that even in non-US Excel versions Excel stores them most of the times internally in US format. Leave it empty for using the systems locale. Default: "US".</li>
	 *            <li>hadoopoffice.read.simple.datePattern: applies only to HadoopOffice components that use the Converter to convert SpreadSheetCellDAOs into simple Java objects. Overrides "hadoopoffice.read.simple.dateFormat" - describes a date pattern according to the pattern in SimpleDateFormat - you can define any pattern that dates have</li>
	 *            <li>hadoopoffice.read.simple.dateTimeFormat: applies only to HadoopOffice components that use the Converter to convert SpreadSheetCellDAOs into simple Java objects. Describes the date/time format to interpret date/timestamps using the BCP47 notation. Leave it empty for using the systems locale. Default: "US". </li>
	 *            <li>hadoopoffice.read.simple.dateTimePattern: applies only to HadoopOffice components that use the Converter to convert SpreadSheetCellDAOs into simple Java objects. Overrides "hadoopoffice.read.simple.dateTimeFormat" - describes a date/time pattern according to the pattern in SimpleDateFormat - you can define any pattern that date/time have. Defaults to java.sql.Timestamp, if not specified</li>
	 *            <li>hadoopoffice.read.simple.decimalFormat: applies only to HadoopOffice components that use the Converter to convert SpreadSheetCellDAOs into simple Java objects. Describes the decimal format to interpret decimal numbers using the BCP47 notation. Leave it empty for using the systems locale. Default: "".</li>
     *            <li>hadoopoffice.read.emulateCSV (since 1.2.1): Simulates when reading Excel to interpret content as if the Excel would have been saved as CSV. Technically it is based on the [emulateCSV option of the DataFormatter](https://poi.apache.org/apidocs/dev/org/apache/poi/ss/usermodel/DataFormatter.html#DataFormatter-java.util.Locale-boolean-) in Apache POI. Default: false</li>
	 *            </ul>
	 * 
	 */
	
	public HadoopOfficeReadConfiguration(Configuration conf) {
		this.mimeType = conf.get(HadoopOfficeReadConfiguration.CONF_MIMETYPE,
				HadoopOfficeReadConfiguration.DEFAULT_MIMETYPE);
		this.sheets = conf.get(HadoopOfficeReadConfiguration.CONF_SHEETS, HadoopOfficeReadConfiguration.DEFAULT_SHEETS);
		this.localeStrBCP47 = conf.get(HadoopOfficeReadConfiguration.CONF_LOCALE,
				HadoopOfficeReadConfiguration.DEFAULT_LOCALE);
		if (!("".equals(localeStrBCP47))) { // create locale
			this.locale = new Locale.Builder().setLanguageTag(this.localeStrBCP47).build();
		}
		this.readLinkedWorkbooks = conf.getBoolean(HadoopOfficeReadConfiguration.CONF_LINKEDWB,
				HadoopOfficeReadConfiguration.DEFAULT_LINKEDWB);
		this.ignoreMissingLinkedWorkbooks = conf.getBoolean(HadoopOfficeReadConfiguration.CONF_IGNOREMISSINGWB,
				HadoopOfficeReadConfiguration.DEFAULT_IGNOREMISSINGLINKEDWB);
		this.linkedWorkbookLocation = conf.get(HadoopOfficeReadConfiguration.CONF_LINKEDWB_LOCATION,
				HadoopOfficeReadConfiguration.DEFAULT_LINKEDWB_LOCATION);
		this.password = conf.get(HadoopOfficeReadConfiguration.CONF_DECRYPT); // null if no password is set
		this.metadataFilter = HadoopUtil.parsePropertiesFromBase(conf,
				HadoopOfficeReadConfiguration.CONF_FILTERMETADATA);
		this.linkedWBCredentialMap = HadoopUtil.parsePropertiesFromBase(conf,
				HadoopOfficeReadConfiguration.CONF_DECRYPTLINKEDWBBASE);
		this.lowFootprint = conf.getBoolean(HadoopOfficeReadConfiguration.CONF_LOWFOOTPRINT,
				HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT);
	
		this.setCryptKeystoreFile(conf.get(HadoopOfficeReadConfiguration.CONF_CRYKEYSTOREFILE,
				HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTOREFILE));
		this.setCryptKeystoreType(conf.get(HadoopOfficeReadConfiguration.CONF_CRYKEYSTORETYPE,
				HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTORETYPE));
		this.setCryptKeystorePassword(conf.get(HadoopOfficeReadConfiguration.CONF_CRYKEYSTOREPW,
				HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTOREPW));
		this.setCryptKeystoreAlias(conf.get(HadoopOfficeReadConfiguration.CONF_CRYKEYSTOREALIAS,
				HadoopOfficeReadConfiguration.DEFAULT_CRYKEYSTOREALIAS));

		this.setVerifySignature(conf.getBoolean(HadoopOfficeReadConfiguration.CONF_VERIFYSIGNATURE,
				HadoopOfficeReadConfiguration.DEFAULT_VERIFYSIGNATURE));

		this.setSigTruststoreFile(conf.get(HadoopOfficeReadConfiguration.CONF_SIGTRUSTFILE,
				HadoopOfficeReadConfiguration.DEFAULT_SIGTRUSTFILE));
		this.setSigTruststoreType(conf.get(HadoopOfficeReadConfiguration.CONF_SIGTRUSTTYPE,
				HadoopOfficeReadConfiguration.DEFAULT_SIGTRUSTTYPE));
		this.setSigTruststorePassword(conf.get(HadoopOfficeReadConfiguration.CONF_SIGTRUSTPW,
				HadoopOfficeReadConfiguration.DEFAULT_SIGTRUSTPW));

		this.setLowFootprintParser(conf.get(HadoopOfficeReadConfiguration.CONF_LOWFOOTPRINT_PARSER,HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT_PARSER));

		this.setReadHeader(conf.getBoolean(HadoopOfficeReadConfiguration.CONF_READHEADER, HadoopOfficeReadConfiguration.DEFAULT_READHEADER));
		this.setIgnoreHeaderInAllSheets(conf.getBoolean(HadoopOfficeReadConfiguration.CONF_HEADERIGNOREHEADERALLSHEETS, HadoopOfficeReadConfiguration.DEFAULT_HEADERIGNOREHEADERALLSHEETS));
		this.setSkipLines(conf.getInt(HadoopOfficeReadConfiguration.CONF_SKIPLINES, HadoopOfficeReadConfiguration.DEFAULT_SKIPLINES));
	    this.setSkipLinesAllSheets(conf.getBoolean(HadoopOfficeReadConfiguration.CONF_SKIPLINESALLSHEETS, HadoopOfficeReadConfiguration.DEFAULT_SKIPLINESALLSHEETS));
		
	    this.setColumnNameRegex(conf.get(HadoopOfficeReadConfiguration.CONF_COLUMNNAMESREGEX,HadoopOfficeReadConfiguration.DEFAULT_COLUMNNAMESREGEX));
	    this.setColumnNameReplace(conf.get(HadoopOfficeReadConfiguration.CONF_COLUMNNAMESREPLACE,HadoopOfficeReadConfiguration.DEFAULT_COLUMNNAMESREPLACE));

		// set date for simple format
		Locale dateLocale = new Locale.Builder().setLanguageTag(conf.get(HadoopOfficeReadConfiguration.CONF_SIMPLEDATEFORMAT,HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATEFORMAT)).build();	
		this.setSimpleDateFormat((SimpleDateFormat) DateFormat.getDateInstance(DateFormat.SHORT, dateLocale));
		// set dateTime for simple format

		Locale	dateTimeLocale = new Locale.Builder().setLanguageTag(conf.get(HadoopOfficeReadConfiguration.CONF_SIMPLEDATETIMEFORMAT,HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATETIMEFORMAT)).build();
		this.setSimpleDateTimeFormat((SimpleDateFormat) DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, dateTimeLocale));
		// check if should set pattern for date instead of locale
		String datePattern=conf.get(HadoopOfficeReadConfiguration.CONF_SIMPLEDATEPATTERN,HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATEPATTERN);
		if (!"".equals(datePattern)) {
			this.setSimpleDateFormat(new SimpleDateFormat(datePattern));
		}
		String dateTimePattern=conf.get(HadoopOfficeReadConfiguration.CONF_SIMPLEDATETIMEPATTERN,HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDATETIMEPATTERN);
		if (!"".equals(dateTimePattern)) {
			this.setSimpleDateFormat(new SimpleDateFormat(dateTimePattern));
		}
		// set decimal for simple format
		String decimaleStr = conf.get(HadoopOfficeReadConfiguration.CONF_SIMPLEDECIMALFORMAT,HadoopOfficeReadConfiguration.DEFAULT_SIMPLEDECIMALFORMAT);
		Locale decimallocale = Locale.getDefault();
		if (!"".equals(decimaleStr)){
			decimallocale = new Locale.Builder().setLanguageTag(decimaleStr).build();
		}
		this.setSimpleDecimalFormat((DecimalFormat) NumberFormat.getInstance(decimallocale));
		this.setEmulateCSV(conf.getBoolean(HadoopOfficeReadConfiguration.CONF_EMULATECSV,HadoopOfficeReadConfiguration.DEFAULT_EMULATECSV));
	    
	    this.setX509CertificateChain(new HashSet<>());
	    this.setSstCacheSize(conf.getInt(HadoopOfficeReadConfiguration.CONF_LOWFOOTPRINT_STAX_CACHE, HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT_STAX_CACHE));
	    this.setCompressSST(conf.getBoolean(HadoopOfficeReadConfiguration.CONF_LOWFOOTPRINT_STAX_COMPRESS, HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT_STAX_COMPRESS));
	}

	/*
	 * Returns the configuration for filtering sheets
	 */
	public String getSheets() {
		return this.sheets;
	}

	/*
	 * Set sheets
	 * 
	 * @param sheets comma-separated list of sheets to take into account for
	 * parsing, null if all should be taken into account
	 * 
	 */
	public void setSheets(String sheets) {
		this.sheets = sheets;
	}

	/*
	 * Returns the configured mimetype
	 * 
	 */
	public String getMimeType() {
		return this.mimeType;
	}

	/*
	 * Sets the configured mimetype
	 * 
	 * @param mimetype MimeType
	 * 
	 * 
	 */

	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	/*
	 * Locale used for interpreting values
	 * 
	 * @return locale or null if the default locale should be used
	 * 
	 */
	public Locale getLocale() {
		return this.locale;
	}

	/*
	 * Set locale used for interpreting values
	 * 
	 * @param locale locale or null if default locale should be used
	 * 
	 */
	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	/*
	 * should linked workbooks be read
	 * 
	 * @return true, if yes, false if not
	 */
	public boolean getReadLinkedWorkbooks() {
		return this.readLinkedWorkbooks;
	}

	/**
	 * Set if linked workbooks should be read
	 * 
	 * @param readLinkedWorkbooks
	 *            true if yes, false if not
	 */

	public void setReadLinkedWorkbooks(boolean readLinkedWorkbooks) {
		this.readLinkedWorkbooks = readLinkedWorkbooks;
	}

	/*
	 * Should missed linked workbooks be ignored or not
	 * 
	 * @return true, if yes, false if not
	 * 
	 */
	public boolean getIgnoreMissingLinkedWorkbooks() {
		return this.ignoreMissingLinkedWorkbooks;
	}

	/**
	 * Set if missed linked workbooks should be ignored
	 * 
	 * @param ignoreMissingLinkedWorkbooks
	 *            true, if yes, false, if not
	 * 
	 */

	public void setIgnoreMissingLinkedWorkbooks(boolean ignoreMissingLinkedWorkbooks) {
		this.ignoreMissingLinkedWorkbooks = ignoreMissingLinkedWorkbooks;
	}

	/*
	 * Password for file, if any
	 * 
	 * @return password, or null if no password
	 * 
	 */

	public String getPassword() {
		return this.password;
	}

	/*
	 * Set the password
	 * 
	 * @param password password
	 * 
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Meta data filter for filtering documents not part of the filter
	 * 
	 * @return key/value map with filter values
	 */
	public Map<String, String> getMetaDataFilter() {
		return this.metadataFilter;
	}

	/*
	 * Set meta data filer for filtering documents not part of the filter
	 * 
	 * @param metadataFilter key/value map for filtering metadata
	 * 
	 */

	public void setMetaDataFilter(Map<String, String> metadataFilter) {
		this.metadataFilter = metadataFilter;
	}

	/*
	 * Get credential map for linked workbooks
	 * 
	 * @return credential map for linked workbooks
	 * 
	 */

	public Map<String, String> getLinkedWBCredentialMap() {
		return this.linkedWBCredentialMap;
	}

	/*
	 * Set the credential map for linked workbooks
	 * 
	 * 
	 * @param linkedWBCredentialMap new credential map for linked workboooks
	 * 
	 */

	public void setLinkedWBCredentialMap(Map<String, String> linkedWBCredentialMap) {
		this.linkedWBCredentialMap = linkedWBCredentialMap;

	}

	/*
	 * returns filename of the document to which the configuration belongs
	 * 
	 * @return filename
	 */
	public String getFileName() {
		return this.fileName;
	}

	/*
	 * Sets the filename of the document to which this configuration belongs
	 * 
	 * 
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	/*
	 * Should files be read in low footprint mode or not
	 * 
	 * @return true, if yes, false if not
	 * 
	 */
	public boolean getLowFootprint() {
		return this.lowFootprint;
	}

	/**
	 * Set if files should be read in low footprint mode or not
	 * 
	 * @param lowFootprint
	 *            true, if yes, false, if not
	 * 
	 */

	public void setLowFootprint(boolean lowFootprint) {
		this.lowFootprint = lowFootprint;
	}

	public boolean getVerifySignature() {
		return verifySignature;
	}

	public void setVerifySignature(boolean verifySignature) {
		this.verifySignature = verifySignature;
	}

	public String getCryptKeystoreFile() {
		return cryptKeystoreFile;
	}

	public void setCryptKeystoreFile(String cryptKeystoreFile) {
		this.cryptKeystoreFile = cryptKeystoreFile;
	}

	public String getCryptKeystoreType() {
		return cryptKeystoreType;
	}

	public void setCryptKeystoreType(String cryptKeystoreType) {
		this.cryptKeystoreType = cryptKeystoreType;
	}

	public String getCryptKeystorePassword() {
		return cryptKeystorePassword;
	}

	public void setCryptKeystorePassword(String cryptKeystorePassword) {
		this.cryptKeystorePassword = cryptKeystorePassword;
	}

	public String getCryptKeystoreAlias() {
		return cryptKeystoreAlias;
	}

	public void setCryptKeystoreAlias(String cryptKeystoreAlias) {
		this.cryptKeystoreAlias = cryptKeystoreAlias;
	}

	public String getSigTruststoreFile() {
		return sigTruststoreFile;
	}

	public void setSigTruststoreFile(String sigTruststoreFile) {
		this.sigTruststoreFile = sigTruststoreFile;
	}

	public String getSigTruststoreType() {
		return sigTruststoreType;
	}

	public void setSigTruststoreType(String sigTruststoreType) {
		this.sigTruststoreType = sigTruststoreType;
	}

	public String getSigTruststorePassword() {
		return sigTruststorePassword;
	}

	public void setSigTruststorePassword(String sigTruststorePassword) {
		this.sigTruststorePassword = sigTruststorePassword;
	}

	public Set<X509Certificate> getX509CertificateChain() {
		return x509CertificateChain;
	}

	public void setX509CertificateChain(Set<X509Certificate> x509CertificateChain) {
		this.x509CertificateChain = x509CertificateChain;
	}

	public boolean getReadHeader() {
		return readHeader;
	}

	public void setReadHeader(boolean readHeader) {
		this.readHeader = readHeader;
	}

	public boolean getIgnoreHeaderInAllSheets() {
		return ignoreHeaderInAllSheets;
	}

	public void setIgnoreHeaderInAllSheets(boolean ignoreHeaderInAllSheets) {
		this.ignoreHeaderInAllSheets = ignoreHeaderInAllSheets;
	}

	public int getSkipLines() {
		return skipLines;
	}

	public void setSkipLines(int skipLines) {
		this.skipLines = skipLines;
	}

	public boolean getSkipLinesAllSheets() {
		return skipLinesAllSheets;
	}

	public void setSkipLinesAllSheets(boolean skipLinesAllSheets) {
		this.skipLinesAllSheets = skipLinesAllSheets;
	}

	public String getLowFootprintParser() {
		return lowFootprintParser;
	}

	public void setLowFootprintParser(String lowFootprintParser) {
		this.lowFootprintParser = lowFootprintParser;
	}

	public String getColumnNameRegex() {
		return columnNameRegex;
	}

	public void setColumnNameRegex(String columnNameRegex) {
		this.columnNameRegex = columnNameRegex;
	}

	public String getColumnNameReplace() {
		return columnNameReplace;
	}

	public void setColumnNameReplace(String columnNameReplace) {
		this.columnNameReplace = columnNameReplace;
	}

	public SimpleDateFormat getSimpleDateFormat() {
		return simpleDateFormat;
	}

	public void setSimpleDateFormat(SimpleDateFormat simpleDateFormat) {
		this.simpleDateFormat = simpleDateFormat;
	}

	public SimpleDateFormat getSimpleDateTimeFormat() {
		return simpleDateTimeFormat;
	}

	public void setSimpleDateTimeFormat(SimpleDateFormat simpleDateTimeFormat) {
		this.simpleDateTimeFormat = simpleDateTimeFormat;
	}

	public DecimalFormat getSimpleDecimalFormat() {
		return simpleDecimalFormat;
	}

	public void setSimpleDecimalFormat(DecimalFormat simpleDecimalFormat) {
		this.simpleDecimalFormat = simpleDecimalFormat;
	}

	public int getSstCacheSize() {
		return sstCacheSize;
	}

	public void setSstCacheSize(int sstCacheSize) {
		this.sstCacheSize = sstCacheSize;
	}

	public boolean getCompressSST() {
		return compressSST;
	}

	public void setCompressSST(boolean compressSST) {
		this.compressSST = compressSST;
	}



	/**
	 * @return the emulateCSV
	 */
	public boolean getEmulateCSV() {
		return emulateCSV;
	}

	/**
	 * @param emulateCSV the emulateCSV to set
	 */
	public void setEmulateCSV(boolean emulateCSV) {
		this.emulateCSV = emulateCSV;
	}

	/**
	 * @return the linkedWorkbookLocation
	 */
	public String getLinkedWorkbookLocation() {
		return linkedWorkbookLocation;
	}

	/**
	 * @param linkedWorkbookLocation the linkedWorkbookLocation to set
	 */
	public void setLinkedWorkbookLocation(String linkedWorkbookLocation) {
		this.linkedWorkbookLocation = linkedWorkbookLocation;
	}
	


}
