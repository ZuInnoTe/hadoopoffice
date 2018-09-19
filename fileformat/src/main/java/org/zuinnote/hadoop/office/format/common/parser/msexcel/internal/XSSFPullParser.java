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

import java.io.InputStream;
import java.util.ArrayList;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.util.StaxHelper;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;

/**
 * Parses .xlsx files in pull mode instead of push
 *
 */
public class XSSFPullParser {
	public static final String CELLTYPE_STRING = "s";

	public static final String CELLTYPE_NUMBER = "n";
	public static final String CELL_NOT_PROCESSABLE = "not processable";
	private static final Log LOG = LogFactory.getLog(XSSFPullParser.class.getName());
	private boolean nextBeingCalled;
	private boolean finalized;
	private int nextRow;
	private int currentRow;
	private XMLEventReader xer;
	private SharedStringsTable sst;
	private StylesTable styles;
	private String sheetName;
	private DataFormatter dataFormatter;
	boolean isDate1904;

	/**
	 * 
	 * @param sheetName        name of sheet
	 * @param sheetInputStream sheet in xlsx format input stream
	 * @param sst              Shared strings table of Excel file
	 * @param styles           StylesTable of the document
	 * @param isDate1904       date format 1904 (true) or 1900 (false)
	 * @throws XMLStreamException
	 */
	public XSSFPullParser(String sheetName, InputStream sheetInputStream, SharedStringsTable sst, StylesTable styles,
			DataFormatter dataFormatter, boolean isDate1904) throws XMLStreamException {
		this.sheetName = sheetName;
		this.xer = StaxHelper.newXMLInputFactory().createXMLEventReader(sheetInputStream);
		this.nextBeingCalled = false;
		this.finalized = false;
		this.nextRow = 1;
		this.currentRow = 1;
		this.sst = sst;
		this.styles = styles;
		this.dataFormatter = dataFormatter;
		this.isDate1904 = isDate1904;
	}

	public boolean hasNext() throws XMLStreamException {
		this.nextBeingCalled = true;
		if (this.finalized) { // we finished already - no more to read
			return false;
		}
		if ((this.currentRow > 1) && (this.currentRow <= this.nextRow)) { // we still have to process an empty row
			return true;
		}
		// search for the next row
		while (this.xer.hasNext()) {
			XMLEvent xe = xer.nextEvent();
			if (xe.isStartElement() && xe.asStartElement().getName().getLocalPart().equalsIgnoreCase("row")) { // we
																												// found
																												// a row
				// get row number.
				Attribute at = xe.asStartElement().getAttributeByName(new QName("r"));
				String atValue = at.getValue();
				this.nextRow = Integer.valueOf(atValue);
				return true;
			}
		}
		this.finalized = true;
		return false;
	}

	public Object[] getNext() throws XMLStreamException, FormatNotUnderstoodException {
		Object[] result = null;
		if (!this.nextBeingCalled) { // skip to the next tag

			if (this.hasNext() == false) {

				return null;
			}
		}
		if (this.finalized) { // no more to read
			return null;
		}
		// check
		ArrayList<SpreadSheetCellDAO> cells = new ArrayList<>();
		if (this.currentRow == this.nextRow) { // only if we have a row to report
			// read through row, cf.
			// http://download.microsoft.com/download/3/E/3/3E3435BD-AA68-4B32-B84D-B633F0D0F90D/SpreadsheetMLBasics.ppt
			int currentCellCount = 0;
			while (this.xer.hasNext()) {
				XMLEvent xe = xer.nextEvent();
				// read+
				if (xe.isEndElement()) {
					if (xe.asEndElement().getName().getLocalPart().equalsIgnoreCase("row")) {
						break; // end of row
					}
				} else if (xe.isStartElement()) {
					if (xe.asStartElement().getName().getLocalPart().equalsIgnoreCase("c")) {

						Attribute cellAddressAT = xe.asStartElement().getAttributeByName(new QName("r"));
						// check if cell is a subsequent cell and add null, if needed
						CellAddress currentCellAddress = new CellAddress(cellAddressAT.getValue());
						for (int i = currentCellCount; i < currentCellAddress.getColumn(); i++) {
							cells.add(null);
							currentCellCount++;
						}
						currentCellCount++; // current cell
						Attribute cellTypeTAT = xe.asStartElement().getAttributeByName(new QName("t"));
						Attribute cellTypeSAT = xe.asStartElement().getAttributeByName(new QName("s"));
						String cellFormattedValue = "";
						String cellFormula = "";
						String cellAddress = cellAddressAT.getValue();

						String cellComment = "";
						String cellSheetName = this.sheetName;

						while ((!xe.isEndElement()) || !((xe.isEndElement())
								&& (xe.asEndElement().getName().getLocalPart().equalsIgnoreCase("c")))) {
							xe = xer.nextEvent();
							if ((xe.isStartElement())
									&& (xe.asStartElement().getName().getLocalPart().equalsIgnoreCase("v"))) {
								// if a cell data type is set (e.g. b boolean (covered?), d date in ISO8601 format, e
								// error, inlineStr (covered), n number (covered), s shared string (covered), str formula string (covered)
								// we return as string
								if (cellTypeTAT != null) {
									XMLEvent xeSubCharacters = xer.nextEvent();
									if (!xeSubCharacters.isCharacters()) {
										LOG.error(
												"Error parsing excel file. Value attribute (v) of cell does not contains characters");
									} else {
										cellFormattedValue = xeSubCharacters.asCharacters().getData();

										if (XSSFPullParser.CELLTYPE_STRING.equals(cellTypeTAT.getValue())) { // need to
																												// read
																												// from
																												// Shared
																												// String
																												// Table
											int strIdx = Integer.valueOf(cellFormattedValue);
											if ((this.sst != null) && (this.sst.getCount() > strIdx)) {
												cellFormattedValue = this.sst.getItemAt(strIdx).getString();
											} else {
												cellFormattedValue = "";
											}
										} else if (XSSFPullParser.CELLTYPE_NUMBER.equals(cellTypeTAT.getValue())) {
											// need to read number and format it (e.g. if it is a date etc.) according
											// to style
											int strStyleIdx = Integer.valueOf(cellTypeSAT.getValue());
											XSSFCellStyle cellStyle = this.styles.getStyleAt(strStyleIdx);
											cellFormattedValue = this.dataFormatter.formatRawCellContents(
													Double.valueOf(cellFormattedValue), cellStyle.getDataFormat(),
													cellStyle.getDataFormatString(), this.isDate1904);
										}

									}
								} else {
									LOG.error("Cannot read celltype");
								}
							} else if ((xe.isStartElement())
									&& (xe.asStartElement().getName().getLocalPart().equalsIgnoreCase("f"))) {
								// read formula
								XMLEvent xeSubCharacters = xer.nextEvent();

								if (!xeSubCharacters.isCharacters()) {
									LOG.error(
											"Error parsing excel file. Formula attribute (f) of cell does not contains characters");
								} else {
									cellFormula = xeSubCharacters.asCharacters().getData();
								}
							} else if ((xe.isStartElement())
									&& (xe.asStartElement().getName().getLocalPart().equalsIgnoreCase("is"))) {
								// read inline string
								cellFormattedValue = this.parseCellInlineStringText(xer);
							}
						}
						cells.add(new SpreadSheetCellDAO(cellFormattedValue, cellComment, cellFormula, cellAddress,
								cellSheetName));

					}
				}
				// else ignore (e.g. col)
			}
		}

		// convert to array
		result = new SpreadSheetCellDAO[cells.size()];
		result = cells.toArray(result);
		// read all cells in row and create SpreadSheetCellDAOs
		this.nextBeingCalled = false;
		this.currentRow++;
		return result;
	}

	/**
	 * Parses an inline string from cell in XML format
	 * 
	 * @param xer XMLEventReader from which to read the inline string content
	 * @throws XMLStreamException           in case the string item cannot be
	 *                                      correctly read from the XML file
	 * @throws FormatNotUnderstoodException in case a string cannot be identified in
	 *                                      cell
	 */
	private String parseCellInlineStringText(XMLEventReader xer)
			throws XMLStreamException, FormatNotUnderstoodException {
		String result = "";
		XMLEvent xe;
		while ((xe = xer.nextTag()).isStartElement()) {
			String elementName = xe.asStartElement().getName().getLocalPart().toUpperCase();
			switch (elementName) {
			case "T": // normal text
				result = xer.getElementText();
				break;
			case "R": // rich text (returned as normal text)
				result = this.parseCellInlineStringRichText(xer);
				break;
			case "RPH": // phonetic (ignored)
			case "PHONETICPR": // phonetic properties (ignored)
				this.skipXMLElementHierarchy(xer);
				break;
			default:
				LOG.error("Unknown inline string tag: " + elementName);
				throw new FormatNotUnderstoodException("Unknown inline string tag: " + elementName);

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
	private String parseCellInlineStringRichText(XMLEventReader xer)
			throws XMLStreamException, FormatNotUnderstoodException {
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
				LOG.error("Unknown rich text inline string tag: " + elementName);
				throw new FormatNotUnderstoodException("Unknown rich text inline string tag: " + elementName);

			}
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

}
