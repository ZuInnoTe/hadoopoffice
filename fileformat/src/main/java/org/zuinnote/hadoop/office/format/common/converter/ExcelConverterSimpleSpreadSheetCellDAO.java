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

package org.zuinnote.hadoop.office.format.common.converter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.util.CellAddress;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBigDecimalDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBooleanDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericByteDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDateDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDoubleDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericFloatDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericIntegerDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericLongDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericNumericDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericShortDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericStringDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericTimestampDataType;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.util.msexcel.MSExcelUtil;

/**
 * This class allows to infer the Java datatypes underlying a SpreadSheet and
 * the corresponding data as Java objects
 *
 */
public class ExcelConverterSimpleSpreadSheetCellDAO implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3281344931609307423L;

	private static final Log LOG = LogFactory.getLog(ExcelConverterSimpleSpreadSheetCellDAO.class.getName());

	private List<GenericDataType> schemaRow;
	private SimpleDateFormat dateFormat;
	private SimpleDateFormat dateTimeFormat;
	private DecimalFormat decimalFormat;

	private Calendar converterCalendar;

	/***
	 * Create a new converter
	 * 
	 * @param dateFormat    format of the dates in the Excel
	 * @param decimalFormat format of the decimals in the Excel
	 */
	public ExcelConverterSimpleSpreadSheetCellDAO(SimpleDateFormat dateFormat, DecimalFormat decimalFormat) {
		this(dateFormat,decimalFormat,null);
	}

	/***
	 * Create a new converter
	 * 
	 * @param dateFormat     format of the dates in the Excel
	 * @param decimalFormat  format of the decimals in the Excel
	 * @param dateTimeFormat format of date time cells in the Excel
	 */
	public ExcelConverterSimpleSpreadSheetCellDAO(SimpleDateFormat dateFormat, DecimalFormat decimalFormat,
			SimpleDateFormat dateTimeFormat) {
		this.schemaRow = new ArrayList<>();
		this.dateFormat = dateFormat;
		this.decimalFormat = decimalFormat;
		this.decimalFormat.setParseBigDecimal(true);
		this.dateTimeFormat = dateTimeFormat;
		this.converterCalendar = Calendar.getInstance(this.dateFormat.getTimeZone());
	}

	/***
	 * This provides another sample to infer schema in form of simple datatypes
	 * (e.g. boolean, byte etc.). You might add as many sample as necessary to get a
	 * precise schema.
	 * 
	 * @param dataRow
	 */
	public void updateSpreadSheetCellRowToInferSchemaInformation(SpreadSheetCellDAO[] dataRow) {
		// check size of cell based on address
		// if necessary add more to schemaRow

		for (SpreadSheetCellDAO currentSpreadSheetCellDAO : dataRow) {
			boolean dataTypeFound = false;
			if (currentSpreadSheetCellDAO != null) {
				// add potential column to list
				int j = new CellAddress(currentSpreadSheetCellDAO.getAddress()).getColumn();
				if (j >= this.schemaRow.size()) {
					// fill up
					for (int x = this.schemaRow.size(); x <= j; x++) {
						this.schemaRow.add(null);
					}
				}
				// check if boolean data type
				if ((currentSpreadSheetCellDAO.getFormattedValue() != null)
						&& (!"".equals(currentSpreadSheetCellDAO.getFormattedValue()))) { // skip null value
					String currentCellValue = currentSpreadSheetCellDAO.getFormattedValue();
					// check if boolean
					if (("TRUE".equals(currentCellValue)) || ("FALSE".equals(currentCellValue))) {
						dataTypeFound = true;
						if (this.schemaRow.get(j) != null) { // check if previous assumption was boolean

							if (!(this.schemaRow.get(j) instanceof GenericBooleanDataType)) {
								// if not then the type needs to be set to string
								this.schemaRow.set(j, new GenericStringDataType());
							}
							// if yes then nothing todo (already boolean)
						} else { // we face this the first time
							this.schemaRow.set(j, new GenericBooleanDataType());
						}
					}
					// check if timestamp using provided format
					if (!dataTypeFound) {
						if (this.dateTimeFormat!=null) { // only if a format is specified
							Date theDate = this.dateTimeFormat.parse(currentCellValue, new ParsePosition(0));
							if (theDate !=null) { // we found indeed a date time
								
								dataTypeFound = true;
								if (this.schemaRow.get(j) != null) { // check if previous assumption was date

									if (!(this.schemaRow.get(j) instanceof GenericTimestampDataType)) {
										// if not then the type needs to be set to string
										this.schemaRow.set(j, new GenericStringDataType());
									} 
								} else { // we face this the first time
									this.schemaRow.set(j, new GenericTimestampDataType());
		
								}
								
							}
						}
					}
					 // check for timestamp using java.sql.Timestamp
					if (!dataTypeFound) {
						
						try {
							java.sql.Timestamp ts = java.sql.Timestamp.valueOf(currentCellValue);
							dataTypeFound=true;
							this.schemaRow.set(j, new GenericTimestampDataType());
						} catch (IllegalArgumentException e) {
							LOG.warn("Could not identify timestamp using TimeStamp.valueOf. Trying last resort Date parsing....");
						}
					}
					// check if date data type
					if (!dataTypeFound) {

						Date theDate = this.dateFormat.parse(currentCellValue, new ParsePosition(0));
						if (theDate != null) { // we have indeed a date

							dataTypeFound = true;
							if (this.schemaRow.get(j) != null) { // check if previous assumption was date

								if (!(this.schemaRow.get(j) instanceof GenericDateDataType)) {
									// if not then the type needs to be set to string
									this.schemaRow.set(j, new GenericStringDataType());
								} 
								
							} else { // we face this the first time
								this.schemaRow.set(j, new GenericDateDataType());
								// check if it has a time component
								
							}
						}
					}
					// check if BigDecimal

					BigDecimal bd = (BigDecimal) this.decimalFormat.parse(currentCellValue, new ParsePosition(0));
					if ((!dataTypeFound) && (bd != null)) {
						BigDecimal bdv = bd.stripTrailingZeros();

						dataTypeFound = true;

						if (this.schemaRow.get(j) != null) { // check if previous assumption was a number

							// check if we need to upgrade to decimal
							if ((bdv.scale() > 0) && (this.schemaRow.get(j) instanceof GenericNumericDataType)) {
								// upgrade to decimal, if necessary
								if (!(this.schemaRow.get(j) instanceof GenericBigDecimalDataType)) {
									this.schemaRow.set(j, new GenericBigDecimalDataType(bdv.precision(), bdv.scale()));
								} else {
									if ((bdv.scale() > ((GenericBigDecimalDataType) this.schemaRow.get(j)).getScale())
											&& (bdv.precision() > ((GenericBigDecimalDataType) this.schemaRow.get(j))
													.getPrecision())) {
										this.schemaRow.set(j,
												new GenericBigDecimalDataType(bdv.precision(), bdv.scale()));
									} else if (bdv.scale() > ((GenericBigDecimalDataType) this.schemaRow.get(j))
											.getScale()) {
										// upgrade scale
										GenericBigDecimalDataType gbd = ((GenericBigDecimalDataType) this.schemaRow
												.get(j));
										gbd.setScale(bdv.scale());
										this.schemaRow.set(j, gbd);
									} else if (bdv.precision() > ((GenericBigDecimalDataType) this.schemaRow.get(j))
											.getPrecision()) {
										// upgrade precision
										// new precision is needed to extend to max scale
										GenericBigDecimalDataType gbd = ((GenericBigDecimalDataType) this.schemaRow
												.get(j));
										int newpre = bdv.precision() + (gbd.getScale() - bdv.scale());
										gbd.setPrecision(newpre);
										this.schemaRow.set(j, gbd);
									}
								}
							} else { // check if we need to upgrade one of the integer types
								// if current is byte
								boolean isByte = false;
								boolean isShort = false;
								boolean isInt = false;
								boolean isLong = true;
								try {
									bdv.longValueExact();
									isLong = true;
									bdv.intValueExact();
									isInt = true;
									bdv.shortValueExact();
									isShort = true;
									bdv.byteValueExact();
									isByte = true;
								} catch (Exception e) {
									LOG.debug("Possible data types: Long: " + isLong + " Int: " + isInt + " Short: "
											+ isShort + " Byte: " + isByte);
								}
								// if it was Numeric before we can ignore testing the byte case, here just for
								// completeness
								if ((isByte) && ((this.schemaRow.get(j) instanceof GenericByteDataType)
										|| (this.schemaRow.get(j) instanceof GenericShortDataType)
										|| (this.schemaRow.get(j) instanceof GenericIntegerDataType)
										|| (this.schemaRow.get(j) instanceof GenericLongDataType))) {
									// if it was Byte before we can ignore testing the byte case, here just for
									// completeness
								} else if ((isShort) && ((this.schemaRow.get(j) instanceof GenericByteDataType))) {
									// upgrade to short
									this.schemaRow.set(j, new GenericShortDataType());
								} else if ((isInt) && ((this.schemaRow.get(j) instanceof GenericShortDataType)
										|| (this.schemaRow.get(j) instanceof GenericByteDataType))) {
									// upgrade to integer
									this.schemaRow.set(j, new GenericIntegerDataType());
								} else if ((!isByte) && (!isShort) && (!isInt)
										&& !((this.schemaRow.get(j) instanceof GenericLongDataType))) {
									// upgrade to long
									this.schemaRow.set(j, new GenericLongDataType());
								}

							}

						} else {
							// we face it for the first time
							// determine value type
							if (bdv.scale() > 0) {
								this.schemaRow.set(j, new GenericBigDecimalDataType(bdv.precision(), bdv.scale()));
							} else {
								boolean isByte = false;
								boolean isShort = false;
								boolean isInt = false;
								boolean isLong = true;
								try {
									bdv.longValueExact();
									isLong = true;
									bdv.intValueExact();
									isInt = true;
									bdv.shortValueExact();
									isShort = true;
									bdv.byteValueExact();
									isByte = true;
								} catch (Exception e) {
									LOG.debug("Possible data types: Long: " + isLong + " Int: " + isInt + " Short: "
											+ isShort + " Byte: " + isByte);
								}
								if (isByte) {
									this.schemaRow.set(j, new GenericByteDataType());
								} else if (isShort) {
									this.schemaRow.set(j, new GenericShortDataType());
								} else if (isInt) {
									this.schemaRow.set(j, new GenericIntegerDataType());
								} else if (isLong) {
									this.schemaRow.set(j, new GenericLongDataType());
								}
							}
						}
					}
					if (!dataTypeFound) {
						// otherwise string
						if (!(this.schemaRow.get(j) instanceof GenericStringDataType)) {
							this.schemaRow.set(j, new GenericStringDataType());
						}

					}

				} else {
					// ignore null values
				}
			}
		}
	}

	/**
	 * Returns a list of objects corresponding to the schema.
	 * 
	 * @return
	 */
	public GenericDataType[] getSchemaRow() {
		GenericDataType[] result = new GenericDataType[this.schemaRow.size()];
		this.schemaRow.toArray(result);
		return result;
	}

	/***
	 * Allows to set a custom schema. Note: the schema must have the the size of the
	 * largest (expected) row in the Excel. In case you do not need a conversion set
	 * the schema for the corresponding column to null
	 * 
	 * @param schemaRow
	 */
	public void setSchemaRow(GenericDataType[] schemaRow) {
		this.schemaRow = new ArrayList<>(Arrays.asList(schemaRow));
	}

	/**
	 * Translate a data row according to the currently defined schema.
	 * 
	 * @param dataRow cells containing data
	 * @return an array of objects of primitive datatypes (boolean, int, byte, etc.)
	 *         containing the data of datarow, null if dataRow does not fit into
	 *         schema. Note: single elements can be null depending on the original
	 *         Excel
	 * 
	 */
	public Object[] getDataAccordingToSchema(SpreadSheetCellDAO[] dataRow) {
		if (dataRow == null) {
			return new Object[this.schemaRow.size()];
		}
		if (dataRow.length > this.schemaRow.size()) {
			LOG.warn("Data row is larger than schema. Will return String for everything that is not specified. ");
		}
		List<Object> returnList = new ArrayList<>();
		for (int i = 0; i < this.schemaRow.size(); i++) { // fill up with schema rows
			returnList.add(null);
		}
		for (int i = 0; i < dataRow.length; i++) {
			SpreadSheetCellDAO currentCell = dataRow[i];

			if (currentCell != null) {
				// determine real position
				int j = new CellAddress(currentCell.getAddress()).getColumn();
				if (j >= returnList.size()) {
					// fill up
					for (int x = returnList.size(); x <= j; x++) {
						returnList.add(null);
					}
				}
				GenericDataType applyDataType = null;
				if (j >= this.schemaRow.size()) {
					LOG.warn(
							"No further schema row for column defined: " + String.valueOf(j) + ". Will assume String.");
				} else {
					applyDataType = this.schemaRow.get(j);
				}
				if (applyDataType == null) {
					returnList.set(j, currentCell.getFormattedValue());
				} else if (applyDataType instanceof GenericStringDataType) {
					returnList.set(j, currentCell.getFormattedValue());
				} else if (applyDataType instanceof GenericBooleanDataType) {
					if (!"".equals(currentCell.getFormattedValue())) {
						if (currentCell.getFormattedValue().equalsIgnoreCase("true")
								|| currentCell.getFormattedValue().equalsIgnoreCase("false")) {
							returnList.set(j, Boolean.valueOf(currentCell.getFormattedValue()));
						}
					}
				} 
				else if (applyDataType instanceof GenericTimestampDataType) {
					if (!"".equals(currentCell.getFormattedValue())) {
						boolean timestampFound=false;
						if (this.dateTimeFormat!=null) { // check first dateTimeFormat
							Date theDate = this.dateTimeFormat.parse(currentCell.getFormattedValue(), new ParsePosition(0));
							if (theDate != null) {
								returnList.set(j, new java.sql.Timestamp(theDate.getTime()));
								timestampFound=true;
							} else {
								returnList.set(j, null);
								LOG.warn("Could not identify timestamp using Date.parse using provided dateTime format. Trying Timestamp.valueOf. Original value: "+currentCell.getFormattedValue());
							}
						} 
						if (!timestampFound) {
							try {
								returnList.set(j, java.sql.Timestamp.valueOf(currentCell.getFormattedValue()));
								timestampFound=true;
							} catch (IllegalArgumentException e) {
								returnList.set(j, null);
								LOG.warn("Could not identify timestamp using TimeStamp.valueOf. Trying last resort Date parsing. Original value: "+currentCell.getFormattedValue());
							}
						}
						if (!timestampFound) {
							Date theDate = this.dateFormat.parse(currentCell.getFormattedValue(), new ParsePosition(0));
							if (theDate != null) {
								returnList.set(j, new java.sql.Timestamp(theDate.getTime()));
	
							} else {
								returnList.set(j, null);
								LOG.warn("Could not identify timestamp using Date.parse using provided date format");
							}
						}
					
					}
				}
				else if (applyDataType instanceof GenericDateDataType) {
					if (!"".equals(currentCell.getFormattedValue())) {
						Date theDate = this.dateFormat.parse(currentCell.getFormattedValue(), new ParsePosition(0));
						
						if (theDate != null) {
							returnList.set(j, theDate);

						} else {
							returnList.set(j, null);
						}
					}
				} 

				else if (applyDataType instanceof GenericNumericDataType) {
					if (!"".equals(currentCell.getFormattedValue())) {
						BigDecimal bd = null;
						try {
							if (!"".equals(currentCell.getFormattedValue())) {
								// check scientific notation
								if (currentCell.getFormattedValue().toUpperCase().contains("E")) { // parse scientific notation
									// remove any characters that could cause issues
									String sanitizedCellContent = currentCell.getFormattedValue().replace(",", ".");
									bd = new BigDecimal(sanitizedCellContent);
								} else {
									bd = (BigDecimal) this.decimalFormat.parse(currentCell.getFormattedValue());
								}
							}
						} catch (ParseException p) {
							LOG.warn(
									"Could not parse decimal in spreadsheet cell, although type was detected as decimal");
						}
						if (bd != null) {
							BigDecimal bdv = bd.stripTrailingZeros();
							if (applyDataType instanceof GenericByteDataType) {
								returnList.set(j, (byte) bdv.byteValueExact());
							} else if (applyDataType instanceof GenericShortDataType) {
								returnList.set(j, (short) bdv.shortValueExact());
							} else if (applyDataType instanceof GenericIntegerDataType) {
								returnList.set(j, (int) bdv.intValueExact());
							} else if (applyDataType instanceof GenericLongDataType) {
								returnList.set(j, (long) bdv.longValueExact());
							} else if (applyDataType instanceof GenericDoubleDataType) {
								returnList.set(j, (double) bdv.doubleValue());
							} else if (applyDataType instanceof GenericFloatDataType) {
								returnList.set(j, (float) bdv.floatValue());
							} else if (applyDataType instanceof GenericBigDecimalDataType) {
								returnList.set(j, bd);
							} else {
								returnList.set(j, null);
							}
						}
					}
				} else {
					returnList.set(j, null);
					LOG.warn("Could not convert object in spreadsheet cellrow. Did you add a new datatype?");
				}
			}
		}
		Object[] result = new Object[returnList.size()];
		returnList.toArray(result);
		return result;
	}

	/***
	 * Converts a row consisting of objects of simple data types (String, byte,
	 * short, int, long, etc.) to a row of SpreadSheetCellDAO
	 * 
	 * @param row
	 * @param sheetName
	 * @param rowNum
	 * @return
	 */
	public SpreadSheetCellDAO[] getSpreadSheetCellDAOfromSimpleDataType(Object[] row, String sheetName, int rowNum) {
		// for each value in the row

		SpreadSheetCellDAO[] result = new SpreadSheetCellDAO[row.length];
		for (int currentColumnNum = 0; currentColumnNum < row.length; currentColumnNum++) { // for each element of the
																							// row
			Object x = row[currentColumnNum];
			String formattedValue = "";
			String comment = "";
			String formula = "";
			String address = "";
			StringBuffer sb = new StringBuffer();
			if (x != null) {
				if (x instanceof SpreadSheetCellDAO) {
					result[currentColumnNum] = (SpreadSheetCellDAO) x;
				} else {
					if (x instanceof Boolean) {
						formattedValue = "";
						comment = "";
						formula = String.valueOf(x);
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Byte) {
						formattedValue = "";
						comment = "";
						formula = String.valueOf(x);
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Short) {
						formattedValue = "";
						comment = "";
						formula = "";
						formula = String.valueOf(x);
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Integer) {
						formattedValue = "";
						comment = "";
						formula = String.valueOf(x);
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Long) {
						formattedValue = "";
						comment = "";
						formula = String.valueOf(x);
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Double) {
						formattedValue = "";
						comment = "";
						formula = String.valueOf(x);
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Float) {
						formattedValue = "";
						comment = "";
						formula = String.valueOf(x);
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof BigDecimal) {
						formattedValue = "";
						comment = "";
						formula = ((BigDecimal) x).toString();
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Timestamp) {
						if (this.dateTimeFormat==null) {
							formattedValue = String.valueOf(x);
						} else {
							formattedValue=this.dateTimeFormat.format(x,sb,new FieldPosition(0)).toString();
							sb.setLength(0);
						}
						comment = "";
						formula = "";
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof Date) {
						formattedValue = this.dateFormat.format((Date) x);
						comment = "";
						formula = "";
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if (x instanceof String) {
						formattedValue = x.toString();
						comment = "";
						formula = "";
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					} else if ((x instanceof String[]) && ((String[]) x).length == 5) {
						String[] spreadSheetCellDAOAsArray = (String[]) x;
						formattedValue = spreadSheetCellDAOAsArray[0];
						comment = spreadSheetCellDAOAsArray[1];
						formula = spreadSheetCellDAOAsArray[2];
						address = spreadSheetCellDAOAsArray[3];
						sheetName = spreadSheetCellDAOAsArray[4];
					} else {
						LOG.warn("Unknown datatype in column number: " + currentColumnNum + ". Reported data type"
								+ x.getClass().getName() + ". Trying to use .toString");
						formattedValue = x.toString();
						comment = "";
						formula = "";
						address = MSExcelUtil.getCellAddressA1Format(rowNum, currentColumnNum);
					}
					result[currentColumnNum] = new SpreadSheetCellDAO(formattedValue, comment, formula, address,
							sheetName);
				}
			}
		}
		return result;
	}

}
