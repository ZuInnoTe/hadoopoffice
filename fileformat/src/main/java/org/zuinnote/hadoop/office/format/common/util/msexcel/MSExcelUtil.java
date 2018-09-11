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

package org.zuinnote.hadoop.office.format.common.util.msexcel;

import org.apache.poi.ss.util.CellAddress;

public class MSExcelUtil {

private MSExcelUtil() {
}

/**
* Generate a cell address in A1 format from a row and column number
*
* @param rowNum row number
* @param columnNum column number
*
* @return Address in A1 format
*
*/

public static String getCellAddressA1Format(int rowNum, int columnNum) {
 CellAddress cellAddr = new CellAddress(rowNum, columnNum);
 return cellAddr.formatAsString();
}


}
