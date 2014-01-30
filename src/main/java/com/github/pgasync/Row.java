/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;


public interface Row {

	String getString(int index);
	String getString(String column);

	Byte getByte(int index);
	Byte getByte(String column);

	Character getChar(int index);
	Character getChar(String column);

	Short getShort(int index);
	Short getShort(String column);

	Integer getInt(int index);
	Integer getInt(String column);

	Long getLong(int index);
	Long getLong(String column);

	BigInteger getBigInteger(int index);
	BigInteger getBigInteger(String column);

	BigDecimal getBigDecimal(int index);
	BigDecimal getBigDecimal(String column);
	
	Date getDate(int index);
	Date getDate(String column);

	Time getTime(int index);
	Time getTime(String column);

	byte[] getBytes(int index);
	byte[] getBytes(String column);

}
