package com.github.pgasync.impl;

import static com.github.pgasync.impl.io.IO.bytes;
import static javax.xml.bind.DatatypeConverter.parseHexBinary;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.io.IO;

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

public enum TypeConverter {

	;

	static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd");
		};
	};
	static final ThreadLocal<DateFormat> TIME_FORMAT = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("HH:mm:ss.SSS");
		};
	};
	static final ThreadLocal<DateFormat> TIME_FORMAT_NO_MILLIS = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("HH:mm:ss");
		};
	};

	public static byte[] toParam(Object o) {
		if(o == null) {
			return null;
		}
		if(o instanceof Time) {
			return bytes(TIME_FORMAT.get().format((Time) o));
		}
		if(o instanceof Date) {
			return bytes(DATE_FORMAT.get().format((Date) o));
		}
		if(o instanceof byte[]) {
			return bytes("\\x" + printHexBinary((byte[]) o));
		}
		return bytes(o.toString());
	}

	static String toString(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case TEXT: // fallthrough
		case CHAR: // fallthrough
		case BPCHAR: // fallthrough
		case VARCHAR:
			return new String(value, IO.UTF8);
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> String");
		}
	}

	static Character toChar(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case CHAR: // fallthrough
		case BPCHAR:
			return IO.UTF8.decode(ByteBuffer.wrap(value)).charAt(0);
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> String");
		}
	}
	
	static Long toLong(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case INT2: // fallthrough
		case INT4: // fallthrough
		case INT8:
			return Long.valueOf(new String(value, IO.UTF8));
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> Long");
		}
	}

	static Integer toInteger(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case INT2: // fallthrough
		case INT4:
			return Integer.valueOf(new String(value, IO.UTF8));
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> Integer");
		}
	}

	static Short toShort(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case INT2:
			return Short.valueOf(new String(value, IO.UTF8));
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> Short");
		}
	}

	static Byte toByte(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case INT2:
			return Byte.valueOf(new String(value, IO.UTF8));
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> Byte");
		}
	}

	static BigInteger toBigInteger(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case INT2: // fallthrough
		case INT4: // fallthrough
		case INT8:
			return new BigInteger(new String(value, IO.UTF8));
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> BigInteger");
		}
	}

	static BigDecimal toBigDecimal(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case FLOAT4: // fallthrough
		case FLOAT8:
			return new BigDecimal(new String(value, IO.UTF8));
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> BigDecimal");
		}
	}

	static Date toDate(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case DATE:
			String date = new String(value, IO.UTF8);
			try {
				return new Date(DATE_FORMAT.get().parse(date).getTime());
			} catch(ParseException e) {
				throw new SqlException("Invalid date: " + date);
			}
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> Date");
		}
	}

	static Time toTime(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case TIMETZ:
		case TIME:
			String time = new String(value, IO.UTF8);
			try {
				DateFormat format = time.length() == 8 ? TIME_FORMAT_NO_MILLIS.get() : TIME_FORMAT.get();
				return new Time(format.parse(time).getTime());
			} catch(ParseException e) {
				throw new SqlException("Invalid time: " + time);
			}
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> Time");
		}
	}

	static byte[] toBytes(Oid oid, byte[] value) {
		if(value == null) {
			return null;
		}
		switch (oid) {
		case UNSPECIFIED: // fallthrough
		case BYTEA:
			return parseHexBinary(new String(value, IO.UTF8).substring(2));
		default:
			throw new SqlException("Unsupported conversion " + oid.name() + " -> byte[]");
		}
	}

}
