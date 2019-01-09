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

package com.github.pgasync.impl;

import com.github.pgasync.Row;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.backend.DataRow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Result row, uses {@link DataConverter} for all conversions.
 *
 * @author Antti Laisi
 */
public class PgRow implements Row {

    final DataRow data;
    final DataConverter dataConverter;
    final Map<String, PgColumn> columns;
    final PgColumn[] pgColumns;

    public PgRow(DataRow data, Map<String, PgColumn> columns, DataConverter dataConverter) {
        this.data = data;
        this.dataConverter = dataConverter;
        this.columns = columns;
        this.pgColumns = columns.values().toArray(new PgColumn[]{});
    }

    @Override
    public String getString(int index) {
        return dataConverter.toString(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public String getString(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toString(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Character getChar(int index) {
        return dataConverter.toChar(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Character getChar(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toChar(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Byte getByte(int index) {
        return dataConverter.toByte(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Byte getByte(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toByte(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Short getShort(int index) {
        return dataConverter.toShort(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Short getShort(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toShort(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Integer getInt(int index) {
        return dataConverter.toInteger(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Integer getInt(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toInteger(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Long getLong(int index) {
        return dataConverter.toLong(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Long getLong(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toLong(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return dataConverter.toBigInteger(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public BigInteger getBigInteger(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toBigInteger(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return dataConverter.toBigDecimal(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public BigDecimal getBigDecimal(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toBigDecimal(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Double getDouble(int index) {
        return dataConverter.toDouble(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Double getDouble(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toDouble(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Date getDate(int index) {
        return dataConverter.toDate(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Date getDate(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toDate(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Time getTime(int index) {
        return dataConverter.toTime(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Time getTime(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toTime(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Timestamp getTimestamp(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toTimestamp(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Timestamp getTimestamp(int index) {
        return dataConverter.toTimestamp(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public byte[] getBytes(int index) {
        return dataConverter.toBytes(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public byte[] getBytes(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toBytes(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public Boolean getBoolean(int index) {
        return dataConverter.toBoolean(pgColumns[index].type, data.getValue(index));
    }

    @Override
    public Boolean getBoolean(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toBoolean(pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public <TArray> TArray getArray(String column, Class<TArray> arrayType) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toArray(arrayType, pgColumn.type, data.getValue(pgColumn.index));
    }

    @Override
    public <TArray> TArray getArray(int index, Class<TArray> arrayType) {
        return dataConverter.toArray(arrayType, pgColumns[index].type, data.getValue(index));
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        return dataConverter.toObject(type, pgColumns[index].type, data.getValue(index));
    }

    @Override
    public <T> T get(String column, Class<T> type) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toObject(type, pgColumn.type, data.getValue(pgColumn.index));
    }

    public Object get(String column) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toObject(pgColumn.type, data.getValue(pgColumn.index));
    }

    public Map<String, PgColumn> getColumns() {
        return columns;
    }

    PgColumn getColumn(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Column name is required");
        }
        PgColumn column = columns.get(name.toUpperCase());
        if (column == null) {
            throw new SqlException("Unknown column '" + name + "'");
        }
        return column;
    }

}
