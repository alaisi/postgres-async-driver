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

import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import com.github.pgasync.impl.message.RowDescription.ColumnDescription;

import java.util.*;

/**
 * {@link ResultSet} constructed from Query/Execute response messages.
 * 
 * @author Antti Laisi
 */
public class PgResultSet implements ResultSet {

    final List<Row> rows;
    final Map<String, PgColumn> columns;
    final int updatedRows;

    public PgResultSet(Map<String, PgColumn> columns, List<Row> rows, int updatedRows) {
        this.columns = columns;
        this.rows = rows;
        this.updatedRows = updatedRows;
    }

    @Override
    public Collection<String> getColumns() {
        return columns != null ? columns.keySet() : Collections.emptyList();
    }

    @Override
    public Iterator<Row> iterator() {
        return rows != null ? rows.iterator() : Collections.<Row> emptyIterator();
    }

    @Override
    public Row row(int index) {
        if (rows == null) {
            throw new IndexOutOfBoundsException();
        }
        return rows.get(index);
    }

    @Override
    public int size() {
        return rows != null ? rows.size() : 0;
    }

    @Override
    public int updatedRows() {
        return updatedRows;
    }

}
