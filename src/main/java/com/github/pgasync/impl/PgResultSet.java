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

    List<Row> rows;
    Map<String, PgColumn> columns;
    int updatedRows;

    public PgResultSet() {
    }

    public PgResultSet(ColumnDescription[] columnDescriptions) {
        columns = new LinkedHashMap<>();
        for (int i = 0; i < columnDescriptions.length; i++) {
            columns.put(columnDescriptions[i].getName().toUpperCase(),
                    new PgColumn(i, columnDescriptions[i].getType()));
        }
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
    public Row get(int index) {
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

    void setUpdatedRows(int updatedRows) {
        this.updatedRows = updatedRows;
    }

    void add(PgRow row) {
        if (rows == null) {
            rows = new ArrayList<>();
        }
        row.setColumns(columns);
        rows.add(row);
    }

}
