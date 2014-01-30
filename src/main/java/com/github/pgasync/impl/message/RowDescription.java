package com.github.pgasync.impl.message;

import com.github.pgasync.impl.Oid;

public class RowDescription implements Message {

	public static class ColumnDescription {

		final String name;
		final Oid type;

		public ColumnDescription(String name, Oid type) {
			this.name = name;
			this.type = type;
		}
		public String getName() {
			return name;
		}
		public Oid getType() {
			return type;
		}
	}

	final ColumnDescription[] columns;

	public RowDescription(ColumnDescription[] columns) {
		this.columns = columns;
	}
	public ColumnDescription[] getColumns() {
		return columns;
	}
}
