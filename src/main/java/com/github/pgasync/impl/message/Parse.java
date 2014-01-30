package com.github.pgasync.impl.message;

public class Parse implements Message {

	final String sql;

	public Parse(String sql) {
		this.sql = sql;
	}
	public String getQuery() {
		return sql;
	}
}
