package com.github.pgasync.impl.message;

public class Query implements Message {

	final String sql;

	public Query(String sql) {
		this.sql = sql;
	}
	public String getQuery() {
		return sql;
	}
}
