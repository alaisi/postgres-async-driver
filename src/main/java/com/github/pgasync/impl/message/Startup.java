package com.github.pgasync.impl.message;

public class Startup implements Message {

	final int protocol = 196608;
	final String username;
	final String database;

	public Startup(String username, String database) {
		this.username = username;
		this.database = database;
	}

	public int getProtocol() {
		return protocol;
	}
	public String getUsername() {
		return username;
	}
	public String getDatabase() {
		return database;
	}
}
