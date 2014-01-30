package com.github.pgasync.impl.message;

public class DataRow implements Message {

	final byte[][] values;

	public DataRow(byte[][] values) {
		this.values = values;
	}
	public byte[] getValue(int i) {
		return values[i];
	}
}
