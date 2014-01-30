package com.github.pgasync.impl.message;

import java.util.List;

import com.github.pgasync.impl.TypeConverter;

public class Bind implements Message {

	final byte[][] params;

	public Bind(List<Object> parameters) {
		this.params = new byte[parameters.size()][];
		int i = 0;
		for(Object param : parameters) {
			params[i++] = TypeConverter.toParam(param);
		}
	}

	public byte[][] getParams() {
		return params;
	}
}
