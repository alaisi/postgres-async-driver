package com.github.pgasync.impl.message;

public class ErrorResponse implements Message {

	public enum Level {
		ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG;
	}

	Level level;
	String code;
	String message;

	public void setLevel(Level level) {
		this.level = level;
	}
	public Level getLevel() {
		return level;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
}
