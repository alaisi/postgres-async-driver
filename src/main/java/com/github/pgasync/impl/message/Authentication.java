package com.github.pgasync.impl.message;

/**
 * AuthenticationOk
 * AuthenticationMD5Password
 * AuthenticationCleartextPassword
 */
public class Authentication implements Message {

	byte[] md5salt;
	boolean success;

	public void setAuthenticationOk() {
		success = true;
	}
	public void setMd5Salt(byte[] md5salt) {
		this.md5salt = md5salt;
	}
	public byte[] getMd5Salt() {
		return md5salt;
	}
	public boolean isAuthenticationOk() {
		return success;
	}
}
