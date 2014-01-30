package com.github.pgasync.impl.message;

import static com.github.pgasync.impl.io.IO.bytes;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PasswordMessage implements Message {

	final String password;
	final byte[] passwordHash;

	public PasswordMessage(String username, String password, byte[] md5salt) {
		this.password = password;
		this.passwordHash = md5salt != null ? md5(username, password, md5salt) : null;
	}

	public byte[] getPasswordHash() {
		return passwordHash;
	}
	public String getPassword() {
		return password;
	}

	static byte[] md5(String username, String password, byte[] md5salt) {
		MessageDigest md5 = digest();
		md5.update(bytes(password));
		md5.update(bytes(username));
		byte[] hash = bytes(printHexBinary(md5.digest()).toLowerCase());

		md5.update(hash);
		md5.update(md5salt);
		hash = bytes(printHexBinary(md5.digest()).toLowerCase());

		byte[] prefixed = new byte[hash.length + 3];
		prefixed[0] = (byte) 'm';
		prefixed[1] = (byte) 'd';
		prefixed[2] = (byte) '5';
		System.arraycopy(hash, 0, prefixed, 3, hash.length);
		return prefixed;
	}

	static MessageDigest digest() {
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
