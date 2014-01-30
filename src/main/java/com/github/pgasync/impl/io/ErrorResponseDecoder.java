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

package com.github.pgasync.impl.io;

import static com.github.pgasync.impl.io.IO.getCString;

import java.nio.ByteBuffer;

import com.github.pgasync.impl.message.ErrorResponse;
import com.github.pgasync.impl.message.ErrorResponse.Level;

public class ErrorResponseDecoder implements Decoder<ErrorResponse> {

	@Override
	public byte getMessageId() {
		return (byte) 'E';
	}

	@Override
	public ErrorResponse read(ByteBuffer buffer) {
		ErrorResponse error = new ErrorResponse();
		byte[] field = new byte[255];
		for(byte type = buffer.get(); type != 0; type = buffer.get()) {
			String value = getCString(buffer, field);
			if(type == (byte) 'S') {
				error.setLevel(Level.valueOf(value));
			} else if(type == 'C') {
				error.setCode(value);
			} else if(type == 'M') {
				error.setMessage(value);
			}
		}
		return error;
	}

}
