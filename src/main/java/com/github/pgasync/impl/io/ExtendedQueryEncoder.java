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

import java.nio.ByteBuffer;

import com.github.pgasync.impl.message.ExtendedQuery;

public class ExtendedQueryEncoder implements Encoder<ExtendedQuery> {

	@Override
	public Class<ExtendedQuery> getMessageType() {
		return ExtendedQuery.class;
	}

	@Override
	public void write(ExtendedQuery msg, ByteBuffer buffer) {
		switch (msg) {
		case DESCRIBE:
			describe(buffer); break;
		case EXECUTE:
			execute(buffer); break;
		case SYNC:
			sync(buffer); break;
		default:
			throw new IllegalStateException(msg.name());
		}
	}

	void describe(ByteBuffer buffer) {
		buffer.put((byte) 'D');
		buffer.putInt(0);
		buffer.put((byte) 'S');
		buffer.put((byte) 0);
		buffer.putInt(1, buffer.position() - 1);
	}

	void execute(ByteBuffer buffer) {
		buffer.put((byte) 'E');
		buffer.putInt(0);
		buffer.put((byte) 0);
		buffer.putInt(0);
		buffer.putInt(1, buffer.position() - 1);
	}

	void sync(ByteBuffer buffer) {
		buffer.put((byte) 'S');
		buffer.putInt(4);
	}

}
