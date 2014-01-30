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

import com.github.pgasync.impl.Oid;
import com.github.pgasync.impl.message.RowDescription;
import com.github.pgasync.impl.message.RowDescription.ColumnDescription;

public class RowDescriptionDecoder implements Decoder<RowDescription> {

	@Override
	public byte getMessageId() {
		return (byte) 'T';
	}

	@Override
	public RowDescription read(ByteBuffer buffer) {
		byte[] bytes = new byte[255];
		ColumnDescription[] columns = new ColumnDescription[buffer.getShort()];

		for(int i = 0; i < columns.length; i++) {
			String name = getCString(buffer, bytes);
			buffer.position(buffer.position() + 6);
			Oid type = Oid.valueOfId(buffer.getInt());
			buffer.position(buffer.position() + 8);
			columns[i] = new ColumnDescription(name, type);
		}

		return new RowDescription(columns);
	}

}
