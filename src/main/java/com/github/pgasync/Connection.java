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

package com.github.pgasync;

import java.util.List;

import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.callback.ResultHandler;
import com.github.pgasync.callback.TransactionHandler;

/**
 * Main interface to a PostgreSQL backend.
 * 
 * @author Antti Laisi
 */
@SuppressWarnings("rawtypes")
public interface Connection {

    /**
     * Executes a simple query.
     * 
     * @param sql SQL to execute.
     * @param onResult Called when query is completed
     * @param onError Called on exception thrown
     */
	void query(String sql, ResultHandler onResult, ErrorHandler onError);

	/**
	 * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
	 * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
	 * and byte[].
	 * 
	 * @param sql SQL to execute
	 * @param params List of parameters
	 * @param onResult
	 * @param onError
	 */
	void query(String sql, List/*<Object>*/ params, ResultHandler onResult, ErrorHandler onError);

	/**
	 * Begins a transaction.
	 * 
	 * @param onTransaction Called when transaction is successfully started.
	 * @param onError Called on exception thrown
	 */
	void begin(TransactionHandler onTransaction, ErrorHandler onError);

	/**
	 * Closes the connection.
	 */
	void close();

}
