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

import com.pgasync.Connectible;
import com.pgasync.Connection;
import com.pgasync.SqlException;
import org.junit.Rule;
import org.junit.Test;

import java.util.function.Consumer;

/**
 * Tests for validated connections.
 *
 * @author Marat Gainullin
 */
public class ValidatedConnectionTest {

    @Rule
    public final DatabaseRule dbr = new DatabaseRule();

    private void withSource(Connectible source, Consumer<Connectible> action) throws Exception {
        try {
            action.accept(source);
        } catch (Exception ex) {
            SqlException.ifCause(ex,
                    sqlException -> {
                        throw sqlException;
                    },
                    () -> {
                        throw ex;
                    });
        } finally {
            source.close().join();
        }
    }

    private void withPlain(String clause, Consumer<Connectible> action) throws Exception {
        withSource(dbr.builder
                        .validationQuery(clause)
                        .plain(),
                action
        );
    }

    private void withPool(String clause, Consumer<Connectible> action) throws Exception {
        withSource(dbr.builder
                        .validationQuery(clause)
                        .pool(),
                action
        );
    }

    @Test
    public void shouldReturnValidPlainConnection() throws Exception {
        withPlain("Select 89", plain -> {
            Connection conn = plain.getConnection().join();
            conn.close().join();
        });
    }

    @Test(expected = SqlException.class)
    public void shouldNotReturnInvalidPlainConnection() throws Exception {
        withPlain("Selec t 89", plain -> plain.getConnection().join());
    }

    @Test
    public void shouldReturnValidPooledConnection() throws Exception {
        withPool("Select 89", source -> {
            Connection conn = source.getConnection().join();
            conn.close().join();
        });
    }

    @Test(expected = SqlException.class)
    public void shouldNotReturnInvalidPooledConnection() throws Exception {
        withPool("Selec t 89", source -> source.getConnection().join());
    }
}
