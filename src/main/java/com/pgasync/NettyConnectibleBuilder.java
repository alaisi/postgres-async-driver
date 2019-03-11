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

package com.pgasync;

import com.github.pgasync.PgConnectionPool;
import com.github.pgasync.PgDatabase;
import com.github.pgasync.ProtocolStream;
import com.github.pgasync.conversion.DataConverter;
import com.github.pgasync.netty.NettyProtocolStream;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Builder for creating {@link Connectible} instances.
 *
 * @author Antti Laisi
 * @author Marat Gainullin
 */
public class NettyConnectibleBuilder {

    private final ConnectibleProperties properties = new ConnectibleProperties();
    // TODO: refactor when Netty will support more advanced threading model
    //new NioEventLoopGroup(0/*Netty defaults will be used*/, futuresExecutor),
    private static final EventLoopGroup group = new NioEventLoopGroup();

    private ProtocolStream newProtocolStream(Executor futuresExecutor) {
        return new NettyProtocolStream(
                group,
                new InetSocketAddress(properties.getHostname(), properties.getPort()),
                properties.getUseSsl(),
                Charset.forName(properties.getEncoding()),
                futuresExecutor
        );
    }

    /**
     * @return Pool ready for use
     */
    public Connectible pool(Executor futuresExecutor) {
        return new PgConnectionPool(properties, this::newProtocolStream, futuresExecutor);
    }

    public Connectible pool() {
        return pool(ForkJoinPool.commonPool());
    }


    /**
     * @return Pool ready for use
     */
    public Connectible plain(Executor futuresExecutor) {
        return new PgDatabase(properties, this::newProtocolStream, futuresExecutor);
    }

    public Connectible plain() {
        return plain(ForkJoinPool.commonPool());
    }

    public NettyConnectibleBuilder hostname(String hostname) {
        properties.hostname = hostname;
        return this;
    }

    public NettyConnectibleBuilder port(int port) {
        properties.port = port;
        return this;
    }

    public NettyConnectibleBuilder username(String username) {
        properties.username = username;
        return this;
    }

    public NettyConnectibleBuilder password(String password) {
        properties.password = password;
        return this;
    }

    public NettyConnectibleBuilder database(String database) {
        properties.database = database;
        return this;
    }

    public NettyConnectibleBuilder maxConnections(int maxConnections) {
        properties.maxConnections = maxConnections;
        return this;
    }

    public NettyConnectibleBuilder maxStatements(int maxStatements) {
        properties.maxStatements = maxStatements;
        return this;
    }

    public NettyConnectibleBuilder converters(Converter<?>... converters) {
        Collections.addAll(properties.converters, converters);
        return this;
    }

    public NettyConnectibleBuilder dataConverter(DataConverter dataConverter) {
        properties.dataConverter = dataConverter;
        return this;
    }

    public NettyConnectibleBuilder ssl(boolean ssl) {
        properties.useSsl = ssl;
        return this;
    }

    public NettyConnectibleBuilder validationQuery(String validationQuery) {
        properties.validationQuery = validationQuery;
        return this;
    }

    public NettyConnectibleBuilder encoding(String value) {
        properties.encoding = value;
        return this;
    }

    /**
     * Configuration for a connectible.
     */
    public static class ConnectibleProperties {

        String hostname = "localhost";
        int port = 5432;
        String username;
        String password;
        String database;
        int maxConnections = 20;
        int maxStatements = 20;
        DataConverter dataConverter;
        List<Converter<?>> converters = new ArrayList<>();
        boolean useSsl;
        String encoding = System.getProperty("pg.async.encoding", "utf-8");
        String validationQuery;

        public String getHostname() {
            return hostname;
        }

        public int getPort() {
            return port;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDatabase() {
            return database;
        }

        public int getMaxConnections() {
            return maxConnections;
        }

        public int getMaxStatements() {
            return maxStatements;
        }

        public boolean getUseSsl() {
            return useSsl;
        }

        public String getEncoding() {
            return encoding;
        }

        public DataConverter getDataConverter() {
            return dataConverter != null ? dataConverter : new DataConverter(converters, Charset.forName(encoding));
        }

        public String getValidationQuery() {
            return validationQuery;
        }
    }
}