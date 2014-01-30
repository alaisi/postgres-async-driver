package com.github.pgasync.impl.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.InetSocketAddress;

import com.github.pgasync.impl.PgConnection;
import com.github.pgasync.impl.PgConnectionPool;

public class NettyPgConnectionPool extends PgConnectionPool {

	final EventLoopGroup group = new NioEventLoopGroup(1);

	public NettyPgConnectionPool(InetSocketAddress address, String username, String password, String database, int poolSize) {
		super(address, username, password, database, poolSize);
	}

	@Override
	protected PgConnection newConnection(InetSocketAddress address) {
		return new PgConnection(new NettyPgProtocolStream(address, group));
	}

	@Override
	public void close() {
		super.close();
		group.shutdownGracefully();
	}
}
