package com.github.pgasync.impl.message.backend;

import com.github.pgasync.impl.message.Message;

/**
 * @author Marat Gainullin
 */
public enum BIndicators implements Message {
    PARSE_COMPLETE, CLOSE_COMPLETE, BIND_COMPLETE
}
