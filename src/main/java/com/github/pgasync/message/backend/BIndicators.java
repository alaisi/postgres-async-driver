package com.github.pgasync.message.backend;

import com.github.pgasync.message.Message;

/**
 * @author Marat Gainullin
 */
public enum BIndicators implements Message {
    PARSE_COMPLETE, CLOSE_COMPLETE, BIND_COMPLETE, NO_DATA
}
