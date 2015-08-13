package com.github.ddth.com.redir.qnd;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.github.ddth.com.redir.RedisDirectory;
import com.github.ddth.com.redir.RedisDirectory.RedisIndexInput;
import com.github.ddth.com.redir.RedisDirectory.RedisIndexOutput;

public class BaseQndRedisDir {

    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_PASSWORD = null;

    public static void initLoggers(Level level) {
        {
            Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            logger.setLevel(Level.ERROR);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(RedisDirectory.class);
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(RedisIndexInput.class);
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(RedisIndexOutput.class);
            logger.setLevel(level);
        }
    }

}
