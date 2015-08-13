package com.github.ddth.com.redir.internal;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.github.ddth.com.redir.RedisDirectory;

/**
 * Lock factory to be used with {@link RedisDirectory}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class RedisLockFactory extends LockFactory {

    public final static RedisLockFactory INSTANCE = new RedisLockFactory();

    private RedisLockFactory() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Lock makeLock(Directory dir, String lockName) {
        if (!(dir instanceof RedisDirectory)) {
            throw new IllegalArgumentException("Expect argument of type ["
                    + RedisDirectory.class.getName() + "]!");
        }
        RedisDirectory reDir = (RedisDirectory) dir;
        return reDir.createLock(lockName);
    }

}
