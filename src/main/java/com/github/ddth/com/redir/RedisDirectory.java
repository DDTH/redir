package com.github.ddth.com.redir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.github.ddth.com.redir.internal.RedisLockFactory;

/**
 * Redis implementation of {@link Directory}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class RedisDirectory extends BaseDirectory {

    public final static int BLOCK_SIZE = 64 * 1024; // 64Kb

    private Logger LOGGER = LoggerFactory.getLogger(RedisDirectory.class);

    public final static String DEFAULT_HASH_FILE_DATA = "file_data";
    public final static String DEFAULT_HASH_DIRECTORY_METADATA = "directory_metadata";

    private byte[] hashFileData = DEFAULT_HASH_FILE_DATA.getBytes();
    private byte[] hashDirectoryMetadata = DEFAULT_HASH_DIRECTORY_METADATA.getBytes();

    private String redisHost = "localhost";
    private int redisPort = 6379;
    private String redisPassword;
    private JedisPool jedisPool;
    private boolean myOwnPool = false;

    /*----------------------------------------------------------------------*/
    public RedisDirectory(String redisHost, int redisPort, String redisPassword) {
        super(RedisLockFactory.INSTANCE);
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        init();
    }

    /**
     * Name of Redis hash to store file data.
     * 
     * @return
     */
    public String getHashFileData() {
        return new String(hashFileData);
    }

    public RedisDirectory setHashFileData(String hashFileData) {
        this.hashFileData = hashFileData.getBytes();
        return this;
    }

    /**
     * Name of Redis hash to store file metadata.
     * 
     * @return
     */
    public String getHashDirectoryMetadata() {
        return new String(hashDirectoryMetadata);
    }

    public RedisDirectory setHashDirectoryMetadata(String hashDirectoryMetadata) {
        this.hashDirectoryMetadata = hashDirectoryMetadata.getBytes();
        return this;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public RedisDirectory setRedisHost(String redisHost) {
        this.redisHost = redisHost;
        return this;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public RedisDirectory setRedisPort(int redisPort) {
        this.redisPort = redisPort;
        return this;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public RedisDirectory setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
        return this;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public RedisDirectory setJedisPool(JedisPool jedisPool) {
        if (this.jedisPool == null || this.jedisPool == jedisPool) {
            myOwnPool = false;
            this.jedisPool = jedisPool;
        } else {
            throw new IllegalStateException("My own pool has been initialized!");
        }
        return this;
    }

    /*----------------------------------------------------------------------*/
    private String keyDataBlock(FileInfo fileInfo, int blockNum) {
        return fileInfo.id() + ":" + blockNum;
    }

    private String keyFileInfo(FileInfo fileInfo) {
        return fileInfo.name();
    }

    private String keyFileInfo(String fileName) {
        return fileName;
    }

    /*----------------------------------------------------------------------*/
    public void init() {
        if (jedisPool == null) {
            myOwnPool = true;
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(Math.min(Runtime.getRuntime().availableProcessors(), 8));
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 10000, redisPassword);
        }
    }

    public void destroy() {
        if (myOwnPool && jedisPool != null) {
            jedisPool.destroy();
        }
    }

    private Jedis getJedis() {
        return jedisPool.getResource();
    }

    /**
     * Loads a file's block data from storage.
     * 
     * @param fileInfo
     * @param blockNum
     * @return {@code null} if file and/or block does not exist, otherwise a
     *         {@code byte[]} with minimum {@link #BLOCK_SIZE} length is
     *         returned
     */
    private byte[] readFileBlock(FileInfo fileInfo, int blockNum) {
        final String KEY = keyDataBlock(fileInfo, blockNum);
        try (Jedis jedis = getJedis()) {
            byte[] dataArr = jedis.hget(hashFileData, KEY.getBytes());
            return dataArr != null ? (dataArr.length >= BLOCK_SIZE ? dataArr : Arrays.copyOf(
                    dataArr, BLOCK_SIZE)) : null;
        }
    }

    /**
     * Write a file's block data to storage.
     * 
     * @param fileInfo
     * @param blockNum
     * @param data
     */
    private void writeFileBlock(FileInfo fileInfo, int blockNum, byte[] data) {
        final String KEY = keyDataBlock(fileInfo, blockNum);
        try (Jedis jedis = getJedis()) {
            jedis.hset(hashFileData, KEY.getBytes(), data);
        }
    }

    /**
     * Gets a file's metadata info.
     * 
     * @param filename
     * @return
     */
    private FileInfo getFileInfo(String filename) {
        final String KEY = keyFileInfo(filename);
        try (Jedis jedis = getJedis()) {
            byte[] dataArr = jedis.hget(hashDirectoryMetadata, KEY.getBytes());
            return FileInfo.newInstance(dataArr);
        }
    }

    private FileInfo[] getAllFileInfo() {
        try (Jedis jedis = getJedis()) {
            List<FileInfo> result = new ArrayList<FileInfo>();
            Map<byte[], byte[]> allFileMap = jedis.hgetAll(hashDirectoryMetadata);
            if (allFileMap != null) {
                for (Entry<byte[], byte[]> entry : allFileMap.entrySet()) {
                    byte[] data = entry.getValue();
                    FileInfo fileInfo = FileInfo.newInstance(data);
                    if (fileInfo != null) {
                        result.add(fileInfo);
                    }
                }
            }
            return result != null ? result.toArray(FileInfo.EMPTY_ARRAY) : FileInfo.EMPTY_ARRAY;
        }
    }

    /**
     * Updates a file's metadata.
     * 
     * @param fileInfo
     * @return
     */
    private FileInfo updateFileInfo(FileInfo fileInfo) {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "updateFile(" + fileInfo.name() + "/" + fileInfo.id() + "/"
                    + fileInfo.size() + ") is called";
            LOGGER.trace(logMsg);
        }
        final String KEY = keyFileInfo(fileInfo);
        try (Jedis jedis = getJedis()) {
            jedis.hset(hashDirectoryMetadata, KEY.getBytes(), fileInfo.asBytes());
            return fileInfo;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        destroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexOutput createOutput(String name, IOContext ioContext) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            fileInfo = FileInfo.newInstance(name);
            updateFileInfo(fileInfo);
        }
        return new RedisIndexOutput(fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexInput openInput(String name, IOContext ioContext) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new IOException("File [" + name + "] not found!");
        }
        return new RedisIndexInput(this, fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteFile(String name) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo != null) {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "deleteFile(" + name + "/" + fileInfo.id() + ") is called";
                LOGGER.trace(logMsg);
            }
            try (Jedis jedis = getJedis()) {
                final String KEY_METADATA = keyFileInfo(fileInfo);
                jedis.hdel(hashDirectoryMetadata, KEY_METADATA.getBytes());

                long size = fileInfo.size();
                long numBlocks = (size / BLOCK_SIZE) + (size % BLOCK_SIZE != 0 ? 1 : 0);
                for (int i = 0; i < numBlocks; i++) {
                    final String KEY_DATABLOCK = keyDataBlock(fileInfo, i);
                    jedis.hdel(hashFileData, KEY_DATABLOCK.getBytes());
                }
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "deleteFile(" + name + ") is called, but file is not found";
                LOGGER.trace(logMsg);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long fileLength(String name) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new IOException("File [" + name + "] not found!");
        }
        return fileInfo.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] listAll() throws IOException {
        List<String> result = new ArrayList<String>();
        for (FileInfo fileInfo : getAllFileInfo()) {
            result.add(fileInfo.name());
        }
        return result.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void renameFile(String oldName, String newName) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            final String logMsg = "rename(" + oldName + "," + newName + ") is called";
            LOGGER.trace(logMsg);
        }

        FileInfo fileInfo = getFileInfo(oldName);
        if (fileInfo == null) {
            throw new IOException("File [" + oldName + "] not found!");
        }
        updateFileInfo(fileInfo.name(newName));
        final String KEY = keyFileInfo(oldName);
        try (Jedis jedis = getJedis()) {
            jedis.hdel(hashDirectoryMetadata, KEY.getBytes());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            final String logMsg = "sync(" + names + ") is called";
            LOGGER.trace(logMsg);
        }
    }

    /*----------------------------------------------------------------------*/
    public RedisLock createLock(String lockName) {
        return new RedisLock(lockName);
    }

    /**
     * Redis implementation of {@link Lock}.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.1.0
     */
    private class RedisLock extends Lock {

        private FileInfo fileInfo;

        public RedisLock(String fileName) {
            fileInfo = FileInfo.newInstance(fileName);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean obtain() throws IOException {
            final String KEY = keyFileInfo(fileInfo);
            try (Jedis jedis = getJedis()) {
                Long result = jedis.hincrBy(hashDirectoryMetadata, KEY.getBytes(), 1);
                return result != null && result.longValue() == 1;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            final String KEY = keyFileInfo(fileInfo);
            try (Jedis jedis = getJedis()) {
                jedis.hdel(hashDirectoryMetadata, KEY.getBytes());
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isLocked() throws IOException {
            final String KEY = keyFileInfo(fileInfo);
            try (Jedis jedis = getJedis()) {
                return jedis.hget(hashDirectoryMetadata, KEY.getBytes()) != null;
            }
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * Redis implementation of {@link IndexOutput}.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.1.0
     */
    public class RedisIndexOutput extends IndexOutput {

        private final Logger LOGGER = LoggerFactory.getLogger(RedisIndexOutput.class);

        private CRC32 crc = new CRC32();
        private long bytesWritten = 0L;
        private FileInfo fileInfo;

        private int bufferOffset = 0;
        private int blockNum = 0;
        private byte[] buffer = new byte[RedisDirectory.BLOCK_SIZE];

        public RedisIndexOutput(FileInfo fileInfo) {
            super(fileInfo.name());
            this.fileInfo = fileInfo;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            flushBlock();
        }

        synchronized private void flushBlock() {
            if (bufferOffset > 0) {
                long t1 = System.currentTimeMillis();
                writeFileBlock(fileInfo, blockNum, buffer);
                blockNum++;
                bufferOffset = 0;
                buffer = new byte[BLOCK_SIZE];
                fileInfo.size(bytesWritten);
                updateFileInfo(fileInfo);
                long t2 = System.currentTimeMillis();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("flushBlock[" + fileInfo.name() + "," + (blockNum - 1) + ","
                            + fileInfo.id() + "] in " + (t2 - t1) + " ms");
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeByte(byte b) throws IOException {
            crc.update(b);
            buffer[bufferOffset++] = b;
            bytesWritten++;
            fileInfo.size(bytesWritten);
            if (bufferOffset >= RedisDirectory.BLOCK_SIZE) {
                flushBlock();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < length; i++) {
                writeByte(b[offset + i]);
            }
            long t2 = System.currentTimeMillis();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("writeBytes[" + fileInfo.name() + "/" + offset + "/" + length
                        + "] in " + (t2 - t1) + " ms");
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getChecksum() throws IOException {
            return crc.getValue();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getFilePointer() {
            return bytesWritten;
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * Redis implementation of {@link IndexInput}.
     * 
     * @author Thanh Nguyen
     * @since 0.1.0
     */
    public class RedisIndexInput extends IndexInput {

        private final Logger LOGGER = LoggerFactory.getLogger(RedisIndexInput.class);

        private RedisDirectory cassDir;
        private FileInfo fileInfo;

        private boolean isSlice = false;
        private byte[] block;
        private int blockOffset = 0;
        private int blockNum = 0;

        private long offset, end, pos;

        public RedisIndexInput(RedisDirectory cassDir, FileInfo fileInfo) {
            super(fileInfo.name());
            this.cassDir = cassDir;
            this.fileInfo = fileInfo;
            this.offset = 0L;
            this.pos = 0L;
            this.end = fileInfo.size();
        }

        public RedisIndexInput(String resourceDesc, RedisIndexInput another, long offset,
                long length) throws IOException {
            super(resourceDesc);
            this.cassDir = another.cassDir;
            this.fileInfo = another.fileInfo;
            this.offset = another.offset + offset;
            this.end = this.offset + length;
            this.blockNum = another.blockNum;
            this.blockOffset = another.blockOffset;
            // if (another.block != null) {
            // this.block = Arrays.copyOf(another.block, another.block.length);
            // }
            seek(0);
        }

        private void loadBlock(int blockNum) {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "loadBlock(" + fileInfo.name() + "/" + blockNum + ")";
                LOGGER.trace(logMsg);
            }
            block = cassDir.readFileBlock(fileInfo, blockNum);
            this.blockNum = blockNum;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RedisIndexInput clone() {
            RedisIndexInput clone = (RedisIndexInput) super.clone();
            clone.cassDir = cassDir;
            clone.fileInfo = fileInfo;
            clone.offset = offset;
            clone.pos = pos;
            clone.end = end;
            clone.blockNum = blockNum;
            clone.blockOffset = blockOffset;
            if (block != null) {
                clone.block = Arrays.copyOf(block, block.length);
            }
            clone.isSlice = this.isSlice;
            return clone;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            // EMPTY
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getFilePointer() {
            return pos;
            // return pos + offset;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long length() {
            return end - offset;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos + offset > end) {
                throw new IllegalArgumentException("Seek position is out of range [0," + length()
                        + "]!");
            }

            if (LOGGER.isTraceEnabled()) {
                String logMsg = "seek(" + fileInfo.name() + "," + isSlice + "," + offset + "/"
                        + end + "," + pos + ") is called";
                LOGGER.trace(logMsg);
            }

            this.pos = pos;
            long newBlockNum = (pos + offset) / RedisDirectory.BLOCK_SIZE;
            if (newBlockNum != blockNum) {
                loadBlock((int) newBlockNum);
            }
            blockOffset = (int) ((pos + offset) % RedisDirectory.BLOCK_SIZE);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public IndexInput slice(String sliceDescription, long offset, long length)
                throws IOException {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "slice(" + sliceDescription + "," + offset + "," + length
                        + ") -> " + fileInfo.name();
                LOGGER.trace(logMsg);
            }
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice(" + sliceDescription + ") "
                        + " out of bounds: " + this);
            }
            RedisIndexInput clone = new RedisIndexInput(sliceDescription, this, offset, length);
            clone.isSlice = true;
            return clone;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public byte readByte() throws IOException {
            if (pos + offset >= end) {
                return -1;
            }

            if (block == null) {
                loadBlock(blockNum);
            }

            byte data = block[blockOffset++];
            pos++;
            if (blockOffset >= RedisDirectory.BLOCK_SIZE) {
                loadBlock(blockNum + 1);
            }
            blockOffset = (int) ((pos + offset) % RedisDirectory.BLOCK_SIZE);
            return data;
        }

        @Override
        public void readBytes(byte[] buffer, int offset, int length) throws IOException {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < length; i++) {
                buffer[offset + i] = readByte();
            }
            long t2 = System.currentTimeMillis();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("readBytes[" + fileInfo.name() + "/" + offset + "/" + length + "] in "
                        + (t2 - t1) + " ms");
            }
        }

    }
}
