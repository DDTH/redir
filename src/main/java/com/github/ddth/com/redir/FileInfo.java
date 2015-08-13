package com.github.ddth.com.redir;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.dao.BaseBo;

/**
 * File metadata info.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class FileInfo extends BaseBo {

    public final static FileInfo[] EMPTY_ARRAY = new FileInfo[0];
    public final static IdGenerator ID_GEN = IdGenerator.getInstance(IdGenerator.getMacAddr());

    public static FileInfo newInstance() {
        String id = ID_GEN.generateId128Hex().toLowerCase();
        FileInfo fileInfo = new FileInfo();
        fileInfo.id(id).size(0);
        return fileInfo;
    }

    public static FileInfo newInstance(String name) {
        FileInfo fileInfo = newInstance();
        fileInfo.name(name);
        return fileInfo;
    }

    @SuppressWarnings("unchecked")
    public static FileInfo newInstance(byte[] data) {
        if (data == null || data.length <= 8) {
            return null;
        }
        Map<String, Object> dataMap = SerializationUtils.fromByteArray(data, Map.class);
        if (dataMap == null) {
            return null;
        }
        FileInfo fileInfo = newInstance();
        fileInfo.fromMap(dataMap);
        return fileInfo;
    }

    private final static String ATTR_NAME = "filename";
    private final static String ATTR_SIZE = "filesize";
    private final static String ATTR_ID = "fileid";

    public byte[] asBytes() {
        Map<?, ?> data = toMap();
        return data != null ? SerializationUtils.toByteArray(data) : null;
    }

    @JsonIgnore
    public String name() {
        return getAttribute(ATTR_NAME, String.class);
    }

    public FileInfo name(String filename) {
        return (FileInfo) setAttribute(ATTR_NAME, filename);
    }

    @JsonIgnore
    public long size() {
        Long result = getAttribute(ATTR_SIZE, Long.class);
        return result != null ? result.longValue() : null;
    }

    public FileInfo size(long filesize) {
        return (FileInfo) setAttribute(ATTR_SIZE, filesize);
    }

    @JsonIgnore
    public String id() {
        return getAttribute(ATTR_ID, String.class);
    }

    public FileInfo id(String id) {
        return (FileInfo) setAttribute(ATTR_ID, id);
    }

}
