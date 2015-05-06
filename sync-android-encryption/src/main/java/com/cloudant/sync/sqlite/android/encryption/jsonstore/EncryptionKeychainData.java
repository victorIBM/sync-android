package com.cloudant.sync.sqlite.android.encryption.jsonstore;

/**
 * Created by estebanmlaver on 4/8/15.
 */
public interface EncryptionKeychainData {
    public String encryptedDPK = null;
    public String salt = null;
    public String IV = null;
    public int iterations = 0;
    public String version = null;
}
