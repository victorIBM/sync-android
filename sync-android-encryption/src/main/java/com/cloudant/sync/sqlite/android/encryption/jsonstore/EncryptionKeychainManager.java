package com.cloudant.sync.sqlite.android.encryption.jsonstore;

/**
 * Created by estebanmlaver.
 */
public interface EncryptionKeychainManager {

    public void initWithStorage(EncryptionKeychainStorage storage);

    public String retrieveEncryptionKeyUsingPassword(String password);

    public String generateEncryptionKeyUsingPassword(String password);

    public boolean clearEncryptionKey();

    public boolean encryptionKeyAlreadyGenerated();
}
