package com.cloudant.sync.sqlite.android.encryption;

/**
 * Created by estebanmlaver on 4/8/15.
 */
public interface EncryptionKeychainStorage {

    public EncryptionKeychainData encryptionKeyData();

    public boolean saveEncryptionKeyData(EncryptionKeychainData data);

    public boolean clearEncryptionKeyData();

    public boolean existingEncryptionKeyData();

}
