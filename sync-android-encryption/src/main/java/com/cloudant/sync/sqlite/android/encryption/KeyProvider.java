/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.cloudant.sync.sqlite.android.encryption;

import android.content.Context;

import com.cloudant.sync.sqlite.android.encryption.jsonstore.SecurityManager;
import com.cloudant.sync.sqlite.android.encryption.jsonstore.SecurityUtils;

/**
 * This class retrieves the user's SQLCipher password.
 * A secure key is generated based on the password, and
 * is then stored into local storage.
 *
 * TODO: Add JSONStore implementation for proper encryption and key management.
 * Created by estebanmlaver.
 */
public class KeyProvider  {
    private String encryptedKey;

    public KeyProvider(String password, String identifier, Context context) {

        this.encryptedKey = password;


        //Create a security manager with the Android app context and string ID
        //SecurityManager encryptionManager = SecurityManager.getInstance(context);
        handleSQLCipherKey(identifier, password, context);
    }

    public String getEncryptedKey() {
        return encryptedKey;
    }

    //Handle managing and storing key to open SQLCipher database in Android's shared preferences
    private void handleSQLCipherKey(String identifier, String password, Context context) { //throws JSONStoreCloseAllException, JSONStoreInvalidPasswordException {


        if ((password != null) && !password.equals("")) {
            //Number of bytes for salt is 32
            String salt = SecurityUtils.getRandomString(32);

            try {

                //Check that the identifier does not already exist
                if (!SecurityManager.getInstance(context).isDPKAvailable(identifier)) {
                    SecurityManager.getInstance(context).storeDPK(password, identifier, salt, false);
                }

                // We should do a simple query to check if we can access the DB
                // if the password is wrong this should throw error -3 'Invalid Key On Provision'.
                // We don't because we don't have an accessor yet.

            }

            catch (Throwable e) {
                // Password must be invalid.
                String message = "Error setting key.";
                //JSONStoreInvalidPasswordException jsException = new JSONStoreInvalidPasswordException(message, e);
                //logger.logTrace(message);
                //throw jsException;
            }
        }
    }


}
