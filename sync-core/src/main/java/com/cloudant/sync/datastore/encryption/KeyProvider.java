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
package com.cloudant.sync.datastore.encryption;

/**
 * This class retrieves the user's SQLCipher password.
 * A secure key is generated based on the password, and
 * is then stored into local storage.
 *
 * TODO: implementation for proper encryption and key management.
 * Created by estebanmlaver.
 */
public class KeyProvider  {
    private String encryptedKey;

    KeyProvider(String password) {

        //TODO Pass Android context - use reflection?
        //this.encryptedKey = handleSQLCipherKey(null, password, null);
        this.encryptedKey = password;
    }



    /*public KeyProvider(String password, String identifier, Context context) {

        this.encryptedKey = handleSQLCipherKey(identifier, password, context);
    }*/


    public String getEncryptedKey() {
        return encryptedKey;
    }

    //Handle managing and storing key to open SQLCipher database in Android's shared preferences
    //For multiple databases, need to have an identifier
    private String handleSQLCipherKey(String identifier, String password) {

        if(identifier == null || identifier.isEmpty()) {
            identifier = "";
        }
        String dpk = null;
        if ((password != null) && !password.equals("")) {

            try {

                //Check that the identifier does not already exist
                //TODO fix
              /*  if (!com.cloudant.sync.sqlite.android.encryption.SecurityManager.getInstance().isDPKAvailable(identifier)) {
                    //TODO initialize Android context requirement under sync-android
                    //SecurityManager.getInstance(context).storeDPK(password, identifier, salt, false);
                    SecurityManager.getInstance().storeDPK(password, identifier, false);
                }
                dpk = SecurityManager.getInstance().getDPK(password, identifier); */

            } catch (Throwable e) {
                // Password must be invalid.
                String message = "Error setting key.";
                //JSONStoreInvalidPasswordException jsException = new JSONStoreInvalidPasswordException(message, e);
                //logger.logTrace(message);
                //throw jsException;
            }
        }
        return dpk;
    }

}
