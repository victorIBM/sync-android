/**
 * Copyright (c) 2015 IBM Cloudant. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore.sqlcallable;

import com.cloudant.sync.datastore.BasicDocumentBody;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.LocalDocument;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;
import com.google.common.base.Strings;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public abstract class LocalDocumentsCallable extends SQLQueueCallable<LocalDocument> {

    private final Logger logger = Logger.getLogger(LocalDocumentsCallable.class.getCanonicalName());

    LocalDocument doGetLocalDocument(SQLDatabase db, String docId)
            throws DocumentNotFoundException, DocumentException, DatastoreException {
        assert !Strings.isNullOrEmpty(docId);
        Cursor cursor = null;
        try {
            String[] args = {docId};
            cursor = db.rawQuery("SELECT json FROM localdocs WHERE docid=?", args);
            if (cursor.moveToFirst()) {
                byte[] json = cursor.getBlob(0);

                return new LocalDocument(docId, BasicDocumentBody.bodyWith(json));
            } else {
                throw new DocumentNotFoundException(String.format("No local document found with id: %s", docId));
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, String.format("Error getting local document with id: %s", docId), e);
            throw new DatastoreException("Error getting local document with id: " + docId, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }

}
