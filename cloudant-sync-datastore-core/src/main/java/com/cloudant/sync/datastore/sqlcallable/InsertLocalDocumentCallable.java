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

import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.LocalDocument;
import com.cloudant.sync.sqlite.ContentValues;
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
public class InsertLocalDocumentCallable extends LocalDocumentsCallable {
    private final String docId;
    private final DocumentBody body;
    private final Logger logger = Logger.getLogger(InsertLocalDocumentCallable.class.getCanonicalName());

    public InsertLocalDocumentCallable(String docId, DocumentBody body) {
        this.docId = docId;
        this.body = body;
    }

    @Override
    public LocalDocument call(SQLDatabase db) throws Exception {
        ContentValues values = new ContentValues();
        values.put("docid", docId);
        values.put("json", body.asBytes());

        long rowId = db.insertWithOnConflict("localdocs", values, SQLDatabase
                .CONFLICT_REPLACE);
        if (rowId < 0) {
            throw new DocumentException("Failed to insert local document");
        } else {
            logger.finer(String.format("Local doc inserted: %d , %s", rowId, docId));
        }

        return doGetLocalDocument(db, docId);
    }
}
