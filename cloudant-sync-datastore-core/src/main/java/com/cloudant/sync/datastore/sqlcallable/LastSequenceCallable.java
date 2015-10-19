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

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class LastSequenceCallable extends SQLQueueCallable<Long> {

    private static final Logger logger = Logger.getLogger(LastSequenceCallable.class.getCanonicalName());

    @Override
    public Long call(SQLDatabase db) throws Exception {
        String sql = "SELECT MAX(sequence) FROM revs";
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = db.rawQuery(sql, null);
            if (cursor.moveToFirst()) {
                if (cursor.columnType(0) == Cursor.FIELD_TYPE_INTEGER) {
                    result = cursor.getLong(0);
                } else if (cursor.columnType(0) == Cursor.FIELD_TYPE_NULL) {
                    result = Datastore.SEQUENCE_NUMBER_START;
                } else {
                    throw new IllegalStateException("SQLite return an unexpected value.");
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting last sequence", e);
            throw new DatastoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return result;
    }
}
