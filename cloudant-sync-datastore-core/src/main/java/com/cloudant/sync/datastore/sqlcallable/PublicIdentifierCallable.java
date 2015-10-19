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

import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;

/**
 * Created by mike on 17/10/2015.
 */
public class PublicIdentifierCallable extends SQLQueueCallable<String> {
    @Override
    public String call(SQLDatabase db) throws Exception {
        Cursor cursor = null;
        try {
            cursor = db.rawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
            if (cursor.moveToFirst()) {
                return "touchdb_" + cursor.getString(0);
            } else {
                throw new IllegalStateException("Error querying PublicUUID, " +
                        "it is probably because the sqlDatabase is not probably initialized.");
            }
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }
}