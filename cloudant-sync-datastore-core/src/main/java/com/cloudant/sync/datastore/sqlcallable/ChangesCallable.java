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

import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.Changes;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mike on 17/10/2015.
 */
public class ChangesCallable extends DocumentsCallable<Changes> {
    private final long verifiedSince;
    private final int limit;

    private static final String SQL_CHANGE_IDS_SINCE_LIMIT = "SELECT doc_id, max(sequence) FROM revs " +
            "WHERE sequence > ? AND sequence <= ? GROUP BY doc_id ";

    public ChangesCallable(long verifiedSince, int limit, AttachmentManager attachmentManager) {
        super(attachmentManager);
        this.verifiedSince = verifiedSince;
        this.limit = limit;
    }

    @Override
    public Changes call(SQLDatabase db) throws Exception {
        String[] args = {Long.toString(verifiedSince), Long.toString(verifiedSince + limit)};
        Cursor cursor = null;
        try {
            Long lastSequence = verifiedSince;
            List<Long> ids = new ArrayList<Long>();
            cursor = db.rawQuery(SQL_CHANGE_IDS_SINCE_LIMIT, args);
            while (cursor.moveToNext()) {
                ids.add(cursor.getLong(0));
                lastSequence = Math.max(lastSequence, cursor.getLong(1));
            }

            GetDocumentsWithInternalIdsCallable callable = new GetDocumentsWithInternalIdsCallable(ids, attachmentManager);
            List<BasicDocumentRevision> results = callable.call(db);
            if (results.size() != ids.size()) {
                throw new IllegalStateException("The number of document does not match number of ids, " +
                        "something must be wrong here.");
            }

            return new Changes(lastSequence, results);
        } catch (SQLException e) {
            throw new IllegalStateException("Error querying all changes since: " + verifiedSince + ", limit: " + limit, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }
}
