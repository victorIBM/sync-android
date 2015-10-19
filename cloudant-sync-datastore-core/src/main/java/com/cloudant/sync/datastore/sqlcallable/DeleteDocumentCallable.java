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
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Callable to delete with given ID, regardless of rev
 */
public class DeleteDocumentCallable extends DocumentsCallable<List<BasicDocumentRevision>> {

    private final String id;

    public DeleteDocumentCallable(String id, AttachmentManager attachmentManager) {
        super(attachmentManager);
        this.id = id;
    }

    @Override
    public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
        ArrayList<BasicDocumentRevision> deleted = new ArrayList<BasicDocumentRevision>();
        Cursor cursor = null;
        // delete all in one tx
        try {
            // get revid for each leaf
            final String sql = "SELECT revs.revid FROM docs,revs " +
                    "WHERE revs.doc_id = docs.doc_id " +
                    "AND docs.docid = ? " +
                    "AND deleted = 0 AND revs.sequence NOT IN " +
                    "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) ";

            cursor = db.rawQuery(sql, new String[]{id});
            while (cursor.moveToNext()) {
                String revId = cursor.getString(0);
                deleted.add(deleteDocumentInQueue(db, id, revId, attachmentManager));
            }
            return deleted;
        } catch (SQLException sqe) {
            throw new DatastoreException("SQLException in deleteDocument, not deleting revisions", sqe);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }
}
