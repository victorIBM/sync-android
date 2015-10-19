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
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class ConflictedDocumentIdsCallable extends SQLQueueCallable<Iterator<String>> {

    // the "SELECT DISTINCT ..." subquery selects all the parent
    // sequence, and so the outer "SELECT ..." practically selects
    // all the leaf nodes. The "GROUP BY" and "HAVING COUNT(*) > 1"
    // make sure only those document with more than one leafs are
    // returned.
    private final String sql = "SELECT docs.docid, COUNT(*) FROM docs,revs " +
            "WHERE revs.doc_id = docs.doc_id " +
            "AND deleted = 0 AND revs.sequence NOT IN " +
            "(SELECT DISTINCT parent FROM revs WHERE parent NOT NULL) " +
            "GROUP BY docs.docid HAVING COUNT(*) > 1";

    private static final Logger logger = Logger.getLogger(ConflictedDocumentIdsCallable.class.getCanonicalName());

    @Override
    public Iterator<String> call(SQLDatabase db) throws Exception {

        List<String> conflicts = new ArrayList<String>();
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, new String[]{});
            while (cursor.moveToNext()) {
                String docId = cursor.getString(0);
                conflicts.add(docId);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting conflicted document: ", e);
            throw new DatastoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
        return conflicts.iterator();
    }
}
