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

import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class AllRevisionsOfDocumentCallable extends SQLQueueCallable<DocumentRevisionTree> {

    private static final Logger logger = Logger.getLogger(AllRevisionsOfDocumentCallable.class.getCanonicalName());
    private final String docId;
    private final AttachmentManager attachmentManager;

    public AllRevisionsOfDocumentCallable(String docId, AttachmentManager attachmentManager) {
        this.docId = docId;
        this.attachmentManager = attachmentManager;
    }

    @Override
    public DocumentRevisionTree call(SQLDatabase db) throws Exception {
        return getAllRevisionsOfDocumentInQueue(db, docId);
    }

    private DocumentRevisionTree getAllRevisionsOfDocumentInQueue(SQLDatabase db, String docId)
            throws DocumentNotFoundException, AttachmentException, DatastoreException {
        String sql = "SELECT " + SqlConstants.FULL_DOCUMENT_COLS + " FROM revs, docs " +
                "WHERE docs.docid=? AND revs.doc_id = docs.doc_id ORDER BY sequence ASC";

        String[] args = {docId};
        Cursor cursor = null;

        try {
            DocumentRevisionTree tree = new DocumentRevisionTree();
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                long sequence = cursor.getLong(3);
                List<? extends Attachment> atts = attachmentManager.attachmentsForRevision(db, sequence);
                BasicDocumentRevision rev = SqlDocumentUtils.getFullRevisionFromCurrentCursor(cursor, atts);
                logger.finer("Rev: " + rev);
                tree.add(rev);
            }
            return tree;
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error getting all revisions of document", e);
            throw new DatastoreException("DocumentRevisionTree not found with id: " + docId, e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }

    }

}
