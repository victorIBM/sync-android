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

import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.ConflictException;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentBody;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.CouchUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Created by mike on 17/10/2015.
 */
public class UpdateDocumentCallable extends SQLQueueCallable<BasicDocumentRevision> {
    private final MutableDocumentRevision rev;
    private final AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments;
    private final AttachmentManager attachmentManager;

    public UpdateDocumentCallable(MutableDocumentRevision rev, AttachmentManager
            .PreparedAndSavedAttachments preparedAndSavedAttachments, AttachmentManager
            attachmentManager) {
        this.rev = rev;
        this.preparedAndSavedAttachments = preparedAndSavedAttachments;
        this.attachmentManager = attachmentManager;
    }

    @Override
    public BasicDocumentRevision call(SQLDatabase db) throws Exception {
        return updateDocumentFromRevision(db, rev, preparedAndSavedAttachments);
    }

    private BasicDocumentRevision updateDocumentFromRevision(SQLDatabase db, MutableDocumentRevision rev,
                                                     AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments)
            throws ConflictException, AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkNotNull(rev, "DocumentRevision can not be null");

        // update document with new body
        BasicDocumentRevision updated = this.updateDocument(db, rev.docId, rev
                .getSourceRevisionId(), rev.body, true, false);
        // set attachments
        this.attachmentManager.setAttachments(db, updated, preparedAndSavedAttachments);
        // now re-fetch the revision with updated attachments
        BasicDocumentRevision updatedWithAttachments = SqlDocumentUtils.getDocumentInQueue(db,
                updated.getId(), updated.getRevision(), attachmentManager);
        return updatedWithAttachments;
    }

    private BasicDocumentRevision updateDocument(SQLDatabase db, String docId,
                                         String prevRevId,
                                         final DocumentBody body,
                                         boolean validateBody,
                                         boolean copyAttachments)
            throws ConflictException, AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prevRevId),
                "Input previous revision id can not be empty");
        Preconditions.checkNotNull(body, "Input document body can not be null");
        if (validateBody) {
            SqlDocumentUtils.validateDBBody(body);
        }
        CouchUtils.validateRevisionId(prevRevId);

        BasicDocumentRevision preRevision = SqlDocumentUtils.getDocumentInQueue(db, docId,
                prevRevId,
                attachmentManager);

        if (!preRevision.isCurrent()) {
            throw new ConflictException("Revision to be updated is not current revision.");
        }

        SqlDocumentUtils.setCurrent(db, preRevision, false);
        String newRevisionId = insertNewWinnerRevision(db, body, preRevision, copyAttachments);
        return SqlDocumentUtils.getDocumentInQueue(db, preRevision.getId(), newRevisionId,
                attachmentManager);
    }

    private String insertNewWinnerRevision(SQLDatabase db, DocumentBody newWinner,
                                           BasicDocumentRevision oldWinner,
                                           boolean copyAttachments)
            throws AttachmentException, DatastoreException {
        String newRevisionId = CouchUtils.generateNextRevisionId(oldWinner.getRevision());

        SqlDocumentUtils.InsertRevisionOptions options = new SqlDocumentUtils.InsertRevisionOptions();
        options.docNumericId = oldWinner.getInternalNumericId();
        options.revId = newRevisionId;
        options.parentSequence = oldWinner.getSequence();
        options.deleted = false;
        options.current = true;
        options.data = newWinner.asBytes();
        options.available = true;

        if (copyAttachments) {
            this.insertRevisionAndCopyAttachments(db, options);
        } else {
            SqlDocumentUtils.insertRevision(db, options);
        }

        return newRevisionId;
    }

    private long insertRevisionAndCopyAttachments(SQLDatabase db, SqlDocumentUtils.InsertRevisionOptions options) throws AttachmentException, DatastoreException {
        long newSequence = SqlDocumentUtils.insertRevision(db, options);
        //always copy attachments
        this.attachmentManager.copyAttachments(db, options.parentSequence, newSequence);
        // inserted revision and copied attachments, so we are done
        return newSequence;
    }
}
