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
import com.cloudant.sync.datastore.ConflictResolver;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class ResolveConflictsCallable extends SQLQueueCallable<Object> {
    private final String docId;
    private final ConflictResolver resolver;
    private final AttachmentManager attachmentManager;

    private final Logger logger = Logger.getLogger(ResolveConflictsCallable.class.getCanonicalName());

    public ResolveConflictsCallable(String docId, ConflictResolver resolver, AttachmentManager
            attachmentManager) {
        this.docId = docId;
        this.resolver = resolver;
        this.attachmentManager = attachmentManager;
    }

    @Override
    public Object call(SQLDatabase db) throws Exception {
        DocumentRevisionTree docTree =
                new AllRevisionsOfDocumentCallable(docId, attachmentManager).call(db);
        if (!docTree.hasConflicts()) {
            return null;
        }
        DocumentRevision newWinner = null;
        try {
            newWinner = resolver.resolve(docId, docTree.leafRevisions(true));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception when calling ConflictResolver", e);
        }
        if (newWinner == null) {
            // resolve() threw an exception or returned null, exit early
            return null;
        }

        AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments = null;
        if (newWinner.getClass() == MutableDocumentRevision.class) {
            preparedAndSavedAttachments = attachmentManager.prepareAttachments(
                    newWinner.getAttachments() != null ? newWinner.getAttachments().values() : null);
        }

        // if it's BasicDocumentRev:
        // - keep the winner, delete the rest
        // if it's MutableDocumentRev:
        // - delete all except the sourceRevId and graft the new revision on later

        // the revid to keep:
        // - this will be the source rev id if it's a MutableDocumentRev
        // - this will be rev id otherwise
        String revIdKeep;
        if (newWinner.getClass() == MutableDocumentRevision.class) {
            revIdKeep = ((MutableDocumentRevision) newWinner).getSourceRevisionId();
        } else {
            revIdKeep = newWinner.getRevision();
        }

        for (BasicDocumentRevision revision : docTree.leafRevisions()) {
            if (revision.getRevision().equals(revIdKeep)) {
                // this is the one we want to keep, set it to current
                DocumentsCallable.setCurrent(db, revision, true);
            } else {
                if (revision.isDeleted()) {
                    // if it is deleted, just make it non-current
                    DocumentsCallable.setCurrent(db, revision, false);
                } else {
                    // if it's not deleted, deleted and make it non-current
                    BasicDocumentRevision deleted = DocumentsCallable.deleteDocumentInQueue(db,
                            revision.getId(), revision.getRevision(), attachmentManager);
                    DocumentsCallable.setCurrent(db, deleted, false);
                }
            }
        }

        // if it's MutableDocumentRev: graft the new revision on
        if (newWinner.getClass() == MutableDocumentRevision.class) {
            new UpdateDocumentCallable((MutableDocumentRevision) newWinner,
                    preparedAndSavedAttachments, attachmentManager).call(db);
        }

        return null;
    }
}
