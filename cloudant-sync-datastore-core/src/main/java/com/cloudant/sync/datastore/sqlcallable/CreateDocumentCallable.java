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
import com.cloudant.sync.util.CouchUtils;
import com.google.common.base.Preconditions;

import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class CreateDocumentCallable extends DocumentsCallable<BasicDocumentRevision> {
    private final Logger logger = Logger.getLogger(CreateDocumentCallable.class.getCanonicalName());
    private final String docId;
    private final MutableDocumentRevision rev;
    private final AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments;

    public CreateDocumentCallable(String docId, MutableDocumentRevision rev,
                                  AttachmentManager.PreparedAndSavedAttachments
                                          preparedAndSavedAttachments, AttachmentManager
                                          attachmentManager) {
        super(attachmentManager);
        this.docId = docId;
        this.rev = rev;
        this.preparedAndSavedAttachments = preparedAndSavedAttachments;
    }

    @Override
    public BasicDocumentRevision call(SQLDatabase db) throws Exception {
        // save document with body
        BasicDocumentRevision saved = createDocument(db, docId, rev.body);
        // set attachments
        attachmentManager.setAttachments(db, saved, preparedAndSavedAttachments);
        // now re-fetch the revision with updated attachments
        BasicDocumentRevision updatedWithAttachments = getDocumentInQueue(db,
                saved.getId(), saved.getRevision(), attachmentManager);
        return updatedWithAttachments;
    }

    private BasicDocumentRevision createDocument(SQLDatabase db, String docId, final DocumentBody body)
            throws AttachmentException, ConflictException, DatastoreException {
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body can not be null");
        validateDBBody(body);

        // check if the docid exists first:

        // if it does exist:
        // * if winning leaf deleted, root the 'created' document there
        // * else raise error
        // if it does not exist:
        // * normal insert logic for a new document

        InsertRevisionOptions options = new InsertRevisionOptions();
        BasicDocumentRevision potentialParent = null;

        try {
            potentialParent = this.getDocumentInQueue(db, docId, null, attachmentManager);
        } catch (DocumentNotFoundException e) {
            //this is an expected exception, it just means we are
            // resurrecting the document
        }

        if (potentialParent != null) {
            if (!potentialParent.isDeleted()) {
                // current winner not deleted, can't insert
                throw new ConflictException(String.format("Cannot create doc, document with id %s already exists "
                        , docId));
            }
            // if we got here, parent rev was deleted
            this.setCurrent(db, potentialParent, false);
            options.revId = CouchUtils.generateNextRevisionId(potentialParent.getRevision());
            options.docNumericId = potentialParent.getInternalNumericId();
            options.parentSequence = potentialParent.getSequence();
        } else {
            // otherwise we are doing a normal create document
            long docNumericId = insertDocumentID(db, docId);
            options.revId = CouchUtils.getFirstRevisionId();
            options.docNumericId = docNumericId;
            options.parentSequence = -1l;
        }
        options.deleted = false;
        options.current = true;
        options.data = body.asBytes();
        options.available = true;
        insertRevision(db, options);

        try {
            BasicDocumentRevision doc = getDocumentInQueue(db, docId, options.revId, attachmentManager);
            logger.finer("New document created: " + doc.toString());
            return doc;
        } catch (DocumentNotFoundException e) {
            throw new RuntimeException(String.format("Couldn't get document we just inserted " +
                    "(id: %s); this shouldn't happen, please file an issue with as much detail " +
                    "as possible.", docId), e);
        }

    }
}
