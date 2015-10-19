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

import com.cloudant.android.Base64InputStreamFactory;
import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.BasicDocumentRevision;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.datastore.DocumentRevisionTree;
import com.cloudant.sync.datastore.PreparedAttachment;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.JSONUtils;
import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class ForceInsertCallable extends SQLQueueCallable<Object> {
    private final BasicDocumentRevision rev;
    private final List<String> revisionHistory;
    private final Map<String, Object> attachments;
    private final boolean pullAttachmentsInline;
    private final Map<String[], List<PreparedAttachment>> preparedAttachments;
    private final AttachmentManager attachmentManager;
    private final Logger logger = Logger.getLogger(ForceInsertCallable.class.getCanonicalName());

    public ForceInsertCallable(BasicDocumentRevision rev, List<String> revisionHistory,
                               Map<String, Object> attachments, boolean
                                       pullAttachmentsInline, Map<String[],
            List<PreparedAttachment>> preparedAttachments, AttachmentManager attachmentManager) {
        this.rev = rev;
        this.revisionHistory = revisionHistory;
        this.attachments = attachments;
        this.pullAttachmentsInline = pullAttachmentsInline;
        this.preparedAttachments = preparedAttachments;
        this.attachmentManager = attachmentManager;
    }

    @Override
    public Object call(SQLDatabase db) throws Exception {
        DocumentCreated documentCreated = null;
        DocumentUpdated documentUpdated = null;

        boolean ok = true;

        long seq = 0;


        // sequence here is -1, but we need it to insert the attachment - also might
        // be wanted by subscribers
        BasicDocumentRevision revisionFromDB = null;
        try {
            revisionFromDB = SqlDocumentUtils.getDocumentInQueue(db, rev.getId(), null,
                    attachmentManager);
        } catch (DocumentNotFoundException e) {
            // this is expected since this method is normally used by replication
            // we may be missing the document from our copy
        }

        if (revisionFromDB != null) {
            seq = this.doForceInsertExistingDocumentWithHistory(db, rev, revisionHistory,
                    attachments);
            rev.initialiseSequence(seq);
            // TODO fetch the parent doc?
            documentUpdated = new DocumentUpdated(null, rev);
        } else {
            seq = doForceInsertNewDocumentWithHistory(db, rev, revisionHistory);
            rev.initialiseSequence(seq);
            documentCreated = new DocumentCreated(rev);
        }

        // now deal with any attachments
        if (pullAttachmentsInline) {
            if (attachments != null) {
                for (String att : attachments.keySet()) {
                    Map attachmentMetadata = (Map) attachments.get(att);
                    Boolean stub = (Boolean) attachmentMetadata.get("stub");

                    if (stub != null && stub) {
                        // stubs get copied forward at the end of
                        // insertDocumentHistoryIntoExistingTree - nothing to do here
                        continue;
                    }
                    String data = (String) attachmentMetadata.get("data");
                    String type = (String) attachmentMetadata.get("content_type");
                    InputStream is = Base64InputStreamFactory.get(new
                            ByteArrayInputStream(data.getBytes()));
                    // inline attachments are automatically decompressed,
                    // so we don't have to worry about that
                    UnsavedStreamAttachment usa = new UnsavedStreamAttachment(is,
                            att, type);
                    try {
                        PreparedAttachment pa = attachmentManager.prepareAttachment(usa);
                        attachmentManager.addAttachment(db, pa, rev);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "There was a problem adding the " +
                                        "attachment "
                                        + usa + "to the datastore for document " + rev,
                                e);
                        throw e;
                    }
                }
            }
        } else {

            try {
                if (preparedAttachments != null) {
                    for (String[] key : preparedAttachments.keySet()) {
                        String id = key[0];
                        String rev = key[1];
                        try {
                            BasicDocumentRevision doc = SqlDocumentUtils.getDocumentInQueue(db,
                                    id, rev, attachmentManager);
                            if (doc != null) {
                                for (PreparedAttachment att : preparedAttachments.get
                                        (key)) {
                                    attachmentManager.addAttachment(db, att, doc);
                                }
                            }
                        } catch (DocumentNotFoundException e) {
                            //safe to continue, previously getDocumentInQueue could return
                            // null and this was deemed safe and expected behaviour
                            // DocumentNotFoundException is thrown instead of returning
                            // null now.
                            continue;
                        }
                    }
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "There was a problem adding an " +
                        "attachment to the datastore", e);
                throw e;
            }

        }
        if (ok) {
            logger.log(Level.FINER, "Inserted revision: %s", rev);
            if (documentCreated != null) {
                return documentCreated;
            } else if (documentUpdated != null) {
                return documentUpdated;
            }
        }
        return null;
    }

    /**
     * @param rev        DocumentRevision to insert
     * @param revHistory revision history to insert, it includes all revisions (include the revision of the DocumentRevision
     *                   as well) sorted in ascending order.
     */
    private long doForceInsertNewDocumentWithHistory(SQLDatabase db,
                                                     BasicDocumentRevision rev,
                                                     List<String> revHistory)
            throws AttachmentException {
        logger.entering("BasicDocumentRevision",
                "doForceInsertNewDocumentWithHistory()",
                new Object[]{rev, revHistory});

        long docNumericID = SqlDocumentUtils.insertDocumentID(db, rev.getId());
        long parentSequence = 0L;
        for (int i = 0; i < revHistory.size() - 1; i++) {
            // Insert stub node
            parentSequence = insertStubRevision(db, docNumericID, revHistory.get(i), parentSequence);
        }
        // Insert the leaf node (don't copy attachments)
        SqlDocumentUtils.InsertRevisionOptions options = new SqlDocumentUtils.InsertRevisionOptions();
        options.docNumericId = docNumericID;
        options.revId = revHistory.get(revHistory.size() - 1);
        options.parentSequence = parentSequence;
        options.deleted = rev.isDeleted();
        options.current = true;
        options.data = rev.getBody().asBytes();
        options.available = true;
        long sequence = SqlDocumentUtils.insertRevision(db, options);
        return sequence;
    }

    /**
     * @param newRevision DocumentRevision to insert
     * @param revisions   revision history to insert, it includes all revisions (include the
     *                    revision of the DocumentRevision
     *                    as well) sorted in ascending order.
     */
    private long doForceInsertExistingDocumentWithHistory(SQLDatabase db, BasicDocumentRevision newRevision,
                                                          List<String> revisions,
                                                          Map<String, Object> attachments)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        logger.entering("BasicDatastore",
                "doForceInsertExistingDocumentWithHistory",
                new Object[]{newRevision, revisions, attachments});
        Preconditions.checkNotNull(newRevision, "New document revision must not be null.");
        Preconditions.checkArgument(SqlDocumentUtils.getDocumentInQueue(db, newRevision.getId(),
                null,
                attachmentManager) != null, "DocumentRevisionTree must exist.");
        Preconditions.checkNotNull(revisions, "Revision history should not be null.");
        Preconditions.checkArgument(revisions.size() > 0, "Revision history should have at least one revision.");

        // First look up all locally-known revisions of this document:

        DocumentRevisionTree localRevs;
        try {
            localRevs = new AllRevisionsOfDocumentCallable(newRevision.getId(), attachmentManager)
                    .call(db);
        } catch (Exception e) {
            // We can't load the document for another reason, so
            // throw an exception saying we couldn't find the document.
            throw new RuntimeException(String.format("Error getting all revisions of document" +
                    " with id %s even though revision exists", newRevision.getId()), e);
        }

        assert localRevs != null;

        long sequence;

        BasicDocumentRevision parent = localRevs.lookup(newRevision.getId(), revisions.get(0));
        if (parent == null) {
            sequence = insertDocumentHistoryToNewTree(db, newRevision, revisions, localRevs.getDocumentNumericId(), localRevs);
        } else {
            sequence = insertDocumentHistoryIntoExistingTree(db, newRevision, revisions, localRevs.getDocumentNumericId(), localRevs, attachments);
        }
        return sequence;
    }

    private long insertDocumentHistoryToNewTree(SQLDatabase db, BasicDocumentRevision newRevision,
                                                List<String> revisions,
                                                Long docNumericID,
                                                DocumentRevisionTree localRevs)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        Preconditions.checkArgument(checkCurrentRevisionIsInRevisionHistory(newRevision, revisions),
                "Current revision must exist in revision history.");

        BasicDocumentRevision previousWinner = localRevs.getCurrentRevision();

        // Adding a brand new tree
        logger.finer("Inserting a brand new tree for an existing document.");
        long parentSequence = 0L;
        for (int i = 0; i < revisions.size() - 1; i++) {
            //we copy attachments here so allow the exception to propagate
            parentSequence = insertStubRevision(db, docNumericID, revisions.get(i), parentSequence);
            BasicDocumentRevision newNode = SqlDocumentUtils.getDocumentInQueue(db, newRevision
                            .getId(),
                    revisions.get(i), attachmentManager);
            localRevs.add(newNode);
        }
        // don't copy attachments
        SqlDocumentUtils.InsertRevisionOptions options = new SqlDocumentUtils.InsertRevisionOptions();
        options.docNumericId = docNumericID;
        options.revId = newRevision.getRevision();
        options.parentSequence = parentSequence;
        options.deleted = newRevision.isDeleted();
        options.current = false; // we'll call pickWinnerOfConflicts to set this if it needs it
        options.data = newRevision.asBytes();
        options.available = !newRevision.isDeleted();
        long sequence = SqlDocumentUtils.insertRevision(db, options);
        BasicDocumentRevision newLeaf = SqlDocumentUtils.getDocumentInQueue(db, newRevision
                        .getId(),
                newRevision.getRevision(), attachmentManager);
        localRevs.add(newLeaf);

        // No need to refresh the previousWinner since we are inserting a new tree,
        // and nothing on the old tree should be touched.
        pickWinnerOfConflicts(db, previousWinner, localRevs);
        return sequence;
    }

    private long insertDocumentHistoryIntoExistingTree(SQLDatabase db, BasicDocumentRevision newRevision, List<String> revisions,
                                                       Long docNumericID, DocumentRevisionTree localRevs,
                                                       Map<String, Object> attachments)
            throws AttachmentException, DocumentNotFoundException, DatastoreException {
        BasicDocumentRevision parent = localRevs.lookup(newRevision.getId(), revisions.get(0));
        Preconditions.checkNotNull(parent, "Parent must not be null");
        BasicDocumentRevision previousLeaf = localRevs.getCurrentRevision();


        // Walk through the remote history in chronological order, matching each revision ID to
        // a local revision. When the list diverges, start creating blank local revisions to fill
        // in the local history
        int i;
        for (i = 1; i < revisions.size(); i++) {
            BasicDocumentRevision nextNode = localRevs.lookupChildByRevId(parent, revisions.get(i));
            if (nextNode == null) {
                break;
            } else {
                parent = nextNode;
            }
        }

        if (i >= revisions.size()) {
            logger.finer("All revision are in local sqlDatabase already, no new revision inserted.");
            return -1;
        }

        // Insert the new stub revisions
        for (; i < revisions.size() - 1; i++) {
            logger.finer("Inserting new stub revision, id: " + docNumericID + ", rev: " + revisions.get(i));
            this.changeDocumentToBeNotCurrent(db, parent.getSequence());
            insertStubRevision(db, docNumericID, revisions.get(i), parent.getSequence());
            parent = SqlDocumentUtils.getDocumentInQueue(db, newRevision.getId(), revisions.get
                    (i), attachmentManager);
            localRevs.add(parent);
        }

        // Insert the new leaf revision
        logger.finer("Inserting new revision, id: " + docNumericID + ", rev: " + revisions.get(i));
        String newRevisionId = revisions.get(revisions.size() - 1);
        this.changeDocumentToBeNotCurrent(db, parent.getSequence());
        // don't copy over attachments
        SqlDocumentUtils.InsertRevisionOptions options = new SqlDocumentUtils.InsertRevisionOptions();
        options.docNumericId = docNumericID;
        options.revId = newRevisionId;
        options.parentSequence = parent.getSequence();
        options.deleted = newRevision.isDeleted();
        options.current = false; // we'll call pickWinnerOfConflicts to set this if it needs it
        options.data = newRevision.asBytes();
        options.available = true;
        long sequence = SqlDocumentUtils.insertRevision(db, options);

        BasicDocumentRevision newLeaf = SqlDocumentUtils.getDocumentInQueue(db, newRevision
                        .getId(), newRevisionId,
                attachmentManager);
        localRevs.add(newLeaf);

        // Refresh previous leaf in case it is changed in sqlDb but not in memory
        previousLeaf = SqlDocumentUtils.getDocumentInQueue(db, previousLeaf.getId(),
                previousLeaf.getRevision(),
                attachmentManager);

        pickWinnerOfConflicts(db, previousLeaf, localRevs);

        // copy stubbed attachments forward from last real revision to this revision
        if (attachments != null) {
            for (String att : attachments.keySet()) {
                Boolean stub = ((Map<String, Boolean>) attachments.get(att)).get("stub");
                if (stub != null && stub.booleanValue()) {
                    try {
                        this.attachmentManager.copyAttachment(db, previousLeaf.getSequence(), sequence, att);
                    } catch (SQLException sqe) {
                        logger.log(Level.SEVERE, "Error copying stubbed attachments", sqe);
                        throw new DatastoreException("Error copying stubbed attachments", sqe);
                    }
                }
            }
        }

        return sequence;
    }


    private void pickWinnerOfConflicts(SQLDatabase db, BasicDocumentRevision previousWinner, DocumentRevisionTree objectTree) {

    /*
     Pick winner and mark the appropriate revision with the 'current' flag set
     - There can only be one winner in a tree (or set of trees - if there is no common root)
       at any one time, so if there is a new winner, we only have to mark the old winner as
       no longer 'current'. This is the 'previousWinner' object
     - The new winner is determined by:
       * consider only non-deleted leafs
       * sort according to the CouchDB sorting algorithm: highest rev wins, if there is a tie
         then do a lexicographical compare of the revision id strings
       * we do a reverse sort (highest first) and pick the 1st and mark it 'current'
       * special case: if all leafs are deleted, then apply sorting and selection criteria
         above to all leafs
     */

        // first get all non-deleted leafs
        List<BasicDocumentRevision> leafs = objectTree.leafRevisions(true);
        if (leafs.size() == 0) {
            // all deleted, apply the normal rules to all the leafs
            leafs = objectTree.leafRevisions();
        }

        Collections.sort(leafs, new Comparator<BasicDocumentRevision>() {
            @Override
            public int compare(BasicDocumentRevision r1, BasicDocumentRevision r2) {
                int generationCompare = r1.getGeneration() - r2.getGeneration();
                // note that the return statements have a unary minus since we are reverse sorting
                if (generationCompare != 0) {
                    return -generationCompare;
                } else {
                    return -r1.getRevision().compareTo(r2.getRevision());
                }
            }
        });
        // new winner will be at the top of the list
        BasicDocumentRevision leaf = leafs.get(0);
        if (previousWinner.getSequence() != leaf.getSequence()) {
            this.changeDocumentToBeNotCurrent(db, previousWinner.getSequence());
            this.changeDocumentToBeCurrent(db, leaf.getSequence());
        }
    }

    private void changeDocumentToBeCurrent(SQLDatabase db, long sequence) {
        ContentValues args = new ContentValues();
        args.put("current", 1);
        String[] whereArgs = {Long.toString(sequence)};
        db.update("revs", args, "sequence=?", whereArgs);
    }

    private void changeDocumentToBeNotCurrent(SQLDatabase db, long sequence) {
        ContentValues args = new ContentValues();
        args.put("current", 0);
        String[] whereArgs = {Long.toString(sequence)};
        db.update("revs", args, "sequence=?", whereArgs);
    }

    private long insertStubRevision(SQLDatabase db, long docNumericId, String revId, long parentSequence) throws AttachmentException {
        // don't copy attachments
        SqlDocumentUtils.InsertRevisionOptions options = new SqlDocumentUtils.InsertRevisionOptions();
        options.docNumericId = docNumericId;
        options.revId = revId;
        options.parentSequence = parentSequence;
        options.deleted = false;
        options.current = false;
        options.data = JSONUtils.EMPTY_JSON;
        options.available = false;
        return SqlDocumentUtils.insertRevision(db, options);
    }

    private boolean checkCurrentRevisionIsInRevisionHistory(BasicDocumentRevision rev, List<String> revisionHistory) {
        return revisionHistory.get(revisionHistory.size() - 1).equals(rev.getRevision());
    }

}
