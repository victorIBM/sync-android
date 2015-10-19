/**
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright (c) 2013 Cloudant, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import com.cloudant.sync.datastore.encryption.KeyProvider;
import com.cloudant.sync.datastore.encryption.NullKeyProvider;
import com.cloudant.sync.datastore.migrations.SchemaOnlyMigration;
import com.cloudant.sync.datastore.migrations.MigrateDatabase6To100;
import com.cloudant.sync.datastore.sqlcallable.AllRevisionsOfDocumentCallable;
import com.cloudant.sync.datastore.sqlcallable.AttachmentManager;
import com.cloudant.sync.datastore.sqlcallable.ChangesCallable;
import com.cloudant.sync.datastore.sqlcallable.CompactDatabaseCallable;
import com.cloudant.sync.datastore.sqlcallable.ConflictedDocumentIdsCallable;
import com.cloudant.sync.datastore.sqlcallable.CreateDocumentCallable;
import com.cloudant.sync.datastore.sqlcallable.DeleteDocumentCallable;
import com.cloudant.sync.datastore.sqlcallable.DeleteDocumentFromRevisionCallable;
import com.cloudant.sync.datastore.sqlcallable.DeleteLocalDocumentCallable;
import com.cloudant.sync.datastore.sqlcallable.DocumentCountCallable;
import com.cloudant.sync.datastore.sqlcallable.ForceInsertCallable;
import com.cloudant.sync.datastore.sqlcallable.GetAllDocumentIdsCallable;
import com.cloudant.sync.datastore.sqlcallable.GetAllDocumentsCallable;
import com.cloudant.sync.datastore.sqlcallable.GetAttachmentCallable;
import com.cloudant.sync.datastore.sqlcallable.GetAttachmentsForRevisionCallable;
import com.cloudant.sync.datastore.sqlcallable.GetDocumentCallable;
import com.cloudant.sync.datastore.sqlcallable.GetDocumentsWithIdsCallable;
import com.cloudant.sync.datastore.sqlcallable.GetDocumentsWithInternalIdsCallable;
import com.cloudant.sync.datastore.sqlcallable.GetLocalDocumentCallable;
import com.cloudant.sync.datastore.sqlcallable.GetPossibleAncestorRevIdsCallable;
import com.cloudant.sync.datastore.sqlcallable.InsertLocalDocumentCallable;
import com.cloudant.sync.datastore.sqlcallable.LastSequenceCallable;
import com.cloudant.sync.datastore.sqlcallable.PublicIdentifierCallable;
import com.cloudant.sync.datastore.sqlcallable.ResolveConflictsCallable;
import com.cloudant.sync.datastore.sqlcallable.RevsDiffCallable;
import com.cloudant.sync.datastore.sqlcallable.UpdateDocumentCallable;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DocumentCreated;
import com.cloudant.sync.notifications.DocumentDeleted;
import com.cloudant.sync.notifications.DocumentUpdated;
import com.cloudant.sync.sqlite.SQLDatabaseQueue;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.CouchUtils;
import com.cloudant.sync.util.JSONUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.EventBus;

import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

class BasicDatastore implements Datastore, DatastoreExtended {

    private static final Logger logger = Logger.getLogger(BasicDatastore.class.getCanonicalName());

    private final String datastoreName;
    private final EventBus eventBus;
    private final AttachmentManager attachmentManager;

    final String datastoreDir;
    final String extensionsDir;

    private static final String DB_FILE_NAME = "db.sync";

    /**
     * Stores a reference to the encryption key provider so
     * it can be passed to extensions.
     */
    private final KeyProvider keyProvider;

    /**
     * Queue for all database tasks.
     */
    private final SQLDatabaseQueue queue;

    public BasicDatastore(String dir, String name) throws SQLException, IOException, DatastoreException {
        this(dir, name, new NullKeyProvider());
    }

    /**
     * Constructor for single thread SQLCipher-based datastore.
     * @param dir The directory where the datastore will be created
     * @param name The user-defined name of the datastore
     * @param provider The key provider object that contains the user-defined SQLCipher key
     * @throws SQLException
     * @throws IOException
     */
    public BasicDatastore(String dir, String name, KeyProvider provider) throws SQLException, IOException, DatastoreException {
        Preconditions.checkNotNull(dir);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(provider);

        this.keyProvider = provider;
        this.datastoreDir = dir;
        this.datastoreName = name;
        this.extensionsDir = FilenameUtils.concat(this.datastoreDir, "extensions");
        final String dbFilename = FilenameUtils.concat(this.datastoreDir, DB_FILE_NAME);
        queue = new SQLDatabaseQueue(dbFilename, provider);

        int dbVersion = queue.getVersion();
        // Increment the hundreds position if a schema change means that older
        // versions of the code will not be able to read the migrated database.
        if(dbVersion >= 200){
            throw new DatastoreException(String.format("Database version is higher than the version supported " +
                    "by this library, current version %d , highest supported version %d",dbVersion, 99));
        }
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion3()), 3);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion4()), 4);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion5()), 5);
        queue.updateSchema(new SchemaOnlyMigration(DatastoreConstants.getSchemaVersion6()), 6);
        queue.updateSchema(new MigrateDatabase6To100(), 100);
        this.eventBus = new EventBus();
        this.attachmentManager = new AttachmentManager(this);
    }

    @Override
    public String getDatastoreName() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return this.datastoreName;
    }

    @Override
    public KeyProvider getKeyProvider() {
        return this.keyProvider;
    }

    @Override
    public long getLastSequence() {
        Preconditions.checkState(this.isOpen(), "Database is closed");

        try {

            return queue.submit(new LastSequenceCallable()).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get last Sequence",e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get last Sequence", e);
            if(e.getCause()!=null){
                if(e.getCause() instanceof IllegalStateException){
                    throw (IllegalStateException) e.getCause();
                }
            }
        }
        return 0;

    }

    @Override
    public int getDocumentCount() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new DocumentCountCallable()).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get document count",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get document count", e);
        }
        return 0;

    }

    @Override
    public boolean containsDocument(String docId, String revId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return getDocument(docId, revId) != null;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean containsDocument(String docId) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return getDocument(docId) != null;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public BasicDocumentRevision getDocument(String id) throws DocumentNotFoundException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return getDocument(id, null);
    }

    @Override
    public BasicDocumentRevision getDocument(final String id, final String rev) throws DocumentNotFoundException{
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "DocumentRevisionTree id can not " +
                "be empty");

        try {
            return queue.submit(new GetDocumentCallable(id, rev, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get document",e);
        } catch (ExecutionException e) {
            throw new DocumentNotFoundException(e);
        }
        return null;
    }

    public DocumentRevisionTree getAllRevisionsOfDocument(final String docId) {

        try {
            return queue.submit(new AllRevisionsOfDocumentCallable(docId, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get all revisions of document", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get all revisions of document", e);
        }
        return null;
    }


    @Override
    public Changes changes(long since,final int limit) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(limit > 0, "Limit must be positive number");
        final long verifiedSince = since >= 0 ? since : 0;

        try {
            return queue.submit(new ChangesCallable(verifiedSince, limit, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get changes",e);
        } catch (ExecutionException e) {
           logger.log(Level.SEVERE, "Failed to get changes",e);
            if(e.getCause()!= null){
                if(e.getCause() instanceof IllegalStateException) {
                    throw (IllegalStateException) e.getCause();
                }
            }
        }

        return null;

    }

    /**
     * Get list of documents for given list of numeric ids. The result list is ordered by sequence number,
     * and only the current revisions are returned.
     *
     * @param docIds given list of internal ids
     * @return list of documents ordered by sequence number
     */
    List<BasicDocumentRevision> getDocumentsWithInternalIds(final List<Long> docIds) {
        Preconditions.checkNotNull(docIds, "Input document internal id list can not be null");

        try {
            return queue.submit(new GetDocumentsWithInternalIdsCallable(docIds, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get documents using internal ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get documents using internal ids",e);
        }
        return null;


    }

    @Override
    public List<BasicDocumentRevision> getAllDocuments(final int offset,final  int limit,final boolean descending) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        try {
            return queue.submit(new GetAllDocumentsCallable(descending, limit, offset, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get all documents",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to get all documents",e);
        }
        return null;

    }

    @Override
    public List<String> getAllDocumentIds() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new GetAllDocumentIdsCallable()).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get all document ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to get all document ids",e);
        }
        return null;
    }

    @Override
    public List<BasicDocumentRevision> getDocumentsWithIds(final List<String> docIds) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(docIds, "Input document id list can not be null");
        try {
            return queue.submit(new GetDocumentsWithIdsCallable(docIds, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get documents with ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get documents with ids", e);
        }

        return null;

    }

    @Override
    public List<String> getPossibleAncestorRevisionIDs(final String docId,
                                                       final String revId,
                                                       final int limit) {
        try {
            return queue.submit(new GetPossibleAncestorRevIdsCallable(docId, revId, limit)).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
           throw new RuntimeException(e);
        }
    }

    @Override
    public LocalDocument getLocalDocument(final String docId) throws DocumentNotFoundException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new GetLocalDocumentCallable(docId)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get local document",e);
        } catch (ExecutionException e) {
            throw new DocumentNotFoundException(e);
        }
        return null;
    }

    @Override
    public LocalDocument insertLocalDocument(final String docId, final DocumentBody body) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        CouchUtils.validateDocumentId(docId);
        Preconditions.checkNotNull(body, "Input document body can not be null");
        try {
            return queue.submitTransaction(new InsertLocalDocumentCallable(docId, body)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to insert local document", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to insert local document",e);
            throw new DocumentException("Cannot insert local document",e);
        }

        return null;
    }

    @Override
    public void deleteLocalDocument(final String docId) throws DocumentNotFoundException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(docId),
                "Input document id can not be empty");

        try {
            queue.submit(new DeleteLocalDocumentCallable(docId)).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
           throw new DocumentNotFoundException(docId,null,e);
        }

    }

    @Override
    public String getPublicIdentifier() throws DatastoreException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        try {
            return queue.submit(new PublicIdentifierCallable()).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get public ID",e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get public ID", e);
            throw new DatastoreException("Failed to get public ID",e);
        }
    }


    @Override
    public void forceInsert(final BasicDocumentRevision rev,
                            final List<String> revisionHistory,
                            final Map<String, Object> attachments,
                            final Map<String[],List<PreparedAttachment>>preparedAttachments,
                            final boolean pullAttachmentsInline) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(rev, "Input document revision can not be null");
        Preconditions.checkNotNull(revisionHistory, "Input revision history must not be null");
        Preconditions.checkArgument(revisionHistory.size() > 0, "Input revision history must not be empty");
        Preconditions.checkArgument(checkRevisionIsInCorrectOrder(revisionHistory),
                "Revision history must be in right order.");
        CouchUtils.validateDocumentId(rev.getId());
        CouchUtils.validateRevisionId(rev.getRevision());

        logger.finer("forceInsert(): " + rev.toString() + ",\n" + JSONUtils.toPrettyJson
                (revisionHistory));

        try {
            Object event = queue.submitTransaction(new ForceInsertCallable(rev, revisionHistory,
                    attachments, pullAttachmentsInline, preparedAttachments, attachmentManager)).get();

            if(event != null) {
                eventBus.post(event);
            }


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new DocumentException(e);
        }

    }

    @Override
    public void forceInsert(BasicDocumentRevision rev, String... revisionHistory) throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        this.forceInsert(rev, Arrays.asList(revisionHistory), null, null, false);
    }

    private boolean checkRevisionIsInCorrectOrder(List<String> revisionHistory) {
        for (int i = 0; i < revisionHistory.size() - 1; i++) {
            CouchUtils.validateRevisionId(revisionHistory.get(i));
            int l = CouchUtils.generationFromRevId(revisionHistory.get(i));
            int m = CouchUtils.generationFromRevId(revisionHistory.get(i + 1));
            if (l >= m) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void compact() {
        try {
            queue.submit(new CompactDatabaseCallable(attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to compact database",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to compact database",e);
        }

    }

    @Override
    public void close() {
        queue.shutdown();
        eventBus.post(new DatabaseClosed(datastoreName));

    }

    boolean isOpen() {
        return !queue.isShutdown();
    }

    @Override
    public Map<String, Collection<String>> revsDiff(final Multimap<String, String> revisions) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkNotNull(revisions, "Input revisions must not be null");

        try {
            return queue.submit(new RevsDiffCallable(revisions)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to do revsdiff",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to do revsdiff",e);
        }

        return null;
    }

    @Override
    public String extensionDataFolder(String extensionName) {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(extensionName),
                "extension name can not be null or empty");
        return FilenameUtils.concat(this.extensionsDir, extensionName);
    }

    @Override
    public Iterator<String> getConflictedDocumentIds() {

        try {
            return queue.submit(new ConflictedDocumentIdsCallable()).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get conflicted document Ids",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get conflicted document Ids", e);
        }
        return null;

    }

    @Override
    public void resolveConflictsForDocument(final String docId, final ConflictResolver resolver)
            throws ConflictException {

        // before starting the tx, get the 'new winner' and see if we need to prepare its attachments

        Preconditions.checkState(this.isOpen(), "Database is closed");

        try {
            queue.submitTransaction(new ResolveConflictsCallable(docId, resolver, attachmentManager)).get();
        } catch (InterruptedException e) {
           logger.log(Level.SEVERE, "Failed to resolve conflicts", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to resolve Conflicts",e);
            if(e.getCause() !=null){
                if(e.getCause() instanceof  IllegalArgumentException){
                    throw (IllegalArgumentException)e.getCause();
                }
            }
        }

    }


    // this is just a facade into attachmentManager.PrepareAttachment for the sake of DatastoreWrapper
    @Override
    public PreparedAttachment prepareAttachment(Attachment att, long length, long encodedLength) throws AttachmentException {
        PreparedAttachment pa = attachmentManager.prepareAttachment(att, length, encodedLength);
        return pa;
    }
    
    @Override
    public Attachment getAttachment(final BasicDocumentRevision rev, final String attachmentName) {
        try {
            return queue.submit(new GetAttachmentCallable(rev, attachmentName, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to get attachment",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to get attachment",e);
        }

        return null;
    }

    @Override
    public List<? extends Attachment> attachmentsForRevision(final BasicDocumentRevision rev) throws AttachmentException {
        try {
            return queue.submit(new GetAttachmentsForRevisionCallable(rev, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to get attachments for revision");
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to get attachments for revision");
            throw new AttachmentException(e);
        }

    }


    @Override
    public EventBus getEventBus() {
        Preconditions.checkState(this.isOpen(), "Database is closed");
        return eventBus;
    }

    @Override
    public BasicDocumentRevision createDocumentFromRevision(final MutableDocumentRevision rev)
            throws DocumentException {
        Preconditions.checkNotNull(rev, "DocumentRevision can not be null");
        Preconditions.checkState(isOpen(), "Datastore is closed");
        final String docId;
        // create docid if docid is null
        if (rev.docId == null) {
            docId = CouchUtils.generateDocumentId();
        } else {
            docId = rev.docId;
        }
        final AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments =
                attachmentManager.prepareAttachments(rev.attachments != null ? rev.attachments.values() : null);
        BasicDocumentRevision created = null;
        try {
            created = queue.submitTransaction(new CreateDocumentCallable(docId, rev,
                    preparedAndSavedAttachments, attachmentManager)).get();
            return created;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to create document",e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE,"Failed to create document",e);
            throw new DocumentException(e);
        }finally {
            if(created != null){
                eventBus.post(new DocumentCreated(created));
            }
        }
        return null;

    }

    @Override
    public BasicDocumentRevision updateDocumentFromRevision(final MutableDocumentRevision rev)
            throws DocumentException {
        Preconditions.checkState(this.isOpen(), "Database is closed");

        final AttachmentManager.PreparedAndSavedAttachments preparedAndSavedAttachments =
                this.attachmentManager.prepareAttachments(rev.attachments != null ? rev.attachments.values() : null);

        try {
            BasicDocumentRevision revision = queue.submitTransaction(new UpdateDocumentCallable
                    (rev, preparedAndSavedAttachments, attachmentManager)).get();

            if (revision != null) {
                eventBus.post(new DocumentUpdated(getDocument(rev.docId,rev.sourceRevisionId),revision));
            }

            return revision;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to update document", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to updated document", e);
            throw new DocumentException(e);
        }

    }

    @Override
    public BasicDocumentRevision deleteDocumentFromRevision(final BasicDocumentRevision rev) throws ConflictException {
        Preconditions.checkNotNull(rev, "DocumentRevision can not be null");
        Preconditions.checkState(this.isOpen(), "Database is closed");

        try {
            BasicDocumentRevision deletedRevision = queue.submitTransaction(new
                    DeleteDocumentFromRevisionCallable(rev, attachmentManager)).get();


            if (deletedRevision != null) {
                eventBus.post(new DocumentDeleted(rev,deletedRevision));
            }

            return deletedRevision;
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to delete document", e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Failed to delete document", e);
            if(e.getCause() != null){
                if(e.getCause() instanceof ConflictException){
                    throw (ConflictException)e.getCause();
                }
            }
        }

        return null;
    }

    // delete all leaf nodes
    @Override
    public List<BasicDocumentRevision> deleteDocument(final String id)
            throws DocumentException  {
        Preconditions.checkNotNull(id, "id can not be null");
        Preconditions.checkState(this.isOpen(), "Database is closed");
        // to return

        try {
            return queue.submitTransaction(new DeleteDocumentCallable(id, attachmentManager)).get();
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE,"Failed to delete document",e);
        } catch (ExecutionException e) {
            throw new DocumentException("Failed to delete document",e);
        }

        return null;
    }

    <T> Future<T> runOnDbQueue(SQLQueueCallable<T> callable){
        return queue.submit(callable);
    }

}
