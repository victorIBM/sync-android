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
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by mike on 17/10/2015.
 */
public class GetDocumentsWithInternalIdsCallable extends DocumentsCallable<List<BasicDocumentRevision>> {
    private final List<Long> docIds;

    public GetDocumentsWithInternalIdsCallable(List<Long> docIds, AttachmentManager
            attachmentManager) {
        super(attachmentManager);
        this.docIds = docIds;
    }

    @Override
    public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
        return getDocumentsWithInternalIdsInQueue(db, docIds);
    }

    List<BasicDocumentRevision> getDocumentsWithInternalIdsInQueue(SQLDatabase db,
                                                                   final List<Long> docIds)
            throws AttachmentException, DocumentNotFoundException, DocumentException, DatastoreException {

        if (docIds.size() == 0) {
            return Collections.emptyList();
        }

        final String GET_DOCUMENTS_BY_INTERNAL_IDS = "SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs " +
                "WHERE revs.doc_id IN ( %s ) AND current = 1 AND docs.doc_id = revs.doc_id";

        // Split into batches because SQLite has a limit on the number
        // of placeholders we can use in a single query. 999 is the default
        // value, but it can be lower. It's hard to find this out from Java,
        // so we use a value much lower.
        List<BasicDocumentRevision> result = new ArrayList<BasicDocumentRevision>(docIds.size());

        List<List<Long>> batches = Lists.partition(docIds, SqlConstants
                .SQLITE_QUERY_PLACEHOLDERS_LIMIT);
        for (List<Long> batch : batches) {
            String sql = String.format(
                    GET_DOCUMENTS_BY_INTERNAL_IDS,
                    DatabaseUtils.makePlaceholders(batch.size())
            );
            String[] args = new String[batch.size()];
            for (int i = 0; i < batch.size(); i++) {
                args[i] = Long.toString(batch.get(i));
            }
            result.addAll(getRevisionsFromRawQuery(db, sql, args, attachmentManager));
        }

        // Contract is to sort by sequence number, which we need to do
        // outside the sqlDb as we're batching requests.
        Collections.sort(result, new Comparator<BasicDocumentRevision>() {
            @Override
            public int compare(BasicDocumentRevision documentRevision, BasicDocumentRevision documentRevision2) {
                long a = documentRevision.getSequence();
                long b = documentRevision2.getSequence();
                return (int) (a - b);
            }
        });

        return result;
    }
}
