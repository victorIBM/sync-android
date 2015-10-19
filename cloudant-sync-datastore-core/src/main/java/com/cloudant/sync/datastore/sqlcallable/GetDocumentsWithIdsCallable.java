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
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.util.DatabaseUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class GetDocumentsWithIdsCallable extends DocumentsCallable<List<BasicDocumentRevision
        >> {
    private final List<String> docIds;
    private final Logger logger = Logger.getLogger(GetDocumentsWithIdsCallable.class.getCanonicalName());

    public GetDocumentsWithIdsCallable(List<String> docIds, AttachmentManager attachmentManager) {
        super(attachmentManager);
        this.docIds = docIds;
    }

    @Override
    public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
        String sql = String.format("SELECT " + FULL_DOCUMENT_COLS + " FROM revs, docs" +
                " WHERE docid IN ( %1$s ) AND current = 1 AND docs.doc_id = revs.doc_id " +
                " ORDER BY docs.doc_id ", DatabaseUtils.makePlaceholders(docIds.size()));
        String[] args = docIds.toArray(new String[docIds.size()]);
        List<BasicDocumentRevision> docs = getRevisionsFromRawQuery(db, sql, args);
        // Sort in memory since seems not able to sort them using SQL
        return sortDocumentsAccordingToIdList(docIds, docs);
    }

    private List<BasicDocumentRevision> sortDocumentsAccordingToIdList(List<String> docIds,
                                                                       List<BasicDocumentRevision> docs) {
        Map<String, BasicDocumentRevision> idToDocs = putDocsIntoMap(docs);
        List<BasicDocumentRevision> results = new ArrayList<BasicDocumentRevision>();
        for (String id : docIds) {
            if (idToDocs.containsKey(id)) {
                results.add(idToDocs.remove(id));
            } else {
                logger.fine("No document found for id: " + id);
            }
        }
        assert idToDocs.size() == 0;
        return results;
    }

    private Map<String, BasicDocumentRevision> putDocsIntoMap(List<BasicDocumentRevision> docs) {
        Map<String, BasicDocumentRevision> map = new HashMap<String, BasicDocumentRevision>();
        for (BasicDocumentRevision doc : docs) {
            // id should be unique cross all docs
            assert !map.containsKey(doc.getId());
            map.put(doc.getId(), doc);
        }
        return map;
    }
}
