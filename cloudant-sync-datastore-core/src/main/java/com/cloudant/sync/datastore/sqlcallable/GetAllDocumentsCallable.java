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
import com.cloudant.sync.sqlite.SQLQueueCallable;

import java.util.List;

/**
 * Created by mike on 17/10/2015.
 */
public class GetAllDocumentsCallable extends SQLQueueCallable<List<BasicDocumentRevision>> {
    private final boolean descending;
    private final int limit;
    private final int offset;
    private final AttachmentManager attachmentManager;

    public GetAllDocumentsCallable(boolean descending, int limit, int offset, AttachmentManager
            attachmentManager) {
        this.descending = descending;
        this.limit = limit;
        this.offset = offset;
        this.attachmentManager = attachmentManager;
    }

    @Override
    public List<BasicDocumentRevision> call(SQLDatabase db) throws Exception {
        // Generate the SELECT statement, based on the options:
        String sql = String.format("SELECT " + SqlDocumentUtils.FULL_DOCUMENT_COLS+
                        " FROM revs, docs " +
                        "WHERE deleted = 0 AND current = 1 AND docs.doc_id = revs.doc_id " +
                        "ORDER BY docs.doc_id %1$s, revid DESC LIMIT %2$s OFFSET %3$s ",
                (descending ? "DESC" : "ASC"), limit, offset);
        return SqlDocumentUtils.getRevisionsFromRawQuery(db, sql, new String[]{},
                attachmentManager);
    }
}
