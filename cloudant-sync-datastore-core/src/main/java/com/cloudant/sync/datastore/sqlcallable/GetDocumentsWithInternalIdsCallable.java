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
}
