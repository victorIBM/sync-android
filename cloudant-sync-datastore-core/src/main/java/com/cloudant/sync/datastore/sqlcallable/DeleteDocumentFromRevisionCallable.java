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

/**
 * Callable to delete document with given revision
 */
public class DeleteDocumentFromRevisionCallable extends DocumentsCallable
            <BasicDocumentRevision> {
    private final BasicDocumentRevision rev;

    public DeleteDocumentFromRevisionCallable(BasicDocumentRevision rev, AttachmentManager
            attachmentManager) {
        super(attachmentManager);
        this.rev = rev;
    }

    @Override
    public BasicDocumentRevision call(SQLDatabase db) throws Exception {
        return deleteDocumentInQueue(db, rev.getId(), rev.getRevision());
    }
}
