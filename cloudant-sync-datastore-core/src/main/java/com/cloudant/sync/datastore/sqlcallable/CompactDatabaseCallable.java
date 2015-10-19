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

import com.cloudant.sync.sqlite.ContentValues;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;

import java.util.logging.Logger;

/**
 * Created by mike on 17/10/2015.
 */
public class CompactDatabaseCallable extends SQLQueueCallable<Object> {

    private final AttachmentManager attachmentManager;
    private final Logger logger = Logger.getLogger(CompactDatabaseCallable.class.getCanonicalName());

    public CompactDatabaseCallable(AttachmentManager attachmentManager) {
        this.attachmentManager = attachmentManager;
    }

    @Override
    public Object call(SQLDatabase db) {
        logger.finer("Deleting JSON of old revisions...");
        ContentValues args = new ContentValues();
        args.put("json", (String) null);
        int i = db.update("revs", args, "current=0", null);

        logger.finer("Deleting old attachments...");
        attachmentManager.purgeAttachments(db);
        logger.finer("Vacuuming SQLite database...");
        db.compactDatabase();
        return null;
    }
}
