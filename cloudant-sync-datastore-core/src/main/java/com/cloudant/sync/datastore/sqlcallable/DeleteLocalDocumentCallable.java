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

import com.cloudant.sync.datastore.DocumentNotFoundException;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;

/**
 * Created by mike on 17/10/2015.
 */
public class DeleteLocalDocumentCallable extends SQLQueueCallable<Object> {
    private final String docId;

    public DeleteLocalDocumentCallable(String docId) {
        this.docId = docId;
    }

    @Override
    public Object call(SQLDatabase db) throws Exception {
        String[] whereArgs = {docId};
        int rowsDeleted = db.delete("localdocs", "docid=? ", whereArgs);
        if (rowsDeleted == 0) {
            throw new DocumentNotFoundException(docId, (String) null);
        }
        return null;
    }
}
