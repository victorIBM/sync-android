/*
 *  Copyright (c) 2016 IBM Corp. All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *   License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 */

package com.cloudant.sync.replication;

import com.cloudant.sync.datastore.DocumentRevision;

/**
 * Created by Rhys Short on 31/03/2016.
 */
public interface PushFilter {

    /**
     * Determines if a DocumentRevision should be replicated to the remote database
     * @param revision The revision to be replication
     * @return true if the DocumentRevision should be replicated.
     */
    boolean shouldReplicateDocument(DocumentRevision revision);

}
