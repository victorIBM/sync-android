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

import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.sqlite.Cursor;
import com.cloudant.sync.sqlite.SQLDatabase;
import com.cloudant.sync.sqlite.SQLQueueCallable;
import com.cloudant.sync.util.DatabaseUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by mike on 17/10/2015.
 */
public class RevsDiffCallable extends SQLQueueCallable<Map<String, Collection<String>>> {
    private final Multimap<String, String> revisions;

    public RevsDiffCallable(Multimap<String, String> revisions) {
        this.revisions = revisions;
    }

    @Override
    public Map<String, Collection<String>> call(SQLDatabase db) throws Exception {
        Multimap<String, String> missingRevs = ArrayListMultimap.create();
        // Break the potentially big multimap into small ones so for each map,
        // a single query can be use to check if the <id, revision> pairs in sqlDb or not
        List<Multimap<String, String>> batches =
                multiMapPartitions(revisions, SqlConstants.SQLITE_QUERY_PLACEHOLDERS_LIMIT);
        for (Multimap<String, String> batch : batches) {
            revsDiffBatch(db, batch);
            missingRevs.putAll(batch);
        }
        return missingRevs.asMap();
    }

    List<Multimap<String, String>> multiMapPartitions(
            Multimap<String, String> revisions, int size) {

        List<Multimap<String, String>> partitions = new ArrayList<Multimap<String, String>>();
        Multimap<String, String> current = HashMultimap.create();
        for (Map.Entry<String, String> e : revisions.entries()) {
            current.put(e.getKey(), e.getValue());
            // the query uses below (see revsDiffBatch())
            // `multimap.size() + multimap.keySet().size()` placeholders
            // and SQLite has limit on the number of placeholders on a single query.
            if (current.size() + current.keySet().size() >= size) {
                partitions.add(current);
                current = HashMultimap.create();
            }
        }

        if (current.size() > 0) {
            partitions.add(current);
        }

        return partitions;
    }

    /**
     * Removes revisions present in the datastore from the input map.
     *
     * @param revisions an multimap from document id to set of revisions. The
     *                  map is modified in place for performance consideration.
     */
    void revsDiffBatch(SQLDatabase db, Multimap<String, String> revisions) throws DatastoreException {

        final String sql = String.format(
                "SELECT docs.docid, revs.revid FROM docs, revs " +
                        "WHERE docs.doc_id = revs.doc_id AND docs.docid IN (%s) AND revs.revid IN (%s) " +
                        "ORDER BY docs.docid",
                DatabaseUtils.makePlaceholders(revisions.keySet().size()),
                DatabaseUtils.makePlaceholders(revisions.size()));

        String[] args = new String[revisions.keySet().size() + revisions.size()];
        String[] keys = revisions.keySet().toArray(new String[revisions.keySet().size()]);
        String[] values = revisions.values().toArray(new String[revisions.size()]);
        System.arraycopy(keys, 0, args, 0, revisions.keySet().size());
        System.arraycopy(values, 0, args, revisions.keySet().size(), revisions.size());

        Cursor cursor = null;
        try {
            cursor = db.rawQuery(sql, args);
            while (cursor.moveToNext()) {
                String docId = cursor.getString(0);
                String revId = cursor.getString(1);
                revisions.remove(docId, revId);
            }
        } catch (SQLException e) {
            throw new DatastoreException(e);
        } finally {
            DatabaseUtils.closeCursorQuietly(cursor);
        }
    }
}
