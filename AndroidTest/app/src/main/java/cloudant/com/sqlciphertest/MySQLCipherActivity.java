/**
 * Copyright (c) 2013 Cloudant, Inc. All rights reserved.
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
package cloudant.com.sqlciphertest;

import android.app.Activity;
import android.os.Bundle;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.cloudant.sync.datastore.AttachmentException;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DatastoreNotCreatedException;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentException;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.MutableDocumentRevision;

import org.json.JSONException;

public class MySQLCipherActivity extends Activity {

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        try {
            CreateSQLCipherDatabase();
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DatastoreNotCreatedException e) {
            e.printStackTrace();
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a datastore manager and datastore using a sqlcipher based sqlite database.
     * Once the database is created, create a document and read the new documented.
     * @throws IOException
     * @throws JSONException
     * @throws DatastoreNotCreatedException
     * @throws DocumentException
     */
    private void CreateSQLCipherDatabase() throws IOException, JSONException, DatastoreNotCreatedException,
            DocumentException {

        //SQLiteDatabase.loadLibs(this);
        // Create a DatastoreManager using application internal storage path
        File path = getApplicationContext().getDir("datastores", MODE_PRIVATE);
        DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());

        //Pass datastore name and test passphrase for SQLCipher
        Datastore ds = manager.openDatastore("sqlcipher_datastore", "SQLCipherPassphrase");
        //Datastore ds = manager.openDatastore("sqlcipher_datastore");

        // Create a document
        MutableDocumentRevision rev = new MutableDocumentRevision();

        // Build up body content from a Map
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("Name", "Cloudant");
        json.put("Location", "Boston");
        rev.body = DocumentBodyFactory.create(json);

        DocumentRevision revision = ds.createDocumentFromRevision(rev);


        // Read a document
        DocumentRevision aRevision = ds.getDocument(revision.getId());

        // Print out aRevision
        System.out.println(aRevision.getBody());

    }


}
