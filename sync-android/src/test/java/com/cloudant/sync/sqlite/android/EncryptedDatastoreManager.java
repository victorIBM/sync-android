/**
 * Original iOS version by  Jens Alfke, ported to Android by Marty Schoch
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Modifications for this distribution by Cloudant, Inc., Copyright (c) 2013 Cloudant, Inc.
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

package com.cloudant.sync.sqlite.android;

import com.cloudant.android.encryption.KeyProvider;
import com.cloudant.sync.datastore.BasicDatastore;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreException;
import com.cloudant.sync.datastore.DatastoreNotCreatedException;
import com.cloudant.sync.notifications.DatabaseClosed;
import com.cloudant.sync.notifications.DatabaseCreated;
import com.cloudant.sync.notifications.DatabaseOpened;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * <p>Manages a set of {@link com.cloudant.sync.datastore.Datastore} objects, with their underlying disk
 * storage residing in a given directory.</p>
 *
 * <p>In general, a directory used for storing datastores -- that is, managed
 * by this manager -- shouldn't be used for storing other data. The manager
 * object assumes all data within is managed by itself, so adding other files
 * to the directory may cause them to be deleted.</p>
 *
 * <p>A datastore's on-disk representation is a single file containing
 * all its data. In future, there may be other files and folders per
 * datastore.</p>
 */
public class EncryptedDatastoreManager {

    private final static String LOG_TAG = "EncryptedDatastoreManager";
    private final static Logger logger = Logger.getLogger(EncryptedDatastoreManager.class.getCanonicalName());

    private final String path;

    private final Map<String, Datastore> openedDatastores = Collections.synchronizedMap(new HashMap<String, Datastore>());

    /**
     * The regex used to validate a datastore name, {@value}.
     */
    protected static final String LEGAL_CHARACTERS = "^[a-zA-Z]+[a-zA-Z0-9_\\Q-$()/\\E]*";

    private final EventBus eventBus = new EventBus();

    /**
     * <p>Constructs a {@code DatastoreManager} to manage a directory.</p>
     *
     * <p>Datastores are created within the {@code directoryPath} directory.
     * In general, this folder should be under the control of, and only used
     * by, a single {@code DatastoreManager} object at any time.</p>
     *
     * @param directoryPath root directory to manage
     *
     * @see EncryptedDatastoreManager#EncryptedDatastoreManager(java.io.File)
     */
    public EncryptedDatastoreManager(String directoryPath) {
        this(new File(directoryPath));
    }

    /**
     * <p>Constructs a {@code DatastoreManager} to manage a directory.</p>
     *
     * <p>Datastores are created within the {@code directoryPath} directory.
     * In general, this folder should be under the control of, and only used
     * by, a single {@code DatastoreManager} object at any time.</p>
     *
     * @param directoryPath root directory to manage
     *
     * @throws IllegalArgumentException if the {@code directoryPath} is not a
     *          directory or isn't writable.
     */
    public EncryptedDatastoreManager(File directoryPath) {
        logger.fine("Datastore path: " + directoryPath);
        if(!directoryPath.isDirectory() ) {
            throw new IllegalArgumentException("Input path is not a valid directory");
        } else if(!directoryPath.canWrite()) {
            throw new IllegalArgumentException("Datastore directory is not writable");
        }
        this.path = directoryPath.getAbsolutePath();
    }

    /**
     * Lists all the names of {@link com.cloudant.sync.datastore.Datastore Datastores} managed by this DatastoreManager
     *
     * @return List of {@link com.cloudant.sync.datastore.Datastore Datastores} names.
     */
    public List<String> listAllDatastores() {
        List<String> datastores = new ArrayList<String>();
        File dsManagerDir = new File(this.path);
        for(File file:dsManagerDir.listFiles()){
            boolean isStore = file.isDirectory() && new File(file, "db.sync").isFile();
            if (isStore) {
                //replace . with a slash, on disk / are replaced with dots
                datastores.add(file.getName().replace(".", "/"));
            }
        }

        return datastores;
    }

    /**
     * <p>Returns the path to the directory this object manages.</p>
     * @return the absolute path to the directory this object manages.
     */
    public String getPath() {
        return path;
    }

    /**
     * <p>Opens a datastore that requires SQLCipher encryption.
     * Key provider object contains the user defined SQLCipher key.</p>
     *
     * <p>This method finds the appropriate datastore file for a
     * datastore, then initialises a {@link com.cloudant.sync.datastore.Datastore} object connected
     * to that underlying storage file.</p>
     *
     * <p>If the datastore was successfully created and opened, a
     * {@link com.cloudant.sync.notifications.DatabaseOpened DatabaseOpened}
     * event is posted on the event bus.</p>
     *
     * @param dbName name of datastore to open
     * @return {@code Datastore} with the given name
     *
     * @see EncryptedDatastoreManager#getEventBus()
     */
    public Datastore openDatastore(String dbName, KeyProvider provider) throws DatastoreNotCreatedException {
        Preconditions.checkArgument(dbName.matches(LEGAL_CHARACTERS),
                "A database must be named with all lowercase letters (a-z), digits (0-9),"
                        + " or any of the _$()+-/ characters. The name has to start with a"
                        + " lowercase letter (a-z).");
        if (!openedDatastores.containsKey(dbName)) {
            synchronized (openedDatastores) {
                if (!openedDatastores.containsKey(dbName)) {
                    Datastore ds = createDatastore(dbName, provider);
                    ds.getEventBus().register(this);
                    openedDatastores.put(dbName, ds);
                }
            }
        }
        return openedDatastores.get(dbName);
    }

    private Datastore createDatastore(String dbName) throws DatastoreNotCreatedException {
        try {
            String dbDirectory = this.getDatastoreDirectory(dbName);
            boolean dbDirectoryExist = new File(dbDirectory).exists();
            logger.info("path: " + this.path);
            logger.info("dbDirectory: " + dbDirectory);
            logger.info("dbDirectoryExist: " + dbDirectoryExist);
            // dbDirectory will created in BasicDatastore constructor
            // if it does not exist
            BasicDatastore ds = new BasicDatastore(dbDirectory, dbName);
            if(!dbDirectoryExist) {
                this.eventBus.post(new DatabaseCreated(dbName));
            }
            eventBus.post(new DatabaseOpened(dbName));
            return ds;
        } catch (IOException e) {
            throw new DatastoreNotCreatedException("Database not found: " + dbName, e);
        } catch (SQLException e) {
            throw new DatastoreNotCreatedException("Database not initialized correctly: " + dbName, e);
        } catch (DatastoreException e){
            throw new DatastoreNotCreatedException("Datastore not initialized correctly: " + dbName, e);
        }
    }

    /**
     * Creates a datastore that requires SQLCipher encryption.
     */
    private Datastore createDatastore(String dbName, KeyProvider provider) throws DatastoreNotCreatedException {
        try {
            String dbDirectory = this.getDatastoreDirectory(dbName);
            boolean dbDirectoryExist = new File(dbDirectory).exists();
            logger.info("path: " + this.path);
            logger.info("dbDirectory: " + dbDirectory);
            logger.info("dbDirectoryExist: " + dbDirectoryExist);
            // dbDirectory will created in BasicDatastore constructor
            // if it does not exist

            //Pass database directory, database name, and SQLCipher passphrase
            BasicDatastore ds = new BasicDatastore(dbDirectory, dbName, provider);
            if(!dbDirectoryExist) {
                this.eventBus.post(new DatabaseCreated(dbName));
            }
            eventBus.post(new DatabaseOpened(dbName));
            return ds;
        } catch (IOException e) {
            throw new DatastoreNotCreatedException("Database not found: " + dbName, e);
        } catch (SQLException e) {
            throw new DatastoreNotCreatedException("Database not initialized correctly: " + dbName, e);
        }
    }

    private String getDatastoreDirectory(String dbName) {
        return FilenameUtils.concat(this.path, dbName.replace("/","."));
    }

    /**
     * <p>Returns the EventBus which this DatastoreManager posts
     * {@link com.cloudant.sync.notifications.DatabaseModified Database Notification Events} to.</p>
     * @return the DatastoreManager's EventBus
     *
     * @see <a href="https://code.google.com/p/guava-libraries/wiki/EventBusExplained">Google Guava EventBus documentation</a>
     */
    public EventBus getEventBus() {
        return eventBus;
    }

    @Subscribe
    public void onDatabaseClosed(DatabaseClosed databaseClosed) {
        synchronized (openedDatastores) {
            this.openedDatastores.remove(databaseClosed.dbName);
        }
        this.eventBus.post(databaseClosed);
    }
}
