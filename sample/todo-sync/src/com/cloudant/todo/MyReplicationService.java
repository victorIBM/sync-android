package com.cloudant.todo;

import android.content.Context;
import android.content.SharedPreferences;
import android.preference.PreferenceManager;
import android.util.Log;

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DatastoreNotCreatedException;
import com.cloudant.sync.replication.WifiPeriodicReplicationReceiver;
import com.cloudant.sync.replication.PeriodicReplicationService;
import com.cloudant.sync.replication.Replicator;
import com.cloudant.sync.replication.ReplicatorBuilder;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class MyReplicationService extends PeriodicReplicationService {

    public static int PUSH_REPLICATION_ID = 0;
    public static int PULL_REPLICATION_ID = 1;

    private static final String TAG = "MyReplicationService";

    private static final String SETTINGS_CLOUDANT_USER = "pref_key_username";
    private static final String SETTINGS_CLOUDANT_DB = "pref_key_dbname";
    private static final String SETTINGS_CLOUDANT_API_KEY = "pref_key_api_key";
    private static final String SETTINGS_CLOUDANT_API_SECRET = "pref_key_api_password";
    private static final String TASKS_DATASTORE_NAME = "tasks";
    private static final String DATASTORE_MANGER_DIR = "data";

    public MyReplicationService() {
        super(MyWifiPeriodicReplicationReceiver.class);
    }

    @Override
    public void onCreate() {
        super.onCreate();

        // If we had to get the credentials for replication from a remote server before we
        // could configure the replicators, we could do it using an AsyncTask, something like
        // this:
        //
        // new AsyncTask<Void, Void, Replicator[]>() {
        //     @Override
        //     protected Replicator[] doInBackground(Void... params) {
        //         // Fetch the credentials needed for replication and set up the replicators.
        //         // ...
        //
        //         // Return the array of replicators you have configured.
        //         return replicators;
        //     }
        //
        //     @Override
        //     protected void onPostExecute(Replicator[] replicators) {
        //         setReplicators(replicators);
        //     }
        //
        // }.execute();

        // In this particular case as we don't need to get the credentials for replication from
        // a remote server, so we can just call setReplicators() here and omit the AsyncTask.
        setReplicators(getReplicators());
    }

    protected Replicator[] getReplicators() {
        try {
            Context context = getApplicationContext();
            URI uri = createServerURI(context);

            File path = context.getApplicationContext().getDir(
                DATASTORE_MANGER_DIR,
                Context.MODE_PRIVATE
            );

            DatastoreManager manager = new DatastoreManager(path.getAbsolutePath());
            Datastore datastore = null;
            try {
                datastore = manager.openDatastore(TASKS_DATASTORE_NAME);
            } catch (DatastoreNotCreatedException dnce) {
                Log.e(TAG, "Unable to open Datastore", dnce);
            }

            Replicator pullReplicator = ReplicatorBuilder.pull().from(uri).to(datastore).withId(PULL_REPLICATION_ID).build();
            Replicator pushReplicator = ReplicatorBuilder.push().to(uri).from(datastore).withId(PUSH_REPLICATION_ID).build();

            return new Replicator[]{pullReplicator, pushReplicator};
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected int getBoundIntervalInSeconds() {
        return 20;
    }

    @Override
    protected int getUnboundIntervalInSeconds() {
        return (int)TimeUnit.SECONDS.convert(5l, TimeUnit.MINUTES);
    }

    @Override
    protected boolean startReplicationOnBind() {
        // Trigger replications when a client binds to the service only if we're on WiFi.
        return WifiPeriodicReplicationReceiver.isConnectedToWifi(this);
    }

    private URI createServerURI(Context context)
        throws URISyntaxException {
        // We store this in plain text for the purposes of simple demonstration,
        // you might want to use something more secure.
        SharedPreferences sharedPref = PreferenceManager.getDefaultSharedPreferences(context);
        String username = sharedPref.getString(SETTINGS_CLOUDANT_USER, "");
        String dbName = sharedPref.getString(SETTINGS_CLOUDANT_DB, "");
        String apiKey = sharedPref.getString(SETTINGS_CLOUDANT_API_KEY, "");
        String apiSecret = sharedPref.getString(SETTINGS_CLOUDANT_API_SECRET, "");
        String host = username + ".cloudant.com";

        // We recommend always using HTTPS to talk to Cloudant.
        return new URI("https", apiKey + ":" + apiSecret, host, 443, "/" + dbName, null, null);
    }
}
