package com.cloudant.todo;

import com.cloudant.sync.replication.WifiPeriodicReplicationReceiver;

public class MyWifiPeriodicReplicationReceiver extends WifiPeriodicReplicationReceiver<MyReplicationService> {

    public MyWifiPeriodicReplicationReceiver() {
        super(MyReplicationService.class);
    }

}
