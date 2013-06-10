package com.thoughtworks.hadoop.utils;

import org.apache.hadoop.mapred.ClusterStatus;

import java.util.ArrayList;
import java.util.Collection;

public class HMount implements HCommand {
    private int portNumber;
    private String jobTrackerAddress;
    private ClusterStatus clusterStatus;
    private ClusterClient clusterClient;


    public HMount(ClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }


    public Collection<String> getTaskTrackerNames() {
        Collection<String> taskTrackerNames = clusterClient.getTaskTrackerNames();
        ArrayList<String> taskTrackers = new ArrayList<String>();
        for (String ttName : taskTrackerNames) {
            taskTrackers.add(this.getHostNameFromTaskTrackerName(ttName));
        }
        return taskTrackers;
    }

    public String getHostNameFromTaskTrackerName(String taskTrackerName) {
        return HostnameParser.fromTaskTrackerName(taskTrackerName);
    }


    @Override
    public boolean wasSuccessful() {
        return false;
    }
}
