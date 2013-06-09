package com.thoughtworks.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class HMount {
    private int portNumber;
    private String jobTrackerAddress;
    private ClusterStatus clusterStatus;

    public HMount(String jobTrackerAddress, int portNumber) throws IOException {
        this.jobTrackerAddress = jobTrackerAddress;
        this.portNumber = portNumber;

        JobClient jobClient = new JobClient(new InetSocketAddress(this.jobTrackerAddress, this.portNumber), new Configuration());
        this.clusterStatus = jobClient.getClusterStatus(true);

    }

    public Collection<String> getTaskTrackerNames() {
        Collection<String> listOfTaskTrackers;
        listOfTaskTrackers = this.clusterStatus.getActiveTrackerNames();
        return listOfTaskTrackers;
    }

    public Collection<String> getTaskTrackerHostnames() {
        Collection<String> taskTrackersNames,taskTrackersHostnames = new ArrayList<String>();
        taskTrackersNames = this.getTaskTrackerNames();

        for (String ttName : taskTrackersNames) {
            taskTrackersHostnames.add(this.getHostNameFromTaskTrackerName(ttName));
        }
        return taskTrackersHostnames;
    }

    public String getHostNameFromTaskTrackerName(String taskTrackerName) {
        return HostnameBuilder.extractHostNameFromTaskTrackerName(taskTrackerName);
    }
}
