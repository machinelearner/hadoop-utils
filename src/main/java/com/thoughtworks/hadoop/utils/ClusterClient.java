package com.thoughtworks.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

public class ClusterClient {
    private String jobTrackerAddress;
    private int portNumber;
    private final JobClient jobClient;

    public ClusterClient(String jobTrackerAddress, int portNumber) {
        this.jobTrackerAddress = jobTrackerAddress;
        this.portNumber = portNumber;

        try {
            jobClient = new JobClient(new InetSocketAddress(this.jobTrackerAddress, this.portNumber), new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public Collection<String> getTaskTrackerNames() {
        ClusterStatus clusterStatus = null;
        try {
            clusterStatus = jobClient.getClusterStatus();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return clusterStatus.getActiveTrackerNames();
    }

    public String getJobTrackerName() {
        return jobTrackerAddress;
    }
}

