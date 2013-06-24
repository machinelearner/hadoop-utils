package com.thoughtworks.hadoop.utils;

public class ClusterConfiguration {

    private final String jobTrackerAddress;
    private final int jtPortNumber;
    private final String nameNodeAddress;
    private final int nnPortNumber;

    public ClusterConfiguration(String jobTrackerAddress, int jtPortNumber,
                                String nameNodeAddress, int nnPortNumber) {
        this.jobTrackerAddress = jobTrackerAddress;
        this.jtPortNumber = jtPortNumber;
        this.nameNodeAddress = nameNodeAddress;
        this.nnPortNumber = nnPortNumber;
    }

    public String getJobTrackerAddress() {
        return jobTrackerAddress;
    }

    public int getJtPortNumber() {
        return jtPortNumber;
    }

    public String getNameNodeAddress() {
        return nameNodeAddress;
    }

    public int getNnPortNumber() {
        return nnPortNumber;
    }
}
