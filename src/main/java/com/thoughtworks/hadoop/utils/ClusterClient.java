package com.thoughtworks.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ClusterClient {
    private String jobTrackerAddress;
    private String nameNodeAddress;
    private int jtPortNumber;
    private int nnPortNumber;
    private JobClient jobClient;
    private DFSClient dfsClient;

    public ClusterClient(String jobTrackerAddress, int jtPortNumber, String nameNodeAddress, int nnPortNumber) {
        this.jobTrackerAddress = jobTrackerAddress;
        this.jtPortNumber = jtPortNumber;
        this.nameNodeAddress= nameNodeAddress;
        this.nnPortNumber = nnPortNumber;
        this.jobClient = getJobClient();
        this.dfsClient = getDFSClient();
    }


    private JobClient getJobClient() {
        try {
            return new JobClient(new InetSocketAddress(this.jobTrackerAddress, this.jtPortNumber), new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DFSClient getDFSClient() {
        try {
            return new DFSClient(new InetSocketAddress(this.nameNodeAddress, this.nnPortNumber), new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<String> getTaskTrackerNames() {
        ClusterStatus clusterStatus = null;
        try {
            clusterStatus = jobClient.getClusterStatus(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return clusterStatus.getActiveTrackerNames();
    }

    public String getJobTrackerName() {
        return jobTrackerAddress;
    }

    public String getNameNodeName() {
        return nameNodeAddress;
    }

    public Collection<String> getDataNodes() {
        DatanodeInfo[] datanodeInfos = new DatanodeInfo[0];
        List<String> dataNodes = new ArrayList<String>();
        try {
            datanodeInfos = dfsClient.datanodeReport(FSConstants.DatanodeReportType.ALL);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (DatanodeInfo info : datanodeInfos) {
            dataNodes.add(info.getHostName());
        }
        return dataNodes;

    }

}

