package com.thoughtworks.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ClusterClient {
    public static final String INVALID_JOB_ID = "Invalid Job Id";
    private String jobTrackerAddress;
    private String nameNodeAddress;
    private int jtPortNumber;
    private int nnPortNumber;
    private JobClient jobClient;
    private DFSClient dfsClient;

    public ClusterClient(String jobTrackerAddress, int jtPortNumber, String nameNodeAddress, int nnPortNumber) {
        this.jobTrackerAddress = jobTrackerAddress;
        this.jtPortNumber = jtPortNumber;
        this.nameNodeAddress = nameNodeAddress;
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

    public int getJtPortNumber() {
        return jtPortNumber;
    }

    public int getNnPortNumber() {
        return nnPortNumber;
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

    public Collection<JobDetail> getAllJobDetails() {
        Collection<JobDetail> jobDetails = new ArrayList<JobDetail>();
        JobStatus[] jobStatuses = new JobStatus[0];
        try {
            jobStatuses = jobClient.getAllJobs();
            for (JobStatus jobStatus : jobStatuses) {
                RunningJob job = jobClient.getJob(jobStatus.getJobID());
                jobDetails.add(JobDetail.create(job, jobStatus));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return jobDetails;

    }

    public Collection<JobDetail> getRunningJobDetails() {
        Collection<JobDetail> jobDetails = new ArrayList<JobDetail>();
        JobStatus[] jobStatuses = new JobStatus[0];
        try {
            jobStatuses = jobClient.getAllJobs();
            for (JobStatus jobStatus : jobStatuses) {
                RunningJob job = jobClient.getJob(jobStatus.getJobID());
                if (!job.isComplete()) {
                    jobDetails.add(JobDetail.create(job, jobStatus));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return jobDetails;
    }

    public Collection<JobDetail> getRetiredJobDetails() {
        // SCREEN Scrape for results!! As there is no way to figure out the mapred configuration for the running JT
        return getCompletedJobDetails();
    }

    public Collection<JobDetail> getCompletedJobDetails() {
        Collection<JobDetail> jobDetails = new ArrayList<JobDetail>();
        JobStatus[] jobStatuses = new JobStatus[0];
        try {
            jobStatuses = jobClient.getAllJobs();
            for (JobStatus jobStatus : jobStatuses) {
                RunningJob job = jobClient.getJob(jobStatus.getJobID());
                if (job.isComplete()) {
                    jobDetails.add(JobDetail.create(job, jobStatus));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return jobDetails;
    }

    public RunningJob getJob(JobID jobID) {
        try {
            return jobClient.getJob((org.apache.hadoop.mapred.JobID) jobID);
        } catch (IOException e) {
            throw new RuntimeException(INVALID_JOB_ID);
        }
    }

    public void killJob(JobID jobID) throws IOException {
        RunningJob job = jobClient.getJob((org.apache.hadoop.mapred.JobID) jobID);
        if (job == null) {
            throw new RuntimeException(INVALID_JOB_ID);
        }
        job.killJob();
    }
}

