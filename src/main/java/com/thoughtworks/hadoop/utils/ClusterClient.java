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
    private JobClient jobClient;
    private DFSClient dfsClient;
    private ClusterConfiguration configuration;

    public ClusterClient(ClusterConfiguration configuration) {
        this.configuration = configuration;
        this.jobClient = getJobClient();
        this.dfsClient = getDFSClient();
    }

    private JobClient getJobClient() {
        try {
            return new JobClient(new InetSocketAddress(configuration.getJobTrackerAddress(),
                    configuration.getJtPortNumber()), new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DFSClient getDFSClient() {
        try {
            return new DFSClient(new InetSocketAddress(configuration.getNameNodeAddress(), configuration.getNnPortNumber()), new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<String> getTaskTrackerNames() {
        ClusterStatus clusterStatus;
        try {
            clusterStatus = jobClient.getClusterStatus(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return clusterStatus.getActiveTrackerNames();
    }

    public String getJobTrackerName() {
        return configuration.getJobTrackerAddress();
    }

    public String getNameNodeName() {
        return configuration.getNameNodeAddress();
    }

    public int getJtPortNumber() {
        return configuration.getJtPortNumber();
    }

    public int getNnPortNumber() {
        return configuration.getNnPortNumber();
    }


    public Collection<String> getDataNodes() {
        DatanodeInfo[] dataNodeInfoList;
        List<String> dataNodes = new ArrayList<String>();
        try {
            dataNodeInfoList = dfsClient.datanodeReport(FSConstants.DatanodeReportType.ALL);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (DatanodeInfo info : dataNodeInfoList) {
            dataNodes.add(info.getHostName());
        }
        return dataNodes;

    }

    public JobDetails getAllJobDetails() {
        JobDetails jobDetails = new JobDetails();
        JobStatus[] jobStatuses;
        try {
            jobStatuses = jobClient.getAllJobs();
            for (JobStatus jobStatus : jobStatuses) {
                RunningJob job = jobClient.getJob(jobStatus.getJobID());
                jobDetails.addDetail(JobDetail.create(job, jobStatus));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return jobDetails;

    }

    public Collection<JobDetail> getRunningJobDetails() {
        Collection<JobDetail> jobDetails = new ArrayList<JobDetail>();
        JobStatus[] jobStatuses;
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
        JobStatus[] jobStatuses;
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

    public void killJob(JobID jobID) throws IOException {
        RunningJob job = jobClient.getJob((org.apache.hadoop.mapred.JobID) jobID);
        if (job == null) {
            throw new RuntimeException(INVALID_JOB_ID);
        }
        job.killJob();

    }

    public void killTask(String taskAttemptId) throws Exception {
        // Bad API version; Cannot do anything better; Les Horribles
        String[] args = {"job", "-kill-task", taskAttemptId};
        jobClient.run(args);
    }

    public TaskDetails getAllTaskDetails(String jobId) {
        String mapTaskURL = getJobTrackerURL() + "/jobfailures.jsp?jobid=" + jobId + "&kind=map";
        String reduceTaskURL = getJobTrackerURL() + "/jobfailures.jsp?jobid=" + jobId + "&kind=reduce";
        TaskDetails mapTaskDetails = new JobDetailsScraper(new FileSource(mapTaskURL)).getTaskDetails();
        TaskDetails reduceTaskDetails = new JobDetailsScraper(new FileSource(reduceTaskURL)).getTaskDetails();
        return mapTaskDetails.concat(reduceTaskDetails);
    }

    private String getJobTrackerURL() {
        String jobTrackerAddress = configuration.getJobTrackerAddress();
        String defaultJobTrackerMonitorPort = "50030";
        return "http://" + jobTrackerAddress + ":" + defaultJobTrackerMonitorPort;
    }

    public TaskDetails getMapTaskDetails(String jobId) {
        String mapTaskURL = getJobTrackerURL() + "/jobfailures.jsp?jobid=" + jobId + "&kind=map";
        return new JobDetailsScraper(new FileSource(mapTaskURL)).getTaskDetails();
    }

    public TaskDetails getReduceTaskDetails(String jobId) {
        String mapTaskURL = getJobTrackerURL() + "/jobfailures.jsp?jobid=" + jobId + "&kind=reduce";
        return new JobDetailsScraper(new FileSource(mapTaskURL)).getTaskDetails();
    }
}

