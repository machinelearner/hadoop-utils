package com.thoughtworks.hadoop.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class JobDetail {
    private String jobName;
    private JobID id;
    private JobPriority priority;
    private String user;
    private float mapProgress;
    private float reduceProgress;
    private long startTime;
    private String state;
    private static final String DELIMITER_STRING = "     ";

    public JobDetail(JobID id, String jobName, JobPriority priority, String user, float mapProgress, float reduceProgress, long startTime, String state) {
        this.id = id;
        this.jobName = jobName;
        this.priority = priority;
        this.user = user;
        this.mapProgress = mapProgress;
        this.reduceProgress = reduceProgress;
        this.startTime = startTime;
        this.state = state;
    }

    public static JobDetail create(RunningJob job, JobStatus jobStatus) {
        JobID jobID = job.getID();
        String jobName = job.getJobName();
        JobPriority jobPriority = jobStatus.getJobPriority();
        String username = jobStatus.getUsername();
        float mapProgress = jobStatus.mapProgress();
        float reduceProgress = jobStatus.reduceProgress();
        long startTime = jobStatus.getStartTime();
        String runState = JobStatus.getJobRunState(jobStatus.getRunState());

        return new JobDetail(jobID, jobName, jobPriority, username, mapProgress, reduceProgress, startTime, runState
        );
    }

    public String getAllHeadersAsFormattedString() {
        Collection<String> listOfFields = new ArrayList<String>();
        listOfFields.add(id.toString());
        listOfFields.add(jobName);
        listOfFields.add(user);
        listOfFields.add(priority.name());
        listOfFields.add(String.valueOf(mapProgress));
        listOfFields.add(String.valueOf(reduceProgress));
        listOfFields.add((new Timestamp(startTime)).toString());
        listOfFields.add(state);

        return StringUtils.join(listOfFields, DELIMITER_STRING);
    }

    public static String formattedHeader() {
        List<String> formattedHeader = Arrays.asList("JobId", "JobName", "User", "Priority", "MapProgress", "ReduceProgress", "StartTime", "JobState");
        return StringUtils.join(formattedHeader, DELIMITER_STRING);
    }
}
