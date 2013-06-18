package com.thoughtworks.hadoop.utils;

import org.apache.commons.cli.Options;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;

public class HKill {
    public static final String INVALID_JOB_ID = "Invalid Job Id given";
    public static final String NO_ARGUMENT_MESSAGE = "No Job Id or Task Id given";
    public static final String CANNOT_KILL_JOB_IO_ERROR = "Cannot Kill Job - IO Error";
    private static final String DIR_OPTION = "f";
    public static final String INVALID_EXECUTION = "Invalid Execution";


    private HCommandArgument applyDefaults(HCommandArgument argument) {
        handleSuperUserOverride(argument);
        return argument;
    }

    private void handleSuperUserOverride(HCommandArgument hCommandArgument) {
        String superUser = hCommandArgument.get("u");
        if (superUser != null) {
            System.setProperty("HADOOP_USER_NAME", superUser);
        }
    }

    public HCommandOutput execute(HCommandArgument argument) {
        if (!argument.hasArgument("job") && !argument.hasArgument("task")) {
            throw new RuntimeException(NO_ARGUMENT_MESSAGE);
        }
        applyDefaults(argument);
        HCluster hCluster = new HCluster();
        ClusterClient clusterClient = hCluster.getClusterClient(argument);
        if (argument.hasArgument("job")) {
            String jobId = argument.get("job");
            killAJob(jobId, clusterClient);
            return new HCommandOutput(HCommandOutput.Result.SUCCESS, jobId);
        }

        return new HCommandOutput(HCommandOutput.Result.FAILURE, INVALID_EXECUTION);
    }

    private void killAJob(String job, ClusterClient clusterClient) {
        JobID jobID = null;
        try {
            jobID = JobID.forName(job);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(INVALID_JOB_ID);
        }
        try {
            clusterClient.killJob(jobID);
        } catch (IOException e) {
            throw new RuntimeException(CANNOT_KILL_JOB_IO_ERROR + "\n" + e.getMessage());
        }
    }

    public static Options options() {
        Options options = new Options();
        options.addOption(DIR_OPTION, "home-dir", true, "Home dir to store hadoop Util configurations");
        options.addOption("u", "super-user", true, "Super User to execute admin commands");
        options.addOption("job", "job", true, "Job Id of job to be killed");
        options.addOption("task", "task", true, "Task Id of task to be killed");
        return options;
    }

    public static void main(String[] args) {
        HCommandArgument argument = HCommandArgument.create(args, options());
        HKill hKill = new HKill();
        HCommandOutput commandOutput = hKill.execute(argument);
        System.out.println("Killed " + commandOutput.getOutput());
    }

}
