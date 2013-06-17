package com.thoughtworks.hadoop.utils;

import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;

public class HJobStat implements HCommand {

    private static final String DIR_OPTION = "f";

    public Collection<JobDetail> getAllJobDetails(ClusterClient clusterClient) {
        return clusterClient.getAllJobDetails();
    }

    private Collection<JobDetail> getRunningJobs(ClusterClient clusterClient) {
        return clusterClient.getRunningJobDetails();
    }

    private Collection<JobDetail> getRetiredJobs(ClusterClient clusterClient) {
        return clusterClient.getRetiredJobDetails();
    }

    private Collection<JobDetail> getCompletedJobs(ClusterClient clusterClient) {
        return clusterClient.getCompletedJobDetails();
    }


    private static void handleSuperUserOverride(HCommandArgument hCommandArgument) {
        String superUser = hCommandArgument.get("u");
        if (superUser != null) {
            System.setProperty("HADOOP_USER_NAME", superUser);
        }
    }


    @Override
    public HCommandArgument applyDefaults(HCommandArgument hCommandArgument) {
        if (!hCommandArgument.hasArgument("r") && !hCommandArgument.hasArgument("re") && !hCommandArgument.hasArgument("c") && !hCommandArgument.hasArgument("a")) {
            hCommandArgument.put("a", "");
        }

        return hCommandArgument;
    }

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        applyDefaults(hCommandArgument);
        HCluster hCluster = new HCluster();
        ClusterClient clusterClient = hCluster.getClusterClient(hCommandArgument);
        Collection<JobDetail> jobDetails = new ArrayList<JobDetail>();
        if (hCommandArgument.hasArgument("r")) {
            jobDetails = getRunningJobs(clusterClient);
        } else if (hCommandArgument.hasArgument("re")) {
            jobDetails = getRetiredJobs(clusterClient);
        } else if (hCommandArgument.hasArgument("c")) {
            jobDetails = getCompletedJobs(clusterClient);
        } else if (hCommandArgument.hasArgument("a")) {
            jobDetails = getAllJobDetails(clusterClient);
        }

        ArrayList<String> formattedJobDetails = new ArrayList<String>();
        for (JobDetail jobDetail : jobDetails) {
            formattedJobDetails.add(jobDetail.getAllHeadersAsFormattedString());
        }
        return new HCommandOutput(HCommandOutput.Result.SUCCESS, StringUtils.join(formattedJobDetails, "\n"));

    }


    public static Options options() {
        Options options = new Options();
        options.addOption(DIR_OPTION, "home-dir", true, "Home dir to store hadoop Util configurations");
        options.addOption("u", "super-user", false, "Super User to execute admin commands");
        options.addOption("a", "all", false, "All jobs");
        options.addOption("r", "running", false, "Running Jobs");
        options.addOption("c", "completed", false, "Completed Jobs");
        options.addOption("re", "retired", false, "Retired Jobs");
        return options;
    }

    public static void main(String[] args) {
        HCommandArgument argument = HCommandArgument.create(args, options());
        handleSuperUserOverride(argument);
        HJobStat hJobStat = new HJobStat();
        HCommandOutput commandOutput = hJobStat.execute(argument);
        System.out.println(JobDetail.formattedHeader());
        System.out.println(commandOutput.getOutput());
    }
}
