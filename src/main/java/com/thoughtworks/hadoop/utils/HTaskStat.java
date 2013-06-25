package com.thoughtworks.hadoop.utils;

import org.apache.commons.cli.Options;

public class HTaskStat implements HCommand {
    private ClusterClient clusterClient;
    private static final String DIR_OPTION = "f";


    public HTaskStat(ClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }


    @Override
    public HCommandArgument applyDefaults(HCommandArgument argument) {
        handleSuperUserOverride(argument);
        if (!argument.hasArgument("m") && !argument.hasArgument("r") && !argument.hasArgument("a")) {
            argument.put("a", "");
        }

        return argument;
    }

    private void handleSuperUserOverride(HCommandArgument argument) {
        String superUser = argument.get("u");
        if (superUser != null) {
            System.setProperty("HADOOP_USER_NAME", superUser);
        }
    }

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        applyDefaults(hCommandArgument);
        String jobId = hCommandArgument.get("jid");
        TaskDetails taskDetails = new TaskDetails();

        if (hCommandArgument.hasArgument("a")) {
            taskDetails = clusterClient.getAllTaskDetails(jobId);
        } else if (hCommandArgument.hasArgument("m")) {
            taskDetails = clusterClient.getMapTaskDetails(jobId);
        } else if (hCommandArgument.hasArgument("r")) {
            taskDetails = clusterClient.getReduceTaskDetails(jobId);
        }
        return new HCommandOutput(HCommandOutput.Result.SUCCESS, taskDetails.toString());
    }

    public static Options options() {
        Options options = new Options();
        options.addOption(DIR_OPTION, "home-dir", true, "Home dir to store hadoop Util configurations");
        options.addOption("u", "super-user", false, "Super User to execute admin commands");
        options.addOption("a", "all", false, "All Tasks");
        options.addOption("m", "map", false, "Map Tasks");
        options.addOption("r", "reduce", false, "Reduce Tasks");
        options.addOption("s", "setup", false, "Setup");
        options.addOption("c", "cleanup", false, "Cleanup");
        return options;
    }
}
