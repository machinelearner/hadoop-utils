package com.thoughtworks.hadoop.utils.commands;

import com.thoughtworks.hadoop.utils.ClusterClient;
import com.thoughtworks.hadoop.utils.TaskDetails;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
    public HCommandOutput execute(HCommandArgument hCommandArgument) throws ParseException {
        applyDefaults(hCommandArgument);
        String jobId = hCommandArgument.get("jid");
        if (jobId == null) {
            throw new ParseException("No Job ID Given");
        }
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
        options.addOption("u", "super-user", true, "Super User to execute admin commands");
        options.addOption("jid", "job-id", true, "Job Id[Required]");
        options.addOption("a", "all", false, "All Tasks[Default]");
        options.addOption("m", "map", false, "Map Tasks");
        options.addOption("r", "reduce", false, "Reduce Tasks");
        return options;
    }

    public static void main(String[] args) {
        HCluster hCluster = new HCluster();
        HCommandArgument argument = null;
        try {
            argument = HCommandArgument.create(args, options());
        } catch (ParseException e) {
            printCommandUsage();
            return;
        }
        ClusterClient clusterClient = hCluster.getClusterClient(argument);
        HTaskStat hTaskStat = new HTaskStat(clusterClient);
        HCommandOutput commandOutput = new HCommandOutput(HCommandOutput.Result.FAILURE, "Unsuccessful Execution");
        try {
            commandOutput = hTaskStat.execute(argument);
        } catch (ParseException e) {
            printCommandUsage();
            return;
        }
        System.out.println(commandOutput.getOutput());
    }

    private static void printCommandUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("command usage", options());
    }

}

