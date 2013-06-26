package com.thoughtworks.hadoop.utils.commands;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.thoughtworks.hadoop.utils.ClusterClient;
import com.thoughtworks.hadoop.utils.ClusterConfiguration;
import com.thoughtworks.hadoop.utils.HostnameParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HMount implements HCommand {
    public static final String DIR_OPTION = "f";
    public static final String DEFAULT_DIR = System.getProperty("user.home") + "/.hadoop";
    private ClusterClient clusterClient;


    public HMount(ClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }


    public Collection<String> getTaskTrackerNames() {
        Collection<String> taskTrackerNames = clusterClient.getTaskTrackerNames();
        List<String> taskTrackers = new ArrayList<String>();
        for (String ttName : taskTrackerNames) {
            taskTrackers.add(this.getHostNameFromTaskTrackerName(ttName));
        }
        return taskTrackers;
    }

    public String getHostNameFromTaskTrackerName(String taskTrackerName) {
        return HostnameParser.fromTaskTrackerName(taskTrackerName);
    }

    public Collection<String> getDataNodes() {
        return clusterClient.getDataNodes();
    }

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        Collection<String> taskTrackerNames = getTaskTrackerNames();
        String jobTracker = clusterClient.getJobTrackerName();
        String nameNode = clusterClient.getNameNodeName();
        int jtPortNumber = clusterClient.getJtPortNumber();
        int nnPortNumber = clusterClient.getNnPortNumber();
        Collection<String> dataNodes = clusterClient.getDataNodes();
        JsonObject allDetails = allDetailsAsJson(jobTracker, jtPortNumber, taskTrackerNames, nameNode, nnPortNumber, dataNodes);
        String clusterDetails = allDetails.toString();
        writeClusterDetails(clusterDetails, applyDefaults(hCommandArgument));
        return new HCommandOutput(HCommandOutput.Result.SUCCESS, clusterDetails);

    }

    private static void handleSuperUserOverride(HCommandArgument hCommandArgument) {
        String superUser = hCommandArgument.get("u");
        if (superUser != null) {
            System.setProperty("HADOOP_USER_NAME", superUser);
        }
    }

    public HCommandArgument applyDefaults(HCommandArgument hCommandArgument) {

        if (!hCommandArgument.hasArgument(DIR_OPTION)) {
            hCommandArgument.put(DIR_OPTION, DEFAULT_DIR);
        }
        return hCommandArgument;
    }

    private void writeClusterDetails(String clusterDetails, HCommandArgument argument) {
        String DIR_NAME = argument.get(DIR_OPTION);
        String FILE_NAME = DIR_NAME + "/cluster.json";
        File dir = new File(DIR_NAME);
        if (!dir.exists()) {
            createWorkingDirectory(dir);
        }
        try {
            File jsonFile = new File(FILE_NAME);
            jsonFile.createNewFile();
            FileWriter fileWriter = new FileWriter(jsonFile);
            fileWriter.write(clusterDetails);
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createWorkingDirectory(File dir) {
        boolean hadoopDir = dir.mkdir();
        if (!hadoopDir) {
            throw new RuntimeException("Failed to create a hadoop dir at " + dir.getAbsolutePath());
        }
    }

    private JsonObject allDetailsAsJson(String jobTracker, int jtPortNumber, Collection<String> taskTrackerNames, String nameNode, int nnPortNumber, Collection<String> dataNodes) {
        Gson gson = new Gson();
        JsonObject allDetails = new JsonObject();
        String taskTrackers = gson.toJson(taskTrackerNames);
        allDetails.addProperty("taskTrackers", taskTrackers);
        allDetails.addProperty("jobTracker", jobTracker);
        allDetails.addProperty("jobTrackerPort", jtPortNumber);
        String dataNodeJson = gson.toJson(dataNodes);
        allDetails.addProperty("dataNodes", dataNodeJson);
        allDetails.addProperty("nameNode", nameNode);
        allDetails.addProperty("nameNodePort", nnPortNumber);
        return allDetails;
    }

    public static Options options() {
        Options options = new Options();
        options.addOption(DIR_OPTION, "home-dir", true, "Home dir to store hadoop Util configurations");
        options.addOption("j", "job-tracker", true, "Job Tracker HostName[Required]");
        options.addOption("n", "name-node", true, "Name Node HostName[Required]");
        options.addOption("jp", "jobtracker-port-number", true, "Port Number of Job Tracker[Required] ");
        options.addOption("np", "namenode-port-number", true, "Port Number of Name Node[Required]");
        options.addOption("u", "super-user", true, "Super User to execute admin commands");
        return options;
    }

    public static void main(String[] args) {
        HCommandArgument argument = null;
        try {
            argument = HCommandArgument.create(args, options());
            argumentsCheck(argument);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.out.println(e.getMessage());
            formatter.printHelp("command usage", options());
            return;
        }
        handleSuperUserOverride(argument);
        ClusterConfiguration configuration = new ClusterConfiguration(argument.get("j"), argument.getAsInt("jp"), argument.get("n"), argument.getAsInt("np"));
        ClusterClient clusterClient = new ClusterClient(configuration);
        HMount mount = new HMount(clusterClient);
        HCommandOutput output = mount.execute(argument);
        System.out.println(output.getOutput());
    }

    private static void argumentsCheck(HCommandArgument argument) throws ParseException {
        if (!argument.hasArgument("j") || !argument.hasArgument("jp") || !argument.hasArgument("n") || !argument.hasArgument("np")) {
            throw new ParseException("Arguments Missing");
        }
    }
}
