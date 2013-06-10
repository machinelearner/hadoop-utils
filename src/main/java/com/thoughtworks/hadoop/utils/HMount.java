package com.thoughtworks.hadoop.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.cli.Options;

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

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        Collection<String> taskTrackerNames = getTaskTrackerNames();
        String jobTracker = clusterClient.getJobTrackerName();
        String clusterDetails = serializeAsJSON(jobTracker, taskTrackerNames);
        writeClusterDetails(clusterDetails, applyDefaults(hCommandArgument));
        return new HCommandOutput(HCommandOutput.Result.SUCCESS, clusterDetails);
    }

    private HCommandArgument applyDefaults(HCommandArgument hCommandArgument) {
        String hadoopHomeDir = hCommandArgument.get(DIR_OPTION);
        if (hadoopHomeDir == null) {
            hCommandArgument.put(DIR_OPTION, DEFAULT_DIR);
        }
        return hCommandArgument;
    }

    private void writeClusterDetails(String clusterDetails, HCommandArgument argument) {
        String DIR_NAME = argument.get(DIR_OPTION);
        String FILE_NAME = DIR_NAME + "/cluster.json";
        File dir = new File(DIR_NAME);
        boolean hadoopDir = dir.mkdir();
        if (!hadoopDir) {
            throw new RuntimeException("Failed to create a hadoop dir at " + dir.getAbsolutePath());
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

    private String serializeAsJSON(String jobTracker, Collection<String> taskTrackerNames) {
        Gson gson = new Gson();
        JsonObject clusterDetails = new JsonObject();
        String taskTrackers = gson.toJson(taskTrackerNames);
        clusterDetails.addProperty("taskTrackers", taskTrackers);
        clusterDetails.addProperty("jobTracker", jobTracker);
        return clusterDetails.toString();
    }

    public static void main(String[] args) {
        HCommandArgument argument = HCommandArgument.create(args, HMount.options());
        HMount mount = new HMount(new ClusterClient(argument.get("j"), argument.getAsInt("p")));
        HCommandOutput output = mount.execute(argument);
        System.out.println(output.getOutput());
    }

    public static Options options() {
        Options options = new Options();
        options.addOption(DIR_OPTION, "home-dir", false, "Home dir to store hadoop Util configurations");
        options.addOption("j", "job-tracker", true, "Job Tracker HostName");
        options.addOption("p", "port-number", true, "Port Number of Job Tracker");
        return options;
    }
}
