package com.thoughtworks.hadoop.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class HMount implements HCommand {
    private ClusterClient clusterClient;


    public HMount(ClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }


    public Collection<String> getTaskTrackerNames() {
        Collection<String> taskTrackerNames = clusterClient.getTaskTrackerNames();
        ArrayList<String> taskTrackers = new ArrayList<String>();
        for (String ttName : taskTrackerNames) {
            taskTrackers.add(this.getHostNameFromTaskTrackerName(ttName));
        }
        return taskTrackers;
    }

    public String getHostNameFromTaskTrackerName(String taskTrackerName) {
        return HostnameParser.fromTaskTrackerName(taskTrackerName);
    }


    @Override
    public boolean wasSuccessful() {
        return false;
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
        String hadoopHomeDir = hCommandArgument.get("-f");
        if (hadoopHomeDir == null) {
            hCommandArgument.put("-f", "~/.hadoop");
        }
        return hCommandArgument;
    }

    private void writeClusterDetails(String clusterDetails, HCommandArgument argument) {
        String DIR_NAME = argument.get("-f");
        String FILE_NAME = DIR_NAME + "/cluster.json";
        boolean hadoopDir = new File(DIR_NAME).mkdir();
        if (!hadoopDir) {
            throw new RuntimeException("Failed to create a hadoop dir");
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
}
