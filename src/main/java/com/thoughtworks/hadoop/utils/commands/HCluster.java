package com.thoughtworks.hadoop.utils.commands;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.thoughtworks.hadoop.utils.ClusterClient;
import com.thoughtworks.hadoop.utils.ClusterConfiguration;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class HCluster implements HCommand {

    public static final String DIR_OPTION = "f";
    public static final String DEFAULT_DIR = System.getProperty("user.home") + "/.hadoop";
    private String clusterDetailsFile = "/cluster.json";

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        applyDefaults(hCommandArgument);
        String dir = hCommandArgument.get(DIR_OPTION);
        String file = dir + clusterDetailsFile;
        String json;
        try {
            json = FileUtils.readFileToString(new File(file));
        } catch (IOException e) {
            return new HCommandOutput(HCommandOutput.Result.FAILURE, e.getMessage());
        }
        json = prettyJson(json);
        return new HCommandOutput(HCommandOutput.Result.SUCCESS, json);
    }

    private String prettyJson(String json) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
        return gson.toJson(jsonObject);
    }

    public ClusterClient getClusterClient(HCommandArgument hCommandArgument) {
        HCommandOutput commandOutput = execute(hCommandArgument);
        if (commandOutput.isSuccess()) {
            String clusterJson = commandOutput.getOutput();
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(clusterJson, JsonObject.class);
            ClusterConfiguration configuration = new ClusterConfiguration(jsonObject.get("jobTracker").getAsString(), jsonObject.get("jobTrackerPort").getAsInt(), jsonObject.get("nameNode").getAsString(), jsonObject.get("nameNodePort").getAsInt());
            return new ClusterClient(configuration);
        }
        throw new RuntimeException("Cluster not found. Please mount a cluster before executing any command.");

    }

    public HCommandArgument applyDefaults(HCommandArgument hCommandArgument) {
        String hadoopHomeDir = hCommandArgument.get(DIR_OPTION);
        if (hadoopHomeDir == null) {
            hCommandArgument.put(DIR_OPTION, DEFAULT_DIR);
        }
        return hCommandArgument;
    }

    public static Options options() {
        Options options = new Options();
        options.addOption(DIR_OPTION, "home-dir", false, "Home dir to store hadoop Util configurations");
        return options;
    }

    public static void main(String[] args) {
        HCommandArgument argument = null;
        try {
            argument = HCommandArgument.create(args, HCluster.options());
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("commands usage", options());
            return;
        }
        HCluster hCluster = new HCluster();
        HCommandOutput output = hCluster.execute(argument);
        System.out.println(output.getOutput());
    }
}
