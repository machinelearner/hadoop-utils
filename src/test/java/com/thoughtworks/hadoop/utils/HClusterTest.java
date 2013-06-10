package com.thoughtworks.hadoop.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class HClusterTest {


    private HCommandArgument hCommandArgument;

    private String serializeAsJSON(String jobTracker, Collection<String> taskTrackerNames) {
        Gson gson = new Gson();
        JsonObject clusterDetails = new JsonObject();
        String taskTrackers = gson.toJson(taskTrackerNames);
        clusterDetails.addProperty("taskTrackers", taskTrackers);
        clusterDetails.addProperty("jobTracker", jobTracker);
        return clusterDetails.toString();
    }

    @Before
    public void setUp() {
        hCommandArgument = new HCommandArgument();
        String DIR = "./hadoop";
        hCommandArgument.put("-f", DIR);
        List<String> expectedTaskTrackerNames = Arrays.asList("hadoop01.com", "hadoop02.com");
        String jobTrackerName = "jobtracker.hadoop.com";

        boolean hadoopDir = new File(DIR).mkdir();
        if (!hadoopDir) {
            throw new RuntimeException("Failed to create a hadoop dir");
        }
        try {
            File jsonFile = new File(DIR + "/cluster.json");
            jsonFile.createNewFile();
            FileWriter fileWriter = new FileWriter(jsonFile);
            fileWriter.write(serializeAsJSON(jobTrackerName, expectedTaskTrackerNames));
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @After
    public void tearDown() throws Exception {
        String dir_name = hCommandArgument.get("-f");
        File hadoopDir = new File(dir_name);
        FileUtils.deleteDirectory(hadoopDir);

    }

    @Test
    public void shouldRevealClusterDetails() {
        HCluster cluster = new HCluster();
        HCommandOutput output = cluster.execute(hCommandArgument);
        assertEquals(HCommandOutput.Result.SUCCESS, output.getResult());
    }
}
