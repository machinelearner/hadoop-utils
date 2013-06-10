package com.thoughtworks.hadoop.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class HCluster implements HCommand {

    public static final String DIR_OPTION = "f";
    public static final String DEFAULT_DIR = "~/.hadoop";

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        applyDefaults(hCommandArgument);
        String dir = hCommandArgument.get(DIR_OPTION);
        String file = dir + "/cluster.json";
        String json;
        try {
            json = FileUtils.readFileToString(new File(file));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new HCommandOutput(HCommandOutput.Result.SUCCESS, json);
    }


    private HCommandArgument applyDefaults(HCommandArgument hCommandArgument) {
        String hadoopHomeDir = hCommandArgument.get(DIR_OPTION);
        if (hadoopHomeDir == null) {
            hCommandArgument.put(DIR_OPTION, DEFAULT_DIR);
        }
        return hCommandArgument;
    }
}
