package com.thoughtworks.hadoop.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class HCluster implements HCommand {

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        applyDefaults(hCommandArgument);
        String dir = hCommandArgument.get("-f");
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
        String hadoopHomeDir = hCommandArgument.get("-f");
        if (hadoopHomeDir == null) {
            hCommandArgument.put("-f", "~/.hadoop");
        }
        return hCommandArgument;
    }
}
