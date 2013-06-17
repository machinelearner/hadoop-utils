package com.thoughtworks.hadoop.utils;


import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

public class HCommandArgument {
    private Map<String, String> arguments = new HashMap<String, String>();

    public String get(String key) {
        return arguments.get(key);
    }

    public void put(String key, String value) {
        arguments.put(key, value);
    }

    public static HCommandArgument create(String[] args, Options options) {
        HCommandArgument hCommandArgument = new HCommandArgument();
        CommandLineParser parser = new BasicParser();
        CommandLine line;
        try {
            // parse the command line arguments
            line = parser.parse(options, args);
            for (Option option : line.getOptions()) {
                hCommandArgument.put(option.getOpt(), option.getValue());
            }
        } catch (org.apache.commons.cli.ParseException e) {
            throw new RuntimeException(e);
        }
        return hCommandArgument;
    }

    public int getAsInt(String key) {
        return Integer.parseInt(arguments.get(key));
    }

    public boolean hasArgument(String key) {
        return arguments.containsKey(key);
    }
}
