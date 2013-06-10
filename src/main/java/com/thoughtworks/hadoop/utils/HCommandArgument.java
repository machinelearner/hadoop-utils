package com.thoughtworks.hadoop.utils;

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
}
