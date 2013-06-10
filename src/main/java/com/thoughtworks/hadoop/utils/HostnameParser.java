package com.thoughtworks.hadoop.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HostnameParser {

    public static final String INVALID_HOSTNAME = "INVALID_HOSTNAME";

    public static String fromTaskTrackerName(String localhost_tracker_name) {
        String matchedHostname = "";
        Pattern hostnameGroupPattern = Pattern.compile("^tracker_([ (\\w|\\d|_|\\.)]+):");
        Matcher hostNameMatcher = hostnameGroupPattern.matcher(localhost_tracker_name);
        hostNameMatcher.find(0);
        try {
            matchedHostname = hostNameMatcher.group(1);
        } catch (IllegalStateException e) {
            return INVALID_HOSTNAME;
        }
        return matchedHostname;
    }
}
