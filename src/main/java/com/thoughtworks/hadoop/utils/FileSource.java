package com.thoughtworks.hadoop.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;

public class FileSource {
    private String handle;

    public FileSource(String handle) {
        this.handle = handle;
    }

    public Document get(String cause) {
        try {
            String targetURL = handle + "&cause=" + cause;
            return Jsoup.parse(new URL(targetURL), 5000);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
