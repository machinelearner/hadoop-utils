package com.thoughtworks.hadoop.utils;

public class HCommandOutput {

    private final Result result;
    private final String output;

    public HCommandOutput(Result result, String output) {
        this.result = result;
        this.output = output;
    }

    public Result getResult() {
        return result;
    }

    public String getOutput() {
        return output;
    }

    public boolean isSuccess() {
        if (this.getResult() == Result.SUCCESS) {
            return true;
        }
        return false;
    }

    static enum Result {
        SUCCESS, FAILURE
    }

}


