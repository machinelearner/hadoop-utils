package com.thoughtworks.hadoop.utils;

public class TaskDetail {
    private final String taskId;
    private final String timesFailed;
    private final String timesKilled;

    public TaskDetail(String taskId, String timesFailed, String timesKilled) {
        this.taskId = taskId;
        this.timesFailed = timesFailed;
        this.timesKilled = timesKilled;
    }


    @Override
    public String toString() {
        String fiveSpaces = "     ";
        return taskId + fiveSpaces + timesFailed + fiveSpaces + timesKilled + "\n";
    }

}
