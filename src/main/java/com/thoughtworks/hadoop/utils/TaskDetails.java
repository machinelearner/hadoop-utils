package com.thoughtworks.hadoop.utils;

import java.util.ArrayList;

public class TaskDetails {

    private ArrayList<TaskDetail> taskDetails = new ArrayList<TaskDetail>();

    public void addDetail(TaskDetail taskDetail) {
        this.taskDetails.add(taskDetail);
    }

    @Override
    public String toString() {
        String fiveSpaces = "     ";
        String metadata = "Task_id" + fiveSpaces + "Times failed" + fiveSpaces + "Times Killed" + "\n";
        String output = metadata;

        for (TaskDetail taskDetail : taskDetails) {
            output += taskDetail.toString();
        }
        return output;
    }
}
