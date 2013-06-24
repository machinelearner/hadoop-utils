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

    public TaskDetails concat(TaskDetails other) {
        taskDetails.addAll(other.taskDetails);
        return this;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof TaskDetails)) return false;
        TaskDetails that = (TaskDetails) object;
        return this.toString().equals(that.toString());
    }

    @Override
    public int hashCode() {
        int defaultHash = super.hashCode();
        return this.toString() != null ? this.toString().hashCode() : defaultHash;
    }
}
