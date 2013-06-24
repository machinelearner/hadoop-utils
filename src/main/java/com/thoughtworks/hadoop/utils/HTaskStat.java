package com.thoughtworks.hadoop.utils;

public class HTaskStat implements HCommand {
    private ClusterClient clusterClient;


    public HTaskStat(ClusterClient clusterClient) {
        this.clusterClient = clusterClient;
    }


    @Override
    public HCommandArgument applyDefaults(HCommandArgument hCommandArgument) {
        return null;
    }

    @Override
    public HCommandOutput execute(HCommandArgument hCommandArgument) {
        String jobId = hCommandArgument.get("jid");
        TaskDetails taskDetails = clusterClient.getTaskDetails(jobId);
        HCommandOutput output = new HCommandOutput(HCommandOutput.Result.SUCCESS, taskDetails.toString());
        return output;
    }
}
