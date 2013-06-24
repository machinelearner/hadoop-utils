package com.thoughtworks.hadoop.utils;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class JobDetailsScraper {
    private FileSource source;

    public JobDetailsScraper(FileSource source) {
        this.source = source;
    }

    public TaskDetails getTaskDetails() {
        Document failedDetailPage = source.get("failed");
        Document killedDetailPage = source.get("killed");
        HashMap failedTaskCountMap = getTaskCountMap(failedDetailPage);
        HashMap killedTaskCountMap = getTaskCountMap(killedDetailPage);
        Set allKeys = getMergedKeySet(failedTaskCountMap, killedTaskCountMap);
        return generateAllDetails(failedTaskCountMap, killedTaskCountMap, allKeys);
    }

    private TaskDetails generateAllDetails(HashMap failedTaskCountMap, HashMap killedTaskCountMap, Set allKeys) {
        Iterator allKeysIterator = allKeys.iterator();
        TaskDetails allTaskDetails = new TaskDetails();
        while (allKeysIterator.hasNext()) {
            String key = (String) allKeysIterator.next();
            int failedCount = (failedTaskCountMap.containsKey(key) ? (Integer) failedTaskCountMap.get(key) : 0);
            int killedCount = (killedTaskCountMap.containsKey(key) ? (Integer) killedTaskCountMap.get(key) : 0);
            TaskDetail taskDetail = new TaskDetail(key, String.valueOf(failedCount), String.valueOf(killedCount));
            allTaskDetails.addDetail(taskDetail);
        }
        return allTaskDetails;
    }

    private Set getMergedKeySet(HashMap failedTaskCountMap, HashMap killedTaskCountMap) {
        Set failedTasksIds = failedTaskCountMap.keySet();
        Set killedTasksIds = killedTaskCountMap.keySet();
        Set<String> allKeys = new HashSet<String>();
        allKeys.addAll(failedTasksIds);
        allKeys.addAll(killedTasksIds);
        return allKeys;
    }

    private HashMap getTaskCountMap(Document document) {
        HashMap<String, Integer> taskIdFailCountMap = new HashMap<String, Integer>();
        Element taskDetailsTable = document.select("table").get(0);
        Elements taskRows = taskDetailsTable.getElementsByTag("tr");
        for (Element taskRow : taskRows) {
            Elements cell = taskRow.getElementsByTag("td");
            if (cell.isEmpty()) {
                continue;
            }
            String taskId = cell.get(1).getElementsByTag("a").text();
            int count = (taskIdFailCountMap.containsKey(taskId) ? taskIdFailCountMap.get(taskId) : 0) + 1;
            taskIdFailCountMap.put(taskId, count);
        }
        return taskIdFailCountMap;
    }

}

