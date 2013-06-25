hadoop-utils
============

###Command line tools for Hadoop

Compatability - Hadoop v1.0.3

Hadoop is evolving into an eco-system comprising of different components which earlier were present in non-distributed world(Operating Systems)

In this project, we attempt to give it a UNIX/Linux like command line utility which is a very tiny step towards establishing 
a much friendlier interaction to the distributed environment which gives the ability to combine the goodness of nix shell with hadoop client API.

####Target user
  1.Cluster Admin
  
  2.Users Monitoring Jobs
  
  3.Any one who would rather stick to *nix way of doing things than the web UI interaction
  
####Current Feature List
  1.Mount a cluster - Equivalent to mounting a device, get the context of a cluster
  
  2.Job Status - Equivalent to ps, list jobs and their details
  
  3.Task Status - for a given job list different kinds of tasks and their execution status(number of times failed/killed)
  
  4.Kill - kill a job or task attempt(not a task, as this is not supported by the version of hadoop being used)

####More Utils to come
  1.Disk Usage in Cluster - Equivalent to du
  
  2.Memory and CPU usage for a given Job
  
  3.Nodes with Frequent Job Failures
  
  and more..
  
  
  ####Got something we can include? 
  Please raise a new issue with "Feature Request" label, add in a precise description and a use case
  we will pick it up from there.
  
  Many more things to follow, keep an eye on the repo.
