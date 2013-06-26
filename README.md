hadoop-utils
============

###Command line tools for Hadoop

Compatability - Hadoop v1.0.3

Hadoop is evolving into an eco-system comprising of different components which earlier were present in non-distributed world(Operating Systems)

In this project, we attempt to give it a UNIX/Linux like command line utility which is a very tiny step towards establishing 
a much friendlier interaction to the distributed environment which gives the ability to combine the goodness of nix shell with hadoop client API.

####Target user
* Cluster Admin
* Users Monitoring Jobs
* Any one who would rather stick to *nix way of doing things than the web UI interaction
 
 
###Installation and Usage
  
  [Installation and Usage Wiki Page](https://github.com/npramod05/hadoop-utils/wiki/Installation-and-Usage)
 
 
####Current Feature List
* Mount a cluster - Equivalent to mounting a device, get the context of a cluster  
* Job Status - Equivalent to ps, list jobs and their details  
* Task Status - for a given job list different kinds of tasks and their execution status(number of times failed/killed)
* Kill - kill a job or task attempt(not a task, as this is not supported by the version of hadoop being used)


####More Utils to come
* Disk Usage in Cluster - Equivalent to du
* Memory and CPU usage for a given Job
* Nodes with Frequent Job Failures
  
  and more..


####Got something we can include? 
  Please raise a new issue with "Feature Request" label, add in a precise description and a use case
  we will pick it up from there.
  
  Many more things to follow, keep an eye on the repo.
  


## Contributors

* Srihari Srinivasan([@systems_we_make](https://twitter.com/systems_we_make))
* Hemanth Yamijala([@yhemanth](http://twitter.com/yhemanth))
* Pramod N([@machinelearner](https://twitter.com/machinelearner))
