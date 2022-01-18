# Follower Maze  

pqueue - 1743 sec., 10kk events, ~5.74k RPS on average

### How to run  

This solution based on top of *unstable* golang version `1.18beta1`, since I'm using generics here.  
To sintall beta version, do the following:  
  1. Most probably you already have go installed, in my case it was the latest stable version `1.17.6`. In this case you only need to run: `go install golang.org/dl/go1.18beta1@latest`  
  2. Download updates: `go1.18beta1 download`  
  4. Check that it works, by running: `go1.18beta1 version`  

### TODO  
 - replace default logger with `glog` and be able to specify levels  
 - add comments to all the public methods and variables  
 - add tests  