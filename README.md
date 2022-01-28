# Follower Maze  

This is a solution for soundcloud backend developers challenge. Check out instructions in `/simulator` folder.  
In short - the one should develop socket server that distributes events from the event source to clients.  

My current solution processes 10kk events (simulator's default) in 400 sec., which translates to the **~25k** RPS on average.  
If I configure a simulator to spawn 10x more concurrent users (100 --> 1000), RPS drops respectively to **~2.5k**.  
Need to come up with solution to that problem.  
Check comments across the code for more info.  

### How to run  

This solution is based on top of *beta* golang version `1.18beta1`, since I'm using generics here.  
To sintall beta version, do the following:  
  1. Most probably you already have go installed, in my case it was the latest stable version `1.17.6`. In this case you only need to run: `go install golang.org/dl/go1.18beta1@latest`.  
  2. Download updates: `go1.18beta1 download`.  
  4. Check that it works, by running: `go1.18beta1 version`.  
After, you can build an app: `make build` or create static binary: `make build-static`.  
And finally - you can run the server:  
```
make run
```  
And simulator, with the default settings:  
```
make simulator
```  
Use `make simulator-test` to test your solution with lower amount of connected clients and events, for debug purposes.  

In order to see debug logs, you need pass env variable while running a server:  
```
LOG_LEVEL=debug make run
```  

### TODO
 - add tests coverage results and widget with tests status
 - think of how to make solution more scalable to support more concurrent users
