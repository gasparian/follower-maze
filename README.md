![tests](https://github.com/gasparian/follower-maze/actions/workflows/test.yml/badge.svg?branch=main)

# Follower Maze  

This is a solution for soundcloud backend developers challenge. Check out instructions in `/simulator` folder.  
In short - the one should develop socket server that distributes events from the event source to clients in real time.  

My current solution processes 10kk events (simulator's default) in 400 sec., which translates to the **~25k** RPS on average.  
If simulator configured to spawn 10x more concurrent users (100 --> 1000), RPS drops ~10x times respectively to **~2.5k**.  

My solution is based on creating two separate socket servers:  
 - for accepting clients connections;  
 - for working with a single event source connection;  

These servers are merged under the `FollowerServer`, which handles all the communication between them.  
After accepting new client connection, client server creates channel associated with that new client, puts this channel to the queue which `FollowerServer` listens, and starts listen for events from that channel. And when the event occurs - it writes this event to this client's socket.  
After `FollowerServer` "sees" new client - it "registers" it by adding new client to special map, where client channels and followers are being stored.  
Events server just listens to the incoming events, parses them and puts in the priority queue, based on event id.  
`FollowerServer` listerns to this events queue and, depending on event recipients, puts event to the needed clients channels (which it gets from the client server, initially).  
All these servers and listeners running in separate goroutines.  

Check the code for more info. You should start "unraveling" from the `follower_server.go`.  

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
