# cornerstone

![Build Status](https://github.com/datatechnology/cornerstone/workflows/Build%20and%20test/badge.svg)

A very lightweight but complete Raft Consensus C++ implementation, the [original implementation](https://github.com/andy-yx-chen/cornerstone) was published by [Andy Chen](https://github.com/andy-yx-chen), as he agrees,  we re-organize his source code, and republish under the same license.

To respect Andy Chen's work, we keep using **cornerstone** as the project's name and we will start iterating based on his work.

## Notice

- fs_log_store is not for production

## Users

 1. [NuRaft](https://github.com/eBay/NuRaft)

## Features

- [x] Core algorithm, implemented based on TLA+ spec (though the spec does not have timer module)
- [x] Configuration change support, add or remove servers without any limitation
- [x] Log compaction
- [x] **Urgent commit**, enables the leader to ask all other peers to commit one or more logs if commit index is advanced
- [x] Client request support, for each server, the state machine could get a raft consensus client to send request to the leader, so leader change listener is not required.

## Where to start?

The key advantage or could be disadvantage for this implementation is it does not have any additional code that is unrelated to raft consensus itself. Which means, it does not have state machine, which could be a storage service.
It also has very few dependencies, actually, only the STL and asio. It chooses asio because asio supports both timer framework and async socket framework, which could be run on Windows, Linux and BSD systems.
The project only contains the following stuff,

 1. core algorithm, raft_server.cxx
 2. fstream based log storage
 3. asio based timer implementation
 4. asio based rpc server and client (through tcp)
 5. buffered logger implementation

### Why these are sufficient?

1. Core is core, it's all about Raft itself. Developers could just copy the headers and this file to make Raft work.
2. For log storage, please make your platform specific implementation, we may or may not add sophisticated implementation as it's not hard for everyone to implement one, the existing fs_log_store is for demo only as it's lack of many stuff including flush file data and meta data into physical device, always keeping three files in correct state (sort of transactional), etc.
3. Asio is sufficient, you may think about having messge queues for incoming and outgoing requests, but that's unnecessary, as long as you are using async io, no matter it's IOCP, kqueue or epoll, there is already a queue behind the scene, asio would be good enough as a production based code.

You are not able to get an exe file to do something meaningful by building the project, actually, you will get archive file instead (or lib file on Windows), however, you can build the test project and see how to use it and how it would work

You most likely should start with test_everything_together.cxx, under test/src folder, as that contains a use case of

- what need to be implemented to leverage the core algorithm
- how to use the core algorithm
- how to send logs to cluster
- ext: how to use asio_service

## Build and run

### On Windows

Please use nmake to do the job

The following command will build the test project and run all tests
```nmake -f Makefile.win tests```

The following command will build the lib file,
```nmake -f Makefile.win all```

### On Linux

Please use gc make to do the job

The following command will build the test project and run all tests
```make -f Makefile.lx testr```

The following command will build the lib file,
```make -f Makefile.lx all```

### On FreeBSD

Please use pmake to do the job

The following command will build the test project and run all tests
```make -f Makefile.bsd test```

The following command will build the lib file,
```make -f Makefile.bsd all```

## Contact Us

For any questions, please contact [Us](mailto:github@data-technology.net)
