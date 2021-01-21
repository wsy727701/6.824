[TOC]





# 6.824-Distributed System

## Lab TaskTable

* Lab1-MapReduce    Yes

## Lab1 MapReduce

### IDEA

#### GO

* Run a request handler server
* Always arrange tasks in sequence to check whether all the tasks have been done 

#### RPC

* New worker enter
* Worker request a new task
* Worker report a task status 

#### Task Status

* waiting
* in_queue
* running
* finished
* error

#### Channel

* Master write a new in-queue task into channel
* Worker read a task from channel while requesting a new task to run 

### Debug

#### MutexLock Block

1. 锁定防止进程竞争

   > 只有一个master处理多个worker ，操作时要避免并发的race

2. 写的时候要锁定，读的时候可以不用

   > 在worker request new task 时，从channel中读取不加锁，写入改变状态时锁定
   >
   > 否则channel为空时(map任务分配结束，reduce还未开始分配)，channel阻塞导致Lock永久性阻塞

#### Arrange Tasks

>需要一个初始状态和一个排队状态，否则会重复添加初始状态的任务



