print("Child Started")
library(taskPR)
host = .Call("PRWorkerInit")
print(host)
StartWorker(host = host)

