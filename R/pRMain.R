StartPE <- function(num = 2, port = 32000, verbose=0, spawn=TRUE) {
    if (spawn) {
		script = system.file("exec", "pRBatch.R", package="taskPR")
		arg = c("--no-save", "CMD", "BATCH", script, "NULL")
		host = Sys.info()[names(Sys.info()) == "nodename"]
		prog = file.path(R.home(), "bin", "R")
		str(arg)
		.C("R_SpawnMPIProcesses", as.character(prog),
			 as.integer(num), as.character(arg), PACKAGE="taskPR")
	}
    s = 0 * (1:num)
    for (i in 1:num) {
		if (spawn) {
			.C("NotifySpawnedMPIProcesses", as.character(host),
					as.integer(i-1), PACKAGE="taskPR")
		}
        s[i] = socketConnection(port = port, block = TRUE, server=TRUE,
				open="a+b")
        s[i] = .Call("SocketFromConn", as.integer(s[i]), PACKAGE="taskPR")
    }
    PE.WorkerConnections <- s
    if (verbose > 0) {
        .Call("EnableVerboseParallelExecution", num, verbose + 1, sys.frame(1),
				PACKAGE="taskPR");
    } else {
        .Call("EnableParallelExecution", num, sys.frame(1), PACKAGE="taskPR")
    }
    return(s)
}

PE <- function(x, global=FALSE) {
	.Call("CallPE", substitute(x), global, rho=sys.frame(-1),
			PACKAGE="taskPR")
}
POBJ <- function(x) {
	name = deparse(substitute(x))
		
	.Call("CallPOBJ", as.symbol(name), rho=sys.frame(-1),
		PACKAGE="taskPR")
}

StopPE <- function() .Call("DisableParallelExecution", PACKAGE="taskPR")
