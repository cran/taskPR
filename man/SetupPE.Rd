\name{StartPE}
\alias{StartPE}
\alias{StopPE}
\alias{taskPR}
\alias{ParallelR}
\title{Start/Stop Parallel Execution}
\usage{
StartPE(num = 2, port = 32000, verbose=0, spawn=TRUE)
StopPE()
}
\arguments{
	\item{num}{ number of worker processes to use }
	\item{port}{ the TCP port to use for communicating with workers }
	\item{verbose}{ the verbose level: 0, 1, or 2 at the moment }
	\item{spawn}{ should the worker processes be spawned? }
}
\description{
	StartPE starts the parallel engine.  If spawn is true, then the worker
		processes are spawned (using \code{MPI\_COMM\_Spawn} from MPI-2).
	StopPE stops the parallel engine.  This call blocks until all jobs
		are finished.
	}
\details{
	The parallel engine must be enabled before instructions can be executed
	in parallel.  The engine can be stopped and restarted with a different
	number of worker processes, if desired.
	The parallel engine consists of \code{num} + 1 threads and \code{num}
	worker processes.  The worker processes can either be spawned (done
	through an MPI call) or connected manually.  If StartPE is run with
	spawn = FALSE, then it will block until \code{num} worker processes
	have connected.
}

\seealso{
	\code{\link{PE}}  For executing jobs in the background/parallel.
	\code{\link{POBJ}}  For returning background/parallel jobs to the main process.
	\code{\link{StartWorker}}  For manually starting worker processes.
}

\examples{
\dontrun{
# If you have MPI running
StartPE(2)

x = matrix(rnorm(128 * 128), 128, 128)

PE( a <- svd(x) )
PE( b <- solve(x) )
PE( y <- b \%*\% a$u )
POBJ( y )
str(y)
StopPE()
}
}
\concept{parallel}
\keyword{programming}
