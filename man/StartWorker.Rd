\name{StartWorker}
\alias{StartWorker}
\title{Start Parallel-R Worker Process}
\usage{
StartWorker (host = "localhost", port=32000, retries=2, sleeptime=1, quiet=TRUE)
}
\arguments{
	\item{host}{ name of the machine that the main/controller process is on }
	\item{port}{ the (TCP/IP) port number to connect to }
	\item{retries}{ the number of times to retry making the connection }
	\item{sleeptime}{ how long (in seconds) to sleep between connection tries }
	\item{quiet}{ should the worker process supress most logging messages? }
}
\description{
	Attempts to connect to the given host and establish itself as a worker
	process.  This function is called automatically when worker processes
	are spawned by the main process.
	}
\details{
	The only time a user should call this function is when they started the
	parallel engine on the main process using the spawn=FALSE option to
	StartPE.  In that case, the main process will block waiting for the
	worker processes to connect.  The user must run the appropriate number
	of worker processes and have them call this function.
}

\seealso{
	\code{\link{StartPE}}  For enabling the parallel engine.
	\code{\link{PE}}	For running parallel jobs.
	\code{\link{POBJ}}  For returning background jobs to the main process.
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
