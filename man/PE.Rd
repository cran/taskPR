\name{PE}
\alias{PE}
\title{Parallel Execute}
\usage{
PE(x, global=FALSE)
}
\arguments{
	\item{x}{ expression to execute in background/parallel }
	\item{global}{ should this command be executed globally? }
}
\description{
	Attempts to execute the given expression through the parallel engine.
	}
\details{
	The parallel engine takes the given expression and tries to execute it
	on a worker process (which may be on a remote system).  It will be
	executed in the background and possibly in parallel with other PE calls.

	Only a limited number of jobs can be run through the parallel engine at
	once.  If that number is exceeded, then the PE call will block until some
	of the jobs have finished.

	\emph{The given expression \strong{must} be of the form "out <- f(...)".}

	When code{global} is true, the command is executed globally.  This does
	several things.  First, the parallel engine will wait until all workers
	are free before started to execute the command.  Second, the command will
	be executed on all worker processes simultaneously.  Third, the output of
	the command will NOT be deleted from the worker processes' workspaces (as
	is usually done).  This "global execute" functionality is designed for
	loading libraries or defining functions, and not for the parallel
	execution of common commands.
}

\seealso{
	\code{\link{StartPE}}  For enabling the parallel engine.
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
