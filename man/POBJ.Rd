\name{POBJ}
\alias{POBJ}
\title{Parallel Object (Return a Parallel Objection to R's workspace)}
\usage{
POBJ(x)
}
\arguments{
	\item{x}{ variable to return. }
}
\description{
	Blocks and waits for the specified variable to be returned by the
	parallel engine.
	}
\details{
	Because the \code{\link{PE}} function executes jobs in the background,
	there must be a way to return jobs to the foreground - to R's workspace.
	That method is the POBJ function.  When passed a variable (which doesn't
	have to exist in R's workspace, yet), the POBJ function waits until that
	variable is available and returned from the parallel engine.
	If variable to return is given as NULL, then the POBJ function waits for
	all variables to be returned.
	The POBJ function returns the \bold{symbol} of the variable, not the
	variable itself.
}

\seealso{
	\code{\link{StartPE}}  For enabling the parallel engine.
	\code{\link{PE}}  For executing jobs in parallel.
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
