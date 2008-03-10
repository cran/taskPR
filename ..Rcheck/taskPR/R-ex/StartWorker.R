### Name: StartWorker
### Title: Start Parallel-R Worker Process
### Aliases: StartWorker
### Keywords: programming

### ** Examples

## Not run: 
##D # If you have MPI running
##D StartPE(2)
##D 
##D x = matrix(rnorm(128 * 128), 128, 128)
##D 
##D PE( a <- svd(x) )
##D PE( b <- solve(x) )
##D PE( y <- b %*% a$u )
##D POBJ( y )
##D str(y)
##D StopPE()
## End(Not run)



