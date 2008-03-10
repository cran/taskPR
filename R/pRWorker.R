StartWorker <- function(host = "localhost", port=32000, retries=2, sleeptime=1, quiet=TRUE) {
	# Note: All variables which need to survive the setVar/unserialize call have
	# "PRW" (Parallel R Worker) prepended to them.

	# Timeout is set at 1 hour.  Some applications may need to change this.
	options(timeout=3600)
	for (i in 1:(retries + 1)) {
		PRWcon = try( socketConnection(host, port, block = TRUE, open="a+b") )
		if (inherits(PRWcon, "try-error")) Sys.sleep(eval(sleeptime))
		else break
	}

	while (1 == 1) {
		PRWglobal = 0
		PRWinputs = "PRWinputs"
		if (!quiet) print("Waiting for job")
		NumberOfInputs = readBin(PRWcon, "integer")
		if (NumberOfInputs == -1) {
			print("Received exit signal.")
			return(0)
		}
		if (NumberOfInputs == -2) {
			print("Received global execute signal.")
			PRWglobal = 1
			NumberOfInputs = readBin(PRWcon, "integer")
		}

		if (!quiet) {
			mesg = paste("Receiving job with", NumberOfInputs, "inputs:")
			print(mesg)
		}
		if (NumberOfInputs != 0)
			for (PRWi in 1:NumberOfInputs) {
				LengthOfString = readBin(PRWcon, "integer")
				PRWname = readBin(PRWcon, "character", 1, LengthOfString)
				PRWinputs = c(PRWinputs, PRWname)
				if (!quiet) {
					PRWmesg = paste("Receiving input '", PRWname, "' :", sep="")
					print(PRWmesg)
				}
				if (exists(PRWname)) rm(list=PRWname)

				# Unserialize the inputs directly into the variables instead
				# of going through a setVar call to try to reduce memory
				# usage.  I do not know any other reason for or against
				# either this method or the previous method.
				PRWexpr = paste(PRWname, "<- try( unserialize(PRWcon) )")
				eval(parse(text=PRWexpr))
			}

		# Removed code to recovered from a unserialization failure.
		# It should never happen in a non-repeatable way without a failure
		# in the lower level networking.

		LengthOfString = readBin(PRWcon, "integer")
		PRWOutputName = readBin(PRWcon, "character", 1, LengthOfString)
		
		if (!quiet) print("Receiving expression:")
		PRWexpr = unserialize(PRWcon)
		if (!quiet) print(PRWexpr)
		answer = try(eval(PRWexpr))
		rm(list=PRWinputs, envir=parent.frame())
		t = .Call("R_serialize", answer, NULL, FALSE, NULL, PACKAGE="base")
		if (!quiet) print(paste("Length of answer =",length(t)))
		writeBin(as.integer(length(t)), PRWcon)
		rm(t)
		serialize(answer, PRWcon)
		rm(answer)
		if (PRWglobal == 0) rm(list=PRWOutputName)
#		gc()
	}
}

