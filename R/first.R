.First.lib <- function (lib, pkg) {
    library.dynam("taskPR", pkg, lib)
    if (length(system("lamnodes", TRUE, TRUE)) == 0) {
      if (interactive()) {
        print("The LAM/MPI daemon does not appear to be running.", quote=FALSE)
		print("If you want the ability to spawn worker processes,", quote=FALSE)
        print("please use the lamboot command to start the daemon.", quote=FALSE)
        print("(From inside R, use the command 'system(\"lamboot\")')", quote=FALSE)
      }
    }
}
                                                                                
.Last.lib <- function(libpath){
    library.dynam.unload("taskPR", libpath)
}

