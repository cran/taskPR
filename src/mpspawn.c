/* ****************************************************************************
*   Copyright (C) 2004  Oak Ridge National Laboratory
*
*   This program is free software; you can redistribute it and/or
*   modify it under the terms of the GNU General Public License
*   as published by the Free Software Foundation, version 2.
*
*   This program is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.
*
*   You should have received a copy of the GNU General Public License
*   along with this program; if not, write to the Free Software
*   Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
**************************************************************************** */
/* mpspawn.c  MPI Spawn  -  Use MPI_Comm_spawn to spawn a number of processes,
 * 		and then establish a communications link with them.
 */
/*	$Author: bauer $
	$Date: 2004/07/16 18:15:17 $
	$Revision: 1.15 $
 */

#include <stdio.h>
#include <mpi.h>
#include "Rinternals.h"

static MPI_Comm childComm = 0;

/* Using the MPI-2 function MPI_Comm_spawn, spawn iNumber of copies of the
 * given program passing the given arguments.  Initializes MPI if needed.
 */
int SpawnMPIProcesses(char *cpProgramName, int iNumber, char **cppArguments) {
	int bIsMPIInitialized = 0;
	int iLen = 0;
	int i;

	/* First, check to see if MPI is initialized, and if not initialize it. */
	MPI_Initialized(&bIsMPIInitialized);

	if (!bIsMPIInitialized) {
		if (MPI_Init(NULL, NULL) != MPI_SUCCESS) {
			printf("Failed to initialize MPI!\n");
			return -1;
		}
		MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
	}

	/* Next, examine the arguments and replace the "NULL" end tag with
	 * a real NULL.
	 */
	if (cppArguments == NULL) {
		printf("Warning:  Arguments list is NULL!\n");
	} else {
		iLen = 0;	/* Redundant */
		while (cppArguments[iLen] != NULL &&
				strcmp(cppArguments[iLen], "NULL") != 0) iLen++;
		cppArguments[iLen] = NULL;
		i = 0;
		printf("Spawning \"%s", cpProgramName);
		while (cppArguments[i] != NULL) printf(" %s", cppArguments[i++]);
		printf("\"\n");
	}

	/* Make the actual spawn call */
	if (MPI_Comm_spawn(cpProgramName, cppArguments, iNumber, MPI_INFO_NULL, 0,
			MPI_COMM_WORLD, &childComm, MPI_ERRCODES_IGNORE) != MPI_SUCCESS) {
		printf("Failed to spawn %d children (\"%s\")!\n", iNumber,
				cpProgramName);
		return -2;
	}

	/* Fix the arguments array, because R can't handle the NULL. */
	if (iLen != 0) cppArguments[iLen] = "";	/* Prevents a Segfault. */

	printf("Spawn of %d children successful\n", iNumber);

	/* Ask MPI to not kill the program on errors.
	 * NOTE:  This is an MPI-2 function which not all implementations
	 * support! */
	if (MPI_Comm_set_errhandler(childComm, MPI_ERRORS_RETURN) != MPI_SUCCESS) {
		printf("Failed to set error handler for parent-child comm\n");
		return -3;
	}

	return 0;
}

/* Wrapper function which can be called from R */
int R_SpawnMPIProcesses(char **cppProgram, int *ipNumber, char **cppArguments) {
	return SpawnMPIProcesses(*cppProgram, *ipNumber, cppArguments);
}

/* This function is used to establish communication with the worker processes,
 * after they have been spawned.  The parameters have an extra pointer level
 * for compatibility with R.
 */
int NotifySpawnedMPIProcesses(char **cppHost, int *ipNumber) {
	int bIsMPIInitialized = 0;
	int iLen;
	int iStatus;

	if (childComm == 0) {
		error("child processes have not been spawned");
	}

	/* Begin by making sure that MPI has been initialized */
	MPI_Initialized(&bIsMPIInitialized);
	if (bIsMPIInitialized == 0) {
		error("MPI has not been initialized");
	}

	if (cppHost == NULL) error("Host name array was NULL");
	if (*cppHost == NULL) error("Host name was NULL");

	iLen = strlen(*cppHost) + 1;
	if (iLen == 1) error("Host name is empty");

	/* Send the host name to the worker process to facilitate making a
	 * socket connection.
	 */
	iStatus = MPI_Send(&iLen, 1, MPI_INT, *ipNumber, 101, childComm);
	if (iStatus != MPI_SUCCESS) {
		printf("Failed to send message length to child %d\n", *ipNumber);
		return -1;
	}
	iStatus = MPI_Send(*cppHost, iLen, MPI_CHAR, *ipNumber, 102, childComm);
	if (iStatus != MPI_SUCCESS) {
		printf("Failed to send host name to child %d\n", *ipNumber);
		return -2;
	}
	iStatus = MPI_Recv(&iLen, 1, MPI_INT, *ipNumber, 103, childComm,
				MPI_STATUS_IGNORE);
	if (iStatus != MPI_SUCCESS) {
		printf("Failed to receive acknowledgement from child %d\n", *ipNumber);
		return -3;
	}

	if (iLen == 0) error("Child process reported a problem");

	return 0;
}

/* To be called by spawned worker processes.
 * Initialize MPI (if needed) and establish communication with the parent
 * process.  Return the hostname of the parent process for use in opening
 * a socket connection.
 */
SEXP PRWorkerInit(void) {
	int bIsMPIInitialized = 0;
	char *cpName;
	MPI_Comm mcParent;
	/* MPI_Comm intercomm;  */
	int iRank;
	int iLen;
	SEXP ret;

	MPI_Initialized(&bIsMPIInitialized);

	if (!bIsMPIInitialized) {
		if (MPI_Init(NULL, NULL) != MPI_SUCCESS) {
			printf("Failed to initialize MPI!\n");
			return R_NilValue;
		}
		MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
	}

	MPI_Comm_rank(MPI_COMM_WORLD, &iRank);

	printf("Child %d:  MPI is initialized\n", iRank);

	MPI_Barrier(MPI_COMM_WORLD);

	/* Get the Parent Communicator */
	if (MPI_Comm_get_parent(&mcParent) != MPI_SUCCESS) {
		printf("Child: Failed to get parent communicator!\n");
		return R_NilValue;
	}

	printf("Child:  Got parent\n");
	fflush(stdout);

	if (mcParent == MPI_COMM_NULL) {
		char *cpNoParent = "No Parent found.  This routine can only be"
				" called by a spawned Parallel-R worker process\n";
		fprintf(stderr, cpNoParent);
		return R_NilValue;
	}

	/* Get the hostname of the parent process. */
 	MPI_Recv(&iLen, 1, MPI_INT, 0, 101, mcParent, MPI_STATUS_IGNORE);

	cpName = (char *) malloc(sizeof(char) * iLen);
	if (cpName == NULL) {
		printf("%s:%d: ERROR: Failed to allocate memory!\n", __FILE__,
				__LINE__);
		return R_NilValue;
	}
	memset(cpName, 0x00, sizeof(char) * iLen);

	MPI_Recv(cpName, iLen, MPI_CHAR, 0, 102, mcParent, MPI_STATUS_IGNORE);

 	printf("Child:  Received parent's name\n");

	MPI_Send(&iLen, 1, MPI_INT, 0, 103, mcParent);

	ret = mkString(cpName);
	free(cpName);
 	return ret;
}
