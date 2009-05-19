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
/* peval.c  Parallel Evalution */
/* All functions in this file are only executed by the MAIN thread! */
/*  $Author: david $
	$Date: 2009-05-19 16:24:47 $
	$Revision: 1.56 $
 */
#include <stdio.h>
#include <Rinternals.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "scheduler.h"
#include "peval.h"

#define WORKER_CONNECTIONS_STRING "PE.WorkerConnections"

void *StartWorkerLoop(void *vpSocket);
void *StartFakeWorkerLoop(void *vpSocket);
char **FindInputs1(SEXP sxIn, char **cppCurrentList, int *ipListSize);
SEXP R_serialize(SEXP object, SEXP icon, SEXP ascii, SEXP fun);
SEXP R_unserialize(SEXP icon, SEXP fun);
SEXP My_unserialize(SEXP icon);
SEXP My_serialize(SEXP object, SEXP ascii);
int my_remove(char *cpName, SEXP rho);

int bSafeToGetResults = 0;		/* Referenced here and in eval() */
static SEXP sxGlobalLastReturned;
extern int iGlobalParallelEngineEnabled;	/* Used by Dprintf */

/* Given a job (as an R object), a socket, and an R environment, send the
 * job to the scheduler in a job packet via the socket, reading any needed
 * variables from the given environment.  Put the given "global" flag into
 * the job packet when it is created.
 */
int ParallelExecute(SEXP s, int iSocket, int iGlobal, SEXP rho) {
	job_packet *jppPacket;
	int iWait;

	if (TYPEOF(s) != LANGSXP) {
		fprintf(stderr, "%s:%d: Assert Warning: Non LANGSXP input to ParallelExecute.\n", __FILE__, __LINE__);
		return -1;
	}

/*	printf("Begining ParallelExecute at %f\n", GETTIME()); */
	/* Generate a job packet from a job (given as a LANGSXP) */
	jppPacket = CreateAndPopulateJobPacket(s, rho, iGlobal);

/*	printf("Job created at %f\n", GETTIME()); */
	/* Send (a pointer to) the packet to the scheduler. */
	write(iSocket, &jppPacket, sizeof(job_packet *));

	/* The scheduler may need this thread to serialize some variables. */
	/* Serialze the input objects as per the request from the scheduler 
	 * thread */
	iWait = SerializeDataRequest(jppPacket, rho, iSocket);

/*	printf("Job %p serialized at %f\n", jppPacket, GETTIME()); */
	/* Note: It is probably bad form to reference the copy of the name from
	 * the job_packet here (after it has been sent to the scheduler.  Of
	 * course, I do the same thing above with cppInVars.  It is safe because
	 * the scheduler can't delete the packet until either after it has
	 * returned or another packet has superseeded it's output. */
	CreateExtPtrObj(jppPacket->cpOutVar, rho);

	/* The scheduler will return -2 for the var ID as a signal that it
	 * is full.  The main thread will then wait for the single next variable
	 * to become available, before it returns to the user.
	 */
	if (iWait == -2)
		WaitForVariable("", iSocket, rho);

	return 0;
}

int SerializeDataRequest(job_packet * jppPacket, SEXP rho, int iSocket) {
	SEXP sxInput;
	int i;

	/* Begin by asking the scheduler the index of the first variable to
	 * serialize.  It will return a negative number if it doesn't need any
	 * (more) variables serialized.
	 */
	read(iSocket, &i, sizeof(int));

	/* Begin - Serialization loop */
	while (i >= 0) {
		data_packet *dpInput;
		if (i >= 0 && i < jppPacket->iNumInVars) {
			Dprintf(("PE: Serializing variable #%d (%s)\n", i,
					jppPacket->cppInVars[i]));
		} else {
			printf("PE: Error: Scheduler requested variable #%d to be serialized.\n", i);
			break;
		}
		PROTECT(sxInput = findVar(install(jppPacket->cppInVars[i]), rho));
		Dprintf(("Input %d (\"%s\") is of type %d\n", i,
					jppPacket->cppInVars[i], TYPEOF(sxInput)));

		/* Set the flag so that if a call is made to eval, it won't try
		 * to get any jobs from the scheduler.
		 */
		bSafeToGetResults = 0;
		if (TYPEOF(sxInput) == PROMSXP) {
			sxInput = eval(sxInput, rho);
			UNPROTECT(1);
			PROTECT(sxInput);
		}
		if (TYPEOF(sxInput) == SYMSXP) {
			sxInput = findVar(install(jppPacket->cppInVars[i]), rho);
			UNPROTECT(1);
			PROTECT(sxInput);

			Dprintf(("Variable \"%s\": type = %d\n",
						jppPacket->cppInVars[i], TYPEOF(sxInput)));
		}
		if (TYPEOF(sxInput) == EXTPTRSXP) {
			printf("ASSERT WARNING: Scheduler requested serialization of an EXTPTRSXP object!\n");
			printf("EXTPTR value = %p\n", (void *) EXTPTR_PTR(sxInput));
		}

		dpInput = CreateDataPacket(sxInput, jppPacket->cppInVars[i]);
		bSafeToGetResults = 1;
		if (dpInput == NULL) return -3;

		write(iSocket, &dpInput, sizeof(SEXP));
		read(iSocket, &i, sizeof(int));
		UNPROTECT(1);
	}
	/* End - Serialization loop */

	return i;
}

/* Create an empty, zeroed out job packet.
 * Returns the packet, unless malloc failed, in which case NULL is returned.
 */
job_packet *CreateJobPacket(void) {
	job_packet *jppPacket = (job_packet *) malloc(sizeof(job_packet));

	if (jppPacket == NULL) {
		fprintf(stderr,	"%s:%d: ERROR: Failed to allocate memory for job packet.\n", __FILE__, __LINE__);
		return NULL;
	}

	memset(jppPacket, 0x00, sizeof(job_packet));

	return jppPacket;
}

/* Given an R object, create a data packet containing the object.
 * Returns the data packet, or NULL upon error.
 */
data_packet *CreateDataPacket(SEXP sxInput, char *cpName) {
	SEXP sxSerial;
	data_packet *dpInput;

#ifdef PARALLEL_R_PACKAGE
	PROTECT(sxSerial = My_serialize(sxInput, ScalarLogical(0)));
#else
	PROTECT(sxSerial = R_serialize(sxInput, R_NilValue,
			ScalarInteger(2), R_NilValue));
#endif
	if (sxSerial == R_NilValue) {
		fprintf(stderr, "Serialization of variable %s failed!\n", cpName);
	}

	/* Begin:  Create the data packet */
	dpInput = (data_packet *) malloc(sizeof(data_packet));
	if (dpInput == NULL) {
		fprintf(stderr, "%s:%d: ERROR: Failed to allocate memory for packet.\n",
				__FILE__, __LINE__);
		return NULL;
	}
	dpInput->iNumRef = 1;
	dpInput->iLen = LENGTH(sxSerial);
	dpInput->vpData = (void *) malloc((size_t) dpInput->iLen);
	memcpy(dpInput->vpData, RAW(sxSerial),
			dpInput->iLen);
	if (cpName == NULL) {
		dpInput->cpName = NULL;
	} else {
		dpInput->cpName = strdup(cpName);
	}
	/* End:  Create the data packet */

	Dprintf(("Peval: Serialized length = %d\n", dpInput->iLen));

	UNPROTECT(1);

	return dpInput;
}

/* Create a job packet from a job given in the form of a PE call
 * (as a LANGSXP), the environment frame of the call, and the global flag.
 */
job_packet *CreateAndPopulateJobPacket(SEXP s, SEXP rho, int iGlobal) {
	job_packet *jppPacket;
	int i;
	static int iSJobNum = 0;

	jppPacket = CreateJobPacket();
	if (jppPacket == NULL) return NULL;

	/* Begin:  Construct the job packet */
	jppPacket->cppInVars = FindInputs1(CDR(CDR(s)), NULL,
			&(jppPacket->iNumInVars));
	jppPacket->cpOutVar = strdup(R_CHAR(PRINTNAME(CAR(CDR(s)))));
	jppPacket->iJobNum = iSJobNum++;
	jppPacket->iGlobal = iGlobal;
	jppPacket->dppInputs = malloc(sizeof(data_packet *) *
			jppPacket->iNumInVars);
	jppPacket->dpResult = NULL;
	jppPacket->rho = rho;
	
	jppPacket->dpCall = CreateDataPacket(s, NULL); /* TODO  Maybe "Call" ? */

	for (i = 0; i < jppPacket->iNumInVars; i++) {
		jppPacket->dppInputs[i] = NULL;
	}
	/* End:  Construct the job packet */

	return jppPacket;
}

/* Initialize the Parallel Execution system.
 * Spawn the scheduler and worker threads and set up the communication
 * sockets.
 * Note:  This code is copied from test_scheduler.c, CVS version 1.2
 */
int InitializeParallelExecution(int iNumWorkers, int *ipMySocket, SEXP rho) {
	int *ipWorkerSockets;
	int *ipSchedulerSockets;
	int iMaxInFlight;

	/* Open Sockets */
	ipWorkerSockets = (int *) malloc(sizeof(int) * iNumWorkers * 2);
	ipSchedulerSockets = (int *) malloc(sizeof(int) * (iNumWorkers + 1));
	if (ipWorkerSockets == NULL || ipSchedulerSockets == NULL) {
		fprintf(stderr, "%s:%d: ERROR: Failed to allocate memory for sockets\n",
				__FILE__, __LINE__);
		return -2;
	}

	/* Create the socketpairs to communicate between the scheduler and each
	 * of the other threads.
	 */
	if (CreateSocketPairs(ipMySocket, ipSchedulerSockets, ipWorkerSockets,
				iNumWorkers, rho) != 0) {
		printf("Failed to create inter-thread communication sockets.\n");
		return -3;
	}

	/* Spawn the Scheduler thread and Worker threads */
	if (SpawnAllThreads(iNumWorkers,ipSchedulerSockets,ipWorkerSockets) != 0) {
		printf("Failed to create threads.\n");
		return -4;                
	}

	/* Wait for the Scheduler thread to signal it's ready */ 
	if (read(*ipMySocket, &iMaxInFlight, sizeof(int)) != sizeof(int)) {
		perror("Failed to read ready signal from scheduler\n");
		return -5;
	}

	/* By the time that the schedule has signalled that it is ready, it is
	 * safe to free the lists.
	 */
	free(ipWorkerSockets);
	free(ipSchedulerSockets);

	printf("Info: Max in flight = %d\n", iMaxInFlight);

	return 0;
}

/* This function spawns the scheduler thread and the requested number of
 * worker threads. */
int SpawnAllThreads(int iNumWorkers, int *ipSchedulerSockets,
		int *ipWorkerSockets) {
	pthread_t tThread;
	int *ipArgs, i;

	/*spawn the various threads, starting with the scheduler thread. */
	ipArgs = (int *) malloc(sizeof(int) * (iNumWorkers + 2));
	if (ipArgs == NULL) {
		fprintf(stderr, "%s:%d:ERROR: Failed to allocate memory for args.\n",
				__FILE__, __LINE__);
		return -4;
	}

	ipArgs[0] = iNumWorkers;
	for (i = 0; i < iNumWorkers + 1; i++)
		ipArgs[i + 1] = ipSchedulerSockets[i];

	if (pthread_create(&tThread, NULL, StartSchedulerLoop, ipArgs)
			!= 0) {
		fprintf(stderr, "Failed to create scheduler thread.\n");
		return -4;
	}

	pthread_detach(tThread);

	/* Spawn the worker threads. */
	for (i = 0; i < iNumWorkers; i++) {
		if (pthread_create(&tThread, NULL, StartWorkerLoop,
				ipWorkerSockets + 2 * i) != 0) {
			fprintf(stderr, "Failed to create worker thread %d.\n", i);
		}
		pthread_detach(tThread);
	}

	return 0;
}

/* Create socketpairs to communicate between: 
 * A. The main thread and the scheduler thread
 * B. The scheduler thread and worker threads
 * In addition, get the interprocess communication sockets from an R variable.
 */ 
int CreateSocketPairs(int *ipMySocket, int *ipSchedulerSockets,
		int *ipWorkerSockets,int iNumWorkers, SEXP rho) {
	int ipSockTemp[2], i;
	SEXP sxWorkerConnections;

	/* First, make the socket to go between the main thread and the
	 * scheduler thread.
	 */
	if (socketpair(AF_LOCAL, SOCK_DGRAM, 0, ipSockTemp) != 0) {
		perror("Failed to open main <-> scheduler socket: ");
		return -1;
	}

	*ipMySocket = ipSockTemp[0];
	ipSchedulerSockets[0] = ipSockTemp[1];

	Dprintf(("InitializePE: rho = %p\n", (void *) rho));

	/* Then, make the sockets to go between the scheduler thread and the
	 * worker threads.
	 */
	sxWorkerConnections = findVar(install(WORKER_CONNECTIONS_STRING), rho);
	if (sxWorkerConnections == R_NilValue) {
		printf("InitializePE: Didn't find worker sockets.\n");
		return -3;
	}

	for (i = 0; i < iNumWorkers; i++) {
		if (socketpair(AF_LOCAL, SOCK_DGRAM, 0, ipSockTemp) != 0) {
			perror("Failed to open scheduler <-> worker thread: ");
			fprintf(stderr, " (Socket #%d)\n", i + 1);
			return -3;
		}

		ipWorkerSockets[2 * i] = ipSockTemp[0];
		ipWorkerSockets[2 * i + 1] = REAL(sxWorkerConnections)[i];
		ipSchedulerSockets[i + 1] = ipSockTemp[1];
	}

	return 0;
}

/* This function does NOT check the "bJobsFinished" flag, but calling it
 * when there are no jobs available causes no problems (just minor overhead).
 */
int GetResultsFromScheduler(int iSocket, SEXP rho, const char *cpCmpName) {
	char *cpName = NULL;
	job_packet *jppPacket;
	data_packet *dpResult;
	int bFound = 0;
	int r;

	jppPacket = CreateJobPacket();
	if (jppPacket == NULL) 
		return -1;

	jppPacket->iNumInVars = -1;

	/* Send (a pointer to) the packet to the scheduler. */
	r = write(iSocket, &jppPacket, sizeof(job_packet *));
	if (r != sizeof(job_packet *)) {
		perror("PEval: Failed to send 'ready for results' signal");
		return -2;
	}

	r = read(iSocket, &cpName, sizeof(char *));
	if (r != sizeof(char *)) {
		perror("PEVal: Failed to receive name of result");
		/* Attempt to continue.... */
	}

	free(jppPacket);

	/* Start Loop - Return all of the available outputs */
	while (cpName != NULL) {
		if (strcmp(cpName, "") == 0) {
			fprintf(stderr,	"%s:%d:ASSERT WARNING: Empty string for variable name!\n",
					__FILE__, __LINE__);
			break;
		}

		/* If a specific variable is being looked for and this is it, then
		 * set the flag saying that it was found.
		 */
		if (cpCmpName != NULL && strcmp(cpCmpName, cpName) == 0)
			bFound = 1;

		Dprintf(("Reading result \"%s\"\n", cpName));

		/* Read the data packet from the scheduler for the output. */
		read(iSocket, &dpResult, sizeof(data_packet *));

		/* Read the rho value for the job from the scheduler */
		read(iSocket, &rho, sizeof(SEXP));

		sxGlobalLastReturned = ReturnResult(dpResult, cpName, rho);

		Dprintf(("Read, unserialized and returned.\n"));

		/* Request the next job from the scheduler. */
		write(iSocket, &r, sizeof(int));
		r = read(iSocket, &cpName, sizeof(char *));
		if (r != sizeof(char *)) {
			perror("Failed to read name for next loop iteration");
			break;
		}
	}				/* Endof  while loop */

	return bFound;
}

/* Return a variable (given as a data packet) to R's workspace.
 * Returns the (unserialized) variable.
 */
SEXP ReturnResult(data_packet *dpResult, char *cpName, SEXP rho) {
	int iLen;
	SEXP sxResult;
	SEXP sxTemp, sxExtPtr;

	iLen = dpResult->iLen;
	PROTECT(sxResult = allocVector(RAWSXP, iLen));
	memcpy(RAW(sxResult), dpResult->vpData, dpResult->iLen);

	/* Unserialize the output and install it into R's workspace. */
#ifdef PARALLEL_R_PACKAGE
	PROTECT(sxTemp = My_unserialize(sxResult));
#else
	PROTECT(sxTemp = R_unserialize(sxResult, R_NilValue));
#endif
	sxExtPtr = findVar(install(cpName), rho);
	if (sxExtPtr == R_NilValue) {
		printf("PEval: Failed to find ExtPtr placeholder\n");
	} else if (TYPEOF(sxExtPtr) != EXTPTRSXP) {
		printf("PEval: Found non ExtPtr instead of placeholder!\n");
	} else {
		SET_TAG(sxExtPtr, sxTemp);
	}
	setVar(install(cpName), sxTemp, rho);
	if (findVar(install(cpName), rho) != sxTemp) {
		printf("PEval: Failed to save result.  Retrying...\n");
		my_remove(cpName, rho);
		setVar(install(cpName), sxTemp, rho);
	}

	if (TYPEOF(findVar(install(cpName), rho)) == EXTPTRSXP) {
		printf("ASSERT ERROR: Failed to write variable \"%s\" into %p\n",
				cpName, (void *) rho);
	}

	UNPROTECT(2);

	return sxTemp;
}

/* If cpName is a specific string, wait for a variable of that name.
 * If cpName is the empty string ("" or "\0"), wait for the next variable.
 * If cpName is NULL wait for all jobs to finish.
 */
SEXP WaitForVariable(const char *cpName, int iSocket, SEXP rho) {
	job_packet *jppPacket;
	int bDone = 0;
	sxGlobalLastReturned = R_NilValue;

	while (!bDone) {
		int iNumJobs = 0;
		int iFound;

		jppPacket = CreateJobPacket();
		if (jppPacket == NULL) 
			return R_NilValue;

		jppPacket->iNumInVars = -2;

		/* Send (a pointer to) the packet to the scheduler. */
		write(iSocket, &jppPacket, sizeof(job_packet *));

		/* Wait for signal from scheduler that a job is ready. */
		read(iSocket, &iNumJobs, sizeof(int));

		free(jppPacket);

		if (iNumJobs == 0) return R_NilValue;

		iFound = GetResultsFromScheduler(iSocket, rho, cpName);

		if (iFound < 0) break;

		if (cpName == NULL) continue;
		if (*cpName == '\0') bDone = 1;
		if (iFound == 1) bDone = 1;
	}

	return sxGlobalLastReturned;
}

#ifdef PARALLEL_R_PACKAGE
#define R_PEngineToken R_NilValue
#endif

/* Given a name and an environment, create a EXT_PTR object with the given
 * name in the given environment.  This is a place-holder object used to
 * detect when a non-PE operation is trying to use a variable that was the
 * output of a PE operation.  (In other words, this removes the need for the
 * POBJ function in the main-line code.)
 */
SEXP CreateExtPtrObj(char *cpName, SEXP rho) {
	SEXP sxTmp;
	SEXP sxName;

	if (cpName == NULL) {
		printf("Warning: Job has NULL output name!\n");
		return R_NilValue;
	}

	/* It turns out that at this time (R version 1.9.0), EXTPTRSXP objects
	 * are NOT handled correctly by the garbage collector.  In particular,
	 * the garbage collector does NOT respect the flag telling it to
	 * that the EXT_PTR should be protected.
	 * In order to get around that, I make both the EXT_PTR and the PROT_PTR
	 * point to the sxName object.
	 */
	PROTECT(sxName = mkChar(cpName));
	PROTECT(sxTmp = R_MakeExternalPtr(sxName, R_PEngineToken, sxName));
	setVar(install(cpName), sxTmp, rho);

	if (findVar(install(cpName), rho) != sxTmp) {
		printf("ASSERT WARNING: setVar failed, trying again...\n");
		my_remove(cpName, rho);
		setVar(install(cpName), sxTmp, rho);
	}
	UNPROTECT(2);

	return sxTmp;
}

