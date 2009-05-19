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
/* Worker thread for Parallel-R.  The worker threads take jobs from the
 * scheduler and pass them on to worker processes.  Each worker thread
 * communicates with a single worker process.
 */
/*  $Author: david $
    $Date: 2009-05-19 16:24:47 $
    $Revision: 1.29 $
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <string.h>
#include "scheduler.h"

int ReceiveBuffer(int iSocket, char *vpData, int iLen);
void PrintJobReceivedMessage(job_packet *jppPacket, int iID);
void *MainWorkerLoop(int iWorkerSocket, int iSchedulerSocket);
int SendJobToWorker(job_packet *jppPacket, int iSocket, int iID);
int SendInputsToWorker(job_packet *jppPacket, int iSocket, int iID);
data_packet *ReceiveResult(char *cpName, int iJobNum, int iSocket, int iID);
data_packet *CreateResultPacket(int iLen, char *cpName);

extern int iGlobalParallelEngineEnabled;

/* This is the starting function for the worker thread.
 * It takes only its two communication sockets as parameters.
 */
void *StartWorkerLoop(void *vpSockets) {
	int iSchedulerSocket;
	int iWorkerSocket;
	int i;
	long lFFlags;

	if (vpSockets == NULL) return NULL;

	iSchedulerSocket = ((int *) vpSockets)[0];
	iWorkerSocket = ((int *) vpSockets)[1];

	/* Force the socket to be non-blocking and low-latency.
	 * Forcing the socket to be low-latency seems to help in some cases,
	 * but not in others.  I may make this configurable (or at least more
	 * intelligent) in the future.
	 */
	lFFlags = fcntl(iWorkerSocket, F_GETFL);
	fcntl(iWorkerSocket, F_SETFL, lFFlags & !O_NONBLOCK);
	i = 1;
	setsockopt(iWorkerSocket, 6 /* TCP */, TCP_NODELAY, &i, sizeof(i));

	/* Send ready signal to scheduler */
	write(iSchedulerSocket, &iSchedulerSocket, sizeof(int));

	Dprintf(("Worker (%x, %x) Reporting ready.\n", iSchedulerSocket,
				iWorkerSocket));

	return MainWorkerLoop(iWorkerSocket, iSchedulerSocket);
}

/* The worker threads simply pass jobs from the scheduler to the worker
 * processes, and then return the results back to the scheduler.
 * The worker threads have two purposes related to seperating communication
 * with the workers from the scheduling of jobs.  First, the type of
 * communication (sockets, shared memory, MPI) doesn't affect the scheduler.
 * Second, the scheduler can perform very quick communication with the worker
 * threads, while the slow communication to the worker processes is handled in
 * each worker thread (often simultaneously).
 */
void *MainWorkerLoop(int iWorkerSocket, int iSchedulerSocket) {
	job_node *jnpNode;
	job_packet *jppPacket;
	int r;
	int iJobNum;

	do {
		r = 1;
#ifdef TCP_CORK
		setsockopt(iWorkerSocket, 6 /* TCP */, TCP_CORK, &r, sizeof(r));
#endif

		/* Get the job from the Scheduler thread */
		read(iSchedulerSocket, &jnpNode, sizeof(job_node *));

		if (jnpNode == NULL) {
			/* Received the signal to exit.  Tell the worker process that
			 * the this thread is exiting and then exit.
			 */
			r = -1;
			send(iWorkerSocket, &r, sizeof(int), 0);
			return NULL;
		}
		
		jppPacket = jnpNode->job;
		iJobNum = jppPacket->iJobNum;

/*		printf("Job %p in worker thread at %f\n", jppPacket, GETTIME()); */
		/* If verbose execution was enabled, print out the message. */
		if (iGlobalParallelEngineEnabled > 1)
			PrintJobReceivedMessage(jppPacket, iSchedulerSocket);

		/* Send the job to the worker process */
		SendJobToWorker(jppPacket, iWorkerSocket, iSchedulerSocket);

		r = 1;
		setsockopt(iWorkerSocket, 6 /* TCP */, TCP_NODELAY, &r, sizeof(r));

/*		printf("Job %p sent to worker at %f\n", jppPacket, GETTIME()); */

		/* Receive the output from the worker process */
		jppPacket->dpResult = ReceiveResult(jppPacket->cpOutVar,
				jppPacket->iJobNum, iWorkerSocket, iSchedulerSocket); 

/*		printf("Job %p returned to worker thread at %f\n", jppPacket, GETTIME()); */
		Dprintf(("Worker (%x) finished job %d\n", iSchedulerSocket, iJobNum));

		/* Return the result to the scheduler thread */
		write(iSchedulerSocket, &jnpNode, sizeof(job_node *));
	} while (1);

	return NULL;	/* Not Reached */
}

/* Get the result from the worker process. */
data_packet *ReceiveResult(char *cpName, int iJobNum, int iSocket, int iID) {
	int r, iLen;
	data_packet *dpResult;
	char cpBuf[256];
/*	double t1, t2; */

	r = 1;
#ifdef TCP_QUICKACK
	setsockopt(iSocket, 6 /* TCP */, TCP_QUICKACK, &r, sizeof(r));
#endif

	/* Receive the output: length(data), data */
	r=recv(iSocket, &iLen, sizeof(int), 0);
	if (r < sizeof(int)) perror("Error receiving length of result");

/*	printf("Job %d about to return at %f\n", iJobNum, GETTIME()); */

	dpResult = CreateResultPacket(iLen, cpName);
	if (dpResult == NULL) {
		printf("Worker: Failed to create output data packet! (%d)\n", iLen);
		return NULL;
	}

#ifdef TCP_QUICKACK
	setsockopt(iSocket, 6 /* TCP */, TCP_QUICKACK, &r, sizeof(r));
#endif

/*	t1 = GETTIME(); */
	r = ReceiveBuffer(iSocket, dpResult->vpData, iLen);
/*	t2 = GETTIME(); */
	sprintf(cpBuf,"Worker (%x) received %d of %d bytes of result of job %d",
			iID, r, iLen, iJobNum);
	if (r < iLen) perror(cpBuf);
	else Dprintf(("%s - Good\n", cpBuf));

/*	printf("Receiving %d byte result packet took %.0f us\n", iLen, t2 - t1); */
	return dpResult;
}

/* Sends the job node to the worker process*/
int SendJobToWorker(job_packet *jppPacket, int iSocket, int iID) {
	int iLen, r;
	char cpBuf[64];

	/* If this is a global call, then inform the worker process. */
	if (jppPacket->iGlobal) {
		iLen = -2;
		send(iSocket, &iLen, sizeof(int), 0);
	}

	/* Send the number of inputs */
	r = send(iSocket, &(jppPacket->iNumInVars), sizeof(int), 0);

	sprintf(cpBuf, "Worker (%x) sent number of inputs:", iID);
	if (r < sizeof(int)) perror(cpBuf);

	/* Send inputs pertaining to this job to the worker process */
	SendInputsToWorker(jppPacket, iSocket, iID);

	/* Send the name of the output (for cleanup purposes) */
	iLen = strlen(jppPacket->cpOutVar) + 1;
	send(iSocket, &iLen, sizeof(int), 0);
	send(iSocket, jppPacket->cpOutVar, iLen, 0);

	/* Send the expression to execute. */
	send(iSocket, jppPacket->dpCall->vpData, jppPacket->dpCall->iLen, 0);

	return 0;
}

/* Print the "worker x received job ..." message.
 * This function does NOT check the verbosity level.
 */
void PrintJobReceivedMessage(job_packet *jppPacket, int iID) {
	char cpBuf[256];
	int iStrLen, i;
	int iJobNum = jppPacket->iJobNum;

	/* Print out notification that the job has been received.
	 * Use sprintf/snprintf followed by printf to ensure that the
	 * line is printed all at once.
	 */
	iStrLen = sprintf(cpBuf, "Worker (%x) received job %d: %s = f( ",
			iID, iJobNum, jppPacket->cpOutVar);

	for (i = 0; i < jppPacket->iNumInVars; i++) {
		iStrLen = strlen(cpBuf);
		if (i == jppPacket->iNumInVars - 1) {
			iStrLen += snprintf(cpBuf + iStrLen, 256 - iStrLen, "%s )",
					jppPacket->cppInVars[i]);
		} else {
			iStrLen += snprintf(cpBuf + iStrLen, 256 - iStrLen, "%s, ",
					jppPacket->cppInVars[i]);
		}
	}

	if (iStrLen > 256) {
		printf("%s... )\n", cpBuf);
	} else {
		printf("%s\n", cpBuf);
	}

	return;
}

/* Send the inputs of the given job to the given socket. */
int SendInputsToWorker(job_packet *jppPacket, int iSocket, int iID) {
	int i, iLen;
	int r;

	/* Send the inputs: length(name), name, data */
	for (i = 0; i < jppPacket->iNumInVars; i++) {
		iLen = strlen(jppPacket->cppInVars[i]) + 1;
		r=send(iSocket, &iLen, sizeof(int), 0);
		if (r < sizeof(int)) {
			perror("Failed to send size of string");
		}

		r=send(iSocket, jppPacket->cppInVars[i], iLen, 0);
		if (r < iLen) {
			perror("Failed to send string");
		}

		iLen = jppPacket->dppInputs[i]->iLen;

		Dprintf(("Worker (%x) sending data (%d)\n", iID, iLen));
		r=send(iSocket, jppPacket->dppInputs[i]->vpData, iLen, 0);
		if (r < iLen) {
			perror("Failed to send data");
		}
	}	/* Endof  for(each input) */

	return 0;
}

/* Create a data packet, allocating memory of the given length.
 * Duplicates code in CreateDataPacket in peval.c
 */
data_packet *CreateResultPacket(int iLen, char *cpName) {
	data_packet *dpResult;

	if (iLen < 0) return NULL;

	dpResult = (data_packet *) malloc(sizeof(data_packet));
	if (dpResult == NULL) {
		fprintf(stderr, "%s:%d: Failed to allocate memory for packet!\n",
				__FILE__, __LINE__);
		return NULL;
	}

	dpResult->iNumRef = 1;
	dpResult->iLen = iLen;
	dpResult->vpData = malloc((size_t) iLen);
	dpResult->cpName = strdup(cpName);

	if (dpResult->vpData == NULL) {
		free(dpResult);
		return NULL;
	}

	return dpResult;
}

/* Receive a large block of data.  Space must be preallocated by the caller. */
int ReceiveBuffer(int iSocket, char *vpData, int iLen) {
	int iReceived;
	int iTotalSize = iLen;

	if (vpData == NULL) return -1;
	if (iLen < 0) return -1;

	while (iLen > 0) {
		iReceived = recv(iSocket, vpData, iLen, MSG_WAITALL);
		if (iReceived < 1) return iReceived;
		iLen -= iReceived;
		vpData += iReceived;
	}

	if (iLen != 0) {
		fprintf(stderr, "%s:%d:ASSERT WARNING:  Apparently read more from file than the buffer size.\n", __FILE__, __LINE__);
	}

	return iTotalSize;
}

