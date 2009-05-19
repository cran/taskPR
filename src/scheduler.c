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
/* scheduler.c  Task Scheduler.  ALL functions pertaining to the task scheduler
 * thread should be defined in this file.
 * The ( O(1) ) task scheduler gets jobs from the main thread and passes them
 * on to worker threads.  It takes finished jobs from worker threads and
 * pases them back to the main thread.  In the middle, it does all necessary
 * dependency handling, memory management, and object tracking.
 */
/*  $Author: bauer $
    $Date: 2004-07-26 13:26:30 $
    $Revision: 1.40 $
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#define _GNU_SOURCE		/* For the reentrant versions of hcreate/hsearch/etc */
#include <search.h>

#include "scheduler.h"
#include "hashtable.h"

#define MAIN_THREAD_SERIALIZES
#define THREAD_STACK_IS_A_QUEUE

static job_node *jnpReadyQueueFirst = NULL;
static job_node *jnpReadyQueueLast = NULL;
static GE_HashTbl htVariableTable;	/* The "Variable Lookup Table" */
static GE_HashTbl htDataTable;	/* A table used with serialized data */
static thread_node *tnpThreadStack = NULL;
#ifdef THREAD_STACK_IS_A_QUEUE
static thread_node *tnpThreadQueueLast = NULL;
#endif
static job_node *jnpFinishedList = NULL;
static int iJobsInWorkers;
static int iJobsInFlight;
static int iMaxJobsInFlight;
static int bMainThreadIsWaiting = 0;

int bJobsFinished = 0;	/* bJobsFinished is read-only in main thread,
						 * read/write in scheduler thread. (No race)
						 */
int iGlobalParallelEngineEnabled = 0;

/* Given a job packet, create a default (i.e, mostly zeroed out) job node
 * to contain the job packet.
 */
job_node *CreateNode(job_packet *jppPacket) {
	job_node *jnpNode;

	if (jppPacket == NULL) return NULL;

	jnpNode = (job_node *) malloc(sizeof(job_node));
	if (jnpNode == NULL) {
		fprintf(stderr, "%s:%d:ERROR: Failed to allocate memory for job node\n",
				__FILE__, __LINE__);
		return NULL;
	}

	jnpNode->job = jppPacket;
	jnpNode->children = NULL;
	jnpNode->iNumChildren = 0;
	jnpNode->iNumParents = 0;
	jnpNode->next = NULL;

	if (jnpNode->job->cpOutVar == NULL) {
		fprintf(stderr, "%s:%d: ASSERT WARNING:  Job has no output!\n",
				__FILE__, __LINE__);
	}

	if (jnpNode->job->dpResult != NULL) {
		fprintf(stderr, "%s:%d: Assert Warning:  Incoming job had none-null result.  Setting to NULL.\n", __FILE__, __LINE__);
		jnpNode->job->dpResult = NULL;
	}

	return jnpNode;
}

/* Given a data packet, look in the serialized data table to see if a
 * duplicate packet is already present.  If so, discard the incoming packet and
 * return a reference to the old packet.  Otherwise, return the given packet.
 */
data_packet *InputDataPacket(data_packet *dpInputPacket) {
	data_packet *dpOutPacket;
	char *cpName = dpInputPacket->cpName;

	if (cpName == NULL) return dpInputPacket;

	/* Check for the variable in the table. */
	dpOutPacket = HSearch(cpName, NULL, FIND, htDataTable);
	if (dpOutPacket != NULL) {
		/* The name was found in the table.  Now, compare the data. */
		if (dpOutPacket->iLen == dpInputPacket->iLen) {
			if (memcmp(dpOutPacket->vpData, dpInputPacket->vpData,
					dpInputPacket->iLen) != 0)
				dpOutPacket = NULL;
		} else  dpOutPacket = NULL;
	}
	if (dpOutPacket == NULL) {
		/* Not found, put into table. */
		if (HSearch(cpName, dpInputPacket, ENTER, htDataTable) == NULL) {
			fprintf(stderr, "%s:%d: Assert Warning: Failed to add item into data table.  Ignoring.\n", __FILE__, __LINE__);
		}
		return dpInputPacket;
	} else {
		/* We have a duplicate packet. */
		Dprintf(("Scheduler: Info: Using already present \"%s\" packet\n",
					dpInputPacket->cpName));
		dpOutPacket->iNumRef += dpInputPacket->iNumRef;
		DeleteDataPacket(dpInputPacket);
	}

	return dpOutPacket;
}

/* Given a data packet, decrease the reference count and delete the packet,
 * if the reference count is now zero.
 */
int DeleteDataPacket(data_packet *dpPacket) {
	if (dpPacket == NULL || dpPacket->vpData == NULL) return -1;
	if (dpPacket->iNumRef < 1) {
		fprintf(stderr, "ASSERT ERROR: Reference count of packet was %d!\n",
				dpPacket->iNumRef);
		return -2;
	}

	if (--dpPacket->iNumRef == 0) {
		if (HSearch(dpPacket->cpName, NULL, FIND, htDataTable) == dpPacket) {
			HDelete(dpPacket->cpName, htDataTable);
		}
		free(dpPacket->vpData);
		if (dpPacket->cpName != NULL) free(dpPacket->cpName);
		free(dpPacket);
		return 1;
	}

	return 0;
}

/* Check the input dependencies, register dependecies with parent nodes,
 * and save currently available inputs. */
int CheckInputDependencies(job_node *jnpNode, int iSocket) {
	int i;

	/* Lookup this node's input parameters.  For each input, if it
	 * is in the Variable Lookup Table, add this node to the owning node's
	 * list of children -- unless that job is actually finished, in which
	 * case make a reference to the data.
	 */
	for (i = 0; i < jnpNode->job-> iNumInVars; i++) {
		void *vp;
		job_node *jnpParent = HSearch(jnpNode->job->cppInVars[i], NULL,
				FIND, htVariableTable);

		if (jnpParent != NULL) {
			if (jnpParent->job->dpResult == NULL) {
				/* The input is an output from another job, so register the
				 * dependency. */
				vp = (void *) realloc(jnpParent->children, sizeof(job_node *) *
						(jnpParent->iNumChildren + 1));
				if (vp == NULL) {
					fprintf(stderr, "%s:%d:ERROR:Failed realloc on children list.\n",
							__FILE__, __LINE__);
					/* Now what? */
					return -1;
				}
				jnpParent->children = vp;
				jnpParent->children[jnpParent->iNumChildren++] = jnpNode;
				jnpNode->iNumParents++;
			} else {
				/* The input is the output from a finished job. */
				Dprintf(("Scheduler: Info: Took input \"%s\" from finished job %d for job %d.\n", jnpNode->job->cppInVars[i], jnpParent->job->iJobNum,
						jnpNode->job->iJobNum));
				jnpNode->job->dppInputs[i] = jnpParent->job->dpResult;
				jnpParent->job->dpResult->iNumRef++;
			}
		} else {
#ifdef MAIN_THREAD_SERIALIZES
			data_packet *dpInput;
			/* The input is currently available, so get it from the main
			 * thread. */
			write(iSocket, &i, sizeof(int));
			read(iSocket, &dpInput, sizeof(data_packet *));
			jnpNode->job->dppInputs[i] = InputDataPacket(dpInput);
#endif
		}
	}

#ifdef MAIN_THREAD_SERIALIZES
	/* When we are done, signal the main thread, so it can continue.
	 * If there are too many jobs in the scheduler, then tell the main
	 * thread to wait.
	 */
	if (iJobsInFlight > iMaxJobsInFlight) {
		i = -2;
	} else {
		i = -1;
	}
	write(iSocket, &i, sizeof(int));
#endif

	return 0;
}

/* Register the output variable.*/
int RegisterOutput(job_node *jnpNode) {
	if (jnpNode->job->cpOutVar != NULL) {
		HSearch(jnpNode->job->cpOutVar, jnpNode, ENTER, htVariableTable);
	}

	return 0;
}

/* If node is ready, add to the end of the ready queue.
 * Returns zero if the node was added, positive non-zero if the node wasn't
 * ready, and negative on error.
 */
int ConditionalAddToReadyQueue(job_node *jnpNode) {
	if (jnpNode == NULL) return -1;
	if (jnpNode->iNumParents > 0) return jnpNode->iNumParents;

	/* If the list is empty, this node becomes the list. */
	if (jnpReadyQueueFirst == NULL) {
		jnpReadyQueueFirst = jnpNode;
		jnpReadyQueueLast = jnpNode;

		if (jnpNode->next != NULL) {
			fprintf(stderr, "%s:%d: Assert Warning: Node had leftover pointers.\n", __FILE__, __LINE__);
			jnpNode->next = NULL;
		}
		return 0;
	}

	if (jnpReadyQueueLast == NULL) {
		fprintf(stderr, "%s:%d:ASSERT ERROR: Ready queue first/last pointers disagree!\n", __FILE__, __LINE__);
		return -2;
	}

	if (jnpReadyQueueLast->next != NULL) {
		fprintf(stderr, "%s:%d:ASSERT WARNING: Last node had a next pointer!\n",
			__FILE__, __LINE__);
	}

	/* The list is not empty, so add this node onto the end. */
	jnpReadyQueueLast->next = jnpNode;
	jnpReadyQueueLast = jnpNode;

	jnpNode->next = NULL;

	return 0;
}

/* Note:  not cleaning up next pointers */
job_node *TakeNodeFromReadyQueue() {
	job_node *jnpNode;

	/* If the queue is empty, return immediately */
	if (jnpReadyQueueFirst == NULL) return NULL;

	jnpNode = jnpReadyQueueFirst;

	/* If the next item in the queue is a GE call and not all workers are
	 * finished, then return NULL (basically, pretending that the queue is
	 * empty).
	 */
	if (jnpNode->job->iGlobal && iJobsInWorkers != 0) return NULL;

	jnpReadyQueueFirst = jnpNode->next;
	if (jnpReadyQueueFirst == NULL) jnpReadyQueueLast = NULL;

	return jnpNode;
}

/* Checks to see if a worker thread is available.
 * Returns non-zero (true) if a thread is available and zero (false) otherwise.
 */
int IsAWorkerAvailable() {
	if (tnpThreadStack == NULL) return 0;
	else return 1;
}

/* While worker threads and jobs are available, give jobs to worker threads.
 * Returns the number of jobs distributed.
 */
int ExecuteLoop() {
	job_node *jnpNode;
	int iNumCycles = 0;

	while (IsAWorkerAvailable() && NULL !=
			(jnpNode = TakeNodeFromReadyQueue())) {
		GiveJobToWorker(jnpNode);
		iNumCycles++;
	}

	/* printf("Info:  Execute finished %d cycles\n", iNumCycles); */
	return iNumCycles;
}

/* Notify the children of the given node that this node is done execution. */
int NotifyChildren(job_node *jnpNode) {
	int i;
	int j;

	if (jnpNode->iNumChildren == 0) return 0;

	if (jnpNode->children == NULL) {
		fprintf(stderr, "%s:%d:ASSERT WARNING: Lost (%d) Children.\n",
				__FILE__, __LINE__, jnpNode->iNumChildren);
		return -1;
	}

	for (i = 0; i < jnpNode->iNumChildren; i++) {
		jnpNode->children[i]->iNumParents--;
		/* Search for the input index which matches this node's output */
		for (j = 0; j < jnpNode->children[i]->job->iNumInVars; j++) {
			if (strcmp(jnpNode->job->cpOutVar,
					jnpNode->children[i]->job->cppInVars[j]) == 0) {
				Dprintf(("Scheduler: Variable \"%s\" given to job %d\n",
						jnpNode->job->cpOutVar,
						jnpNode->children[i]->job->iJobNum));
				jnpNode->children[i]->job->dppInputs[j] =
						jnpNode->job->dpResult;
				jnpNode->job->dpResult->iNumRef++;
			}
		}
		ConditionalAddToReadyQueue(jnpNode->children[i]);
	}

	return 0;
}

/* Does the given node still own its output?  i.e.  Is the value in the
 * Variable Lookup Table, for the key of this node's output, a pointer to
 * this node?
 */
int IsOutputStillOwned(job_node *jnpNode) {
	job_node *jnpTemp;

	if (jnpNode->job->cpOutVar != NULL) {
		jnpTemp = HSearch(jnpNode->job->cpOutVar, NULL, FIND, htVariableTable);
		if (jnpTemp == jnpNode) return 1;
		else return 0;
	}

	return 0;	/*  If the output is not still owned, then that means
				 *  that it isn't needed anymore.  Therefore, this is
				 *  the best option for no output.
				 */
}

/* Remove the given node's output from the Variable Lookup Table, if it
 * is still owned.
 */
int UnRegisterOutput(job_node *jnpNode) {
	job_node *jnpTemp;

	if (jnpNode->job->cpOutVar != NULL) {
		jnpTemp = HSearch(jnpNode->job->cpOutVar, NULL, FIND, htVariableTable);
		if (jnpTemp == jnpNode) {
			HDelete(jnpNode->job->cpOutVar, htVariableTable);
		}
	}

	return 0;
}

/* Add a finished job to the Finished List.  Clear the list of children at
 * this time so that any new children can be detected/notified later.
 */
int AddJobToFinishedList(job_node *jnpNode) {
	if (jnpNode == NULL) {
		fprintf(stderr, "%s:%d: ASSERT WARNING: Tried to put NULL node on finished stack.\n", __FILE__, __LINE__);
		return -1;
	}

	Dprintf(("Scheduler:  Put job with output \"%s\" on finished list.\n",
				jnpNode->job->cpOutVar));

	/* Add the job to the finished stack */
	jnpNode->next = jnpFinishedList;
	jnpFinishedList = jnpNode;

	/* Clear list of children */
	if (jnpNode->children != NULL) {
		free(jnpNode->children);
		jnpNode->children = NULL;
		jnpNode->iNumChildren = 0;
	}

	return 0;
}

/* Take (remove) the first job from the finished list and return it. */
job_node *TakeJobFromFinishedList(void) {
	job_node *jnpNode = jnpFinishedList;

	if (jnpFinishedList == NULL) return NULL;

	jnpFinishedList = jnpFinishedList->next;

	return jnpNode;
}

/* Get the next job to be returned to the main thread and return it. */
job_node *GetNextFinishedJob(void) {
	job_node *jnpNode;

	jnpNode = TakeJobFromFinishedList();
	while (jnpNode != NULL) {
		if (IsOutputStillOwned(jnpNode)) return jnpNode;
		Dprintf(("Scheduler: Info: Threw away job from finished list\n"));
		CleanupJob(jnpNode);
		jnpNode = TakeJobFromFinishedList();
	};

	return NULL;
}


/* Add the given socket (corresponding to a worker thread) to the Thread Stack.
 */
int AddWorkerToStack(int iSocket) {
	thread_node *tnpNode;

	tnpNode = (thread_node *) malloc(sizeof(thread_node));
	if (tnpNode == NULL) {
		fprintf(stderr, "%s:%d:ERROR: Failed to allocate memory for thread node!\n", __FILE__, __LINE__);
		return -1;
	}

	tnpNode->iSocket = iSocket;
	tnpNode->next = NULL;

#ifndef THREAD_STACK_IS_A_QUEUE
	if (tnpThreadStack == NULL) {
		tnpThreadStack = tnpNode;
	} else {
		tnpNode->next = tnpThreadStack;
		tnpThreadStack = tnpNode;
	}
#else
	if (tnpThreadStack == NULL) {
		tnpThreadStack = tnpThreadQueueLast = tnpNode;
	} else {
		tnpThreadQueueLast->next = tnpNode;
		tnpThreadQueueLast = tnpNode;
	}
#endif

	iJobsInWorkers--;

	if (iJobsInWorkers < 0) {
		fprintf(stderr, "%s:%d: ASSERT WARNING: More jobs returned than sent out!\n", __FILE__, __LINE__);
		iJobsInWorkers = 0;
	}

	return 0;
}

/* Take (remove) the top socket (corresponding to a worker thread) from the
 * Thread Stack and return it.
 */
int TakeWorkerFromStack(void) {
	thread_node *tnpNode;
	int iSocket;

	if (tnpThreadStack == NULL) {
		fprintf(stderr, "%s:%d: ASSERT ERROR:  Tried to take worker from an empty stack!\n", __FILE__, __LINE__);
		return -1;
	}

	tnpNode = tnpThreadStack;
	tnpThreadStack = tnpNode->next;

	iSocket = tnpNode->iSocket;
	free(tnpNode);

	iJobsInWorkers++;

	return iSocket;
}

/* Peforms a semi-shallow copy of the given job packet.
 * (Maybe the function should be called "MixedCopy"?).
 */
job_packet *ShallowCopyJobPacket(job_packet *jppIn) {
	int i;
	job_packet *jppOut;

	if (jppIn == NULL) {
		fprintf(stderr, "ASSERT WARNING: Input to ShallowCopyJobPacket was NULL!\n");
		return NULL;
	}

	jppOut = (job_packet *) malloc(sizeof(job_packet));
	if (jppOut == NULL) {
		fprintf(stderr, "%s:%d: ASSERT ERROR: Failed to allocate memory!\n",
				__FILE__, __LINE__);
		return NULL;
	}

	/* Start the shallow copy (so that the two allocated arrays don't get
	 * overwritten later).
	 */
	memcpy(jppOut, jppIn, sizeof(job_packet));

	/* Finish the memory allocations. */
	jppOut->cppInVars = (char **) malloc(sizeof(char *) * jppIn->iNumInVars);
	if (jppOut->cppInVars == NULL) {
		fprintf(stderr, "%s:%d: ASSERT ERROR: Failed to allocate memory!\n",
				__FILE__, __LINE__);
		return NULL;
	}

	jppOut->dppInputs = (data_packet **) malloc(sizeof(char *) *
			jppIn->iNumInVars);
	if (jppOut->dppInputs == NULL) {
		fprintf(stderr, "%s:%d: ASSERT ERROR: Failed to allocate memory!\n",
				__FILE__, __LINE__);
		return NULL;
	}

	/* Continue the shallow copy. */
	jppOut->dpCall->iNumRef++;

	for (i = 0; i < jppIn->iNumInVars; i++) {
		jppOut->dppInputs[i] = jppIn->dppInputs[i];
		jppOut->dppInputs[i]->iNumRef++;
	}

	/* Next, do a deep copy of the variable names, because they aren't
	 * reference counted.
	 */
	if (jppIn->cpOutVar != NULL) jppOut->cpOutVar = strdup(jppIn->cpOutVar);

	for (i = 0; i < jppIn->iNumInVars; i++)
		jppOut->cppInVars[i] = strdup(jppIn->cppInVars[i]);

	return jppOut;
}


/* Given the given job node to the top worker on the Thread Stack.
 * If this is a GE (Global Execute) instruction, give the job node to
 * all of the workers.  Make shallow copies of GE instructions.
 */
int GiveJobToWorker(job_node *jnpNode) {
	int iSocket;
	job_node *jnpTemp;

	if (!IsAWorkerAvailable()) {
		fprintf(stderr, "%s:%d: ASSERT WARNING:  Scheduler tried to start a job when no worker threads were available.\n", __FILE__, __LINE__);
		return -1;
	}

	iSocket = TakeWorkerFromStack();
	write(iSocket, &jnpNode, sizeof(job_node *));

	if (jnpNode->job->iGlobal) {
		while (IsAWorkerAvailable()) {
			/* Begin shallow copy */
			jnpTemp = CreateNode(ShallowCopyJobPacket(jnpNode->job));

			iSocket = TakeWorkerFromStack();
			write(iSocket, &jnpTemp, sizeof(job_node *));

			iJobsInFlight++; /* This must be incremented here to maintain
							  * a proper count. */
		}
	}

	return 0;
}

/* Return all available jobs to the main thread via the given socket.
 * It is assumed that the main thread is actively waiting to receive these
 * jobs.
 */
int ReturnJobToMainThread(int iSocket) {
	int iLen;
	void *vp;
	job_node *jnpNode = GetNextFinishedJob();

	while (jnpNode != NULL) {
		int iSize;

#if 0
		/* The node shouldn't have any children, because they should have
		 * all been notified/removed when the job finished.  This prevents
		 * a race whereby after this job finishes, the main thread sends
		 * a job depending on this job, before it checks for finished jobs.
		 */
		NotifyChildren(jnpNode);
#endif

		if (jnpNode->iNumChildren != 0) {
			fprintf(stderr, "ASSERT WARNING:  Finished job still had children!\n");
			NotifyChildren(jnpNode);
		}

		Dprintf(("Returning result \"%s\"\n", jnpNode->job->cpOutVar));
		/* Send the result back:  name then ptr to data */
		iSize=write(iSocket, &(jnpNode->job->cpOutVar), sizeof(char *));
		if (iSize != sizeof(char *))
			perror("Scheduler: Failed while writing name");
		iSize=write(iSocket, &(jnpNode->job->dpResult),
				sizeof(data_packet *));
		if (iSize != sizeof(data_packet  *))
			perror("Scheduler: Failed while writing data");

		write(iSocket, &(jnpNode->job->rho), sizeof(SEXP));

		/* Wait for signal from main thread saying it is done with this
		 * variable. */
		read(iSocket, &iLen, sizeof(int));
		CleanupJob(jnpNode);

		jnpNode = GetNextFinishedJob();
	}

	/* Reset flag and signal the main thread that there are no more jobs. */
	bJobsFinished = 0;
	vp = NULL;
	write(iSocket, &vp, sizeof(void *));

	return 0;
}

/* A new job was received from the main thread.  Take the job packet,
 * create a node for the packet, check input dependencies and fill in
 * available variables, register the output variable, and put it in the
 * ready queue if it is ready.
 */
int ReceivedNewJob(job_packet *jppPacket, int iSocket) {
	job_node *jnpNode;

	if (jppPacket == NULL) return -1;

	if (jppPacket->iNumInVars == -1) {
		/* The main thread is ready to read finished jobs. */
		return ReturnJobToMainThread(iSocket);
	}

	if (jppPacket->iNumInVars == -2) {
		/* The main thread is waiting on the next finished job. */
		if (iJobsInFlight == 0 || jnpFinishedList != NULL) {
			write(iSocket, &iJobsInFlight, sizeof(int));
		} else {
			bMainThreadIsWaiting = 1;
		}
		return 0;
	}

	jnpNode = CreateNode(jppPacket);
	if (jnpNode == NULL) return -2;

	if (CheckInputDependencies(jnpNode, iSocket) != 0) return -3;

	Dprintf(("Scheduler: Info: New job has %d parents, %d inputs\n",
			jnpNode->iNumParents, jnpNode->job->iNumInVars));

	iJobsInFlight++;

	RegisterOutput(jnpNode);

	ConditionalAddToReadyQueue(jnpNode);
	ExecuteLoop();

	return 0;
}

/* Delete the given job node (including the job packet, and possibly the
 * input/output variables).
 */
int CleanupJob(job_node *jnpNode) {
	int i;
	if (jnpNode == NULL) {
		fprintf(stderr, "%s:%d:ASSERT WARNING:  Tried to cleanup NULL node!\n",
				__FILE__, __LINE__);
		return -1;
	}

	UnRegisterOutput(jnpNode);

	/* TODO  What to do with job_packet? */
	/* Actually, now this is about right. */
	for (i = 0; i < jnpNode->job->iNumInVars; i++) {
		if (jnpNode->job->dppInputs[i] == NULL) {
			fprintf(stderr, "%s:%d: ASSERT WARNING:  NULL Data Node found in finished job!!\n", __FILE__, __LINE__);
			continue;
		}

		DeleteDataPacket(jnpNode->job->dppInputs[i]);
		free(jnpNode->job->cppInVars[i]);
	}
	free(jnpNode->job->dppInputs);

	DeleteDataPacket(jnpNode->job->dpResult);
	DeleteDataPacket(jnpNode->job->dpCall);
	free(jnpNode->job->cppInVars);
	free(jnpNode->job->cpOutVar);
	free(jnpNode->job);

	if (jnpNode->children != NULL)
		free(jnpNode->children);
	free(jnpNode);

	iJobsInFlight--;

	return 0;
}

/* Inputs:  The job which just finished, the socket to the main thread,
 *          and the socket to the worker thread.
 */
int ReceivedFinishedJob(job_node *jnpNode, int iMSocket, int iWSocket) {

	Dprintf(("Scheduler: Received finished job %d from worker %x.  It has %d children.\n", jnpNode->job->iJobNum, iWSocket, jnpNode->iNumChildren));


	NotifyChildren(jnpNode);

	/* As a not-so-minor complication to the scheduler, we have to return
	 * the answers into R environment.  To avoid a race condition, I keep
	 * the finished job around (and in the Variable Lookup Table) until it
	 * is really done.  If the job no longer owns its output, then it is
	 * done.  If the job still owns its output, then it needs to be kept
	 * so that any incoming jobs (we hope there aren't any, but it is a
	 * race issue) can still get their inputs from this jobs output.
	 */
	if (IsOutputStillOwned(jnpNode)) {
		AddJobToFinishedList(jnpNode);
		bJobsFinished = 1;	/* Set signal for main thread */
		if (bMainThreadIsWaiting) {
			write(iMSocket, &iJobsInFlight, sizeof(int));
			bMainThreadIsWaiting = 0;
		}
	} else {
		CleanupJob(jnpNode);

		if (iJobsInFlight == 0 && bMainThreadIsWaiting == 1) {
			fprintf(stderr, "Warning: Finished job triggered \"useless instruction\" race detector.\n");
			write(iMSocket, &iJobsInFlight, sizeof(int));
			bMainThreadIsWaiting = 0;
		}
	}

	AddWorkerToStack(iWSocket);

	ExecuteLoop();

	return 0;
}

#define MAX(a, b) (((a) > (b)) ? a : b)

/* The main work loop of the scheduler.  Using a select() call, the scheduler
 * waits for messages from the main thread and the workers threads.
 */
int SchedulerLoop(int iNumWorkers, int *ipSockets) {
	int iMaxFD = -1;
	int i;
	int bDone = 0;
	int bAlmostDone = 0;
	fd_set fdsRead, fdsAll;

	FD_ZERO(&fdsAll);

	for(i = 0; i < (iNumWorkers + 1); i++) {
		FD_SET(ipSockets[i], &fdsAll);
		iMaxFD = MAX(iMaxFD, ipSockets[i]);
	}

	memcpy(&fdsRead, &fdsAll, sizeof(fd_set));

	/* Wait for ready signals from all threads. */
	for (i = 0; i < iNumWorkers; i++) {
		int iBuf;
		read(ipSockets[i+1], &iBuf, sizeof(int));
		AddWorkerToStack(ipSockets[i+1]);
	}

	printf("Scheduler:  All (%d) worker threads report ready\n", iNumWorkers);

	write(ipSockets[0], &iMaxJobsInFlight, sizeof(int));

	do {
		if (iJobsInFlight == 0 && bMainThreadIsWaiting == 1) {
			fprintf(stderr, "ASSERT WARNING:  Main thread is waiting for non-existant job!\n");
			fprintf(stderr, "WARNING:  Assuming main thread is actually waiting.\n");
			write(ipSockets[0], &iJobsInFlight, sizeof(int));
			bMainThreadIsWaiting = 0;
		}

		Dprintf(("Scheduler: Before Loop: %d jobs in flight (%d in workers)\n",
					iJobsInFlight, iJobsInWorkers));

		select( iMaxFD + 1, &fdsRead, NULL, NULL, NULL);
		/* First, check for finished jobs. */
		for (i = 0; i < iNumWorkers; i++) {
			if (FD_ISSET(ipSockets[i+1], &fdsRead)) {
				job_node *jnpNode;

				if (read(ipSockets[i+1], &jnpNode, sizeof(void *)) !=
						sizeof(void *)) {
					perror("Failed to read job from worker thread: ");
					break;
				}

				ReceivedFinishedJob(jnpNode, ipSockets[0], ipSockets[i+1]);
			}
		}
		/* Then, check for new jobs. */
		if (FD_ISSET(ipSockets[0], &fdsRead)) {
			job_packet *jppPacket;
			if (read(ipSockets[0], &jppPacket, sizeof(void *)) !=
					sizeof(void *)) {
				perror("Failed to read job from main thread: ");
				continue;	/* ??? */
			}

			if (jppPacket == NULL) {
				bAlmostDone = 1;
			} else {
				ReceivedNewJob(jppPacket, ipSockets[0]);
			}
		}

		/* Run ExecuteLoop again, to prevent a rare deadlock.
		 * I think that the deadlock occured because a job being returned
		 * to the main thread can trigger the move of a job to the ready
		 * queue, but without running ExecuteLoop.
		 */
		if (ExecuteLoop() != 0) {
			printf("ASSERT WARNING: End of loop execute found job.\n");
		}

		memcpy(&fdsRead, &fdsAll, sizeof(fd_set));

		if (bAlmostDone && iJobsInFlight == 0)  bDone = 1;
	} while (!bDone);

	return 0;
}

/* Start of the scheduler thread.  Initialize the stacks and queues, the
 * counters and flags, and the hash tables.  Then run the Scheduler Loop,
 * and cleanup when the Scheduler Loop finishes.
 */
void *StartSchedulerLoop(void *vpArgs) {
	int *ipArgs = vpArgs;
	int iNumWorkers;
	int *ipSockets;
	int i;

	if (vpArgs == NULL) return NULL;

	iNumWorkers = ipArgs[0];
	ipSockets = ipArgs + 1;

	if (ipSockets == NULL) return NULL;

	if (iNumWorkers < 1 || iNumWorkers > 1048576) {
		fprintf(stderr, "Number of worker threads (%d) is invalid.\n",
				ipArgs[0]);
		return NULL;
	}

	if (iNumWorkers > FD_SETSIZE - 1) {
		fprintf(stderr, "%s:%d: ERROR:  Too many worker threads (%d > %d)!\n",
				__FILE__, __LINE__, iNumWorkers, FD_SETSIZE - 1);
		return NULL;
	}

	/* Initialize the global/semi-global variables used by the scheduler. */
	jnpReadyQueueFirst = NULL;
	jnpReadyQueueLast = NULL;
	tnpThreadStack = NULL;
#ifdef THREAD_STACK_IS_A_QUEUE
	tnpThreadQueueLast = NULL;
#endif
	jnpFinishedList = NULL;
	bMainThreadIsWaiting = 0;
	bJobsFinished = 0;

	iJobsInFlight = 0;
	iJobsInWorkers = iNumWorkers;	/* (Will be decremented to zero as the
									 *  workers 'check-in'.)
									 */

	iMaxJobsInFlight = 2 * iNumWorkers;
	HCreate((int) (4 + 1.5 * iMaxJobsInFlight), &htVariableTable);
	HCreate((int) (4 + 3 * iMaxJobsInFlight), &htDataTable);

	/* Enter the scheduler's main loop. */
	SchedulerLoop(iNumWorkers, ipSockets);

	/* Kill the worker threads */
	for (i = 1; i <= iNumWorkers; i++) {
		void *vp = NULL;
		write(ipSockets[i], &vp, sizeof(void *));
	}

	/* Now, shutdown the scheduler. */
	HDestroy(htVariableTable);
	HDestroy(htDataTable);
	printf("Info:  Scheduler done\n");
	write(ipSockets[0], &iMaxJobsInFlight, sizeof(int));
	close(ipSockets[0]);

	return NULL;
}

