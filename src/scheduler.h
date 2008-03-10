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
/* scheduler.h   Header file for the task scheduler.
 * Contains the structure definitions used by the scheduler (and peval and 
 * the worker threads).
 */
/*  $Author: david $
    $Date: 2008-03-03 19:28:48 $
    $Revision: 1.20 $
 */
#ifndef _SCHEDULER_H_
#define _SCHEDULER_H_

#ifdef BENCHMARK_PENGINE
#include <sys/time.h>
#include <time.h>
static struct timeval _tv_tmp;
#define GETTIME(y) (gettimeofday(&_tv_tmp, NULL), (double) (_tv_tmp).tv_sec * 1.0e6 + (double) (_tv_tmp).tv_usec)
#endif

// #include <pthread.h>
#include <Rinternals.h>

#define Dprintf(x) ((void)((1 < iGlobalParallelEngineEnabled) ? printf x : 0))

struct data_packet {
	int iNumRef;	/* The number of references to this data packet */ 
	int iLen;		/* Length of the serialized data */
	void *vpData;	/* Pointer to the data itself */
	char *cpName;	/* The name of the variable */
};

/* Job packets are ONLY created in the main thread (with the exception of
  duplicated job packets from GE statements).  After a job packet has
  been passed to the scheduler thread (and any immediately available inputs
  have been serialized), the main thread does NOT keep a reference to them
  (and never gets a refence back to them).  When the scheduler passes the job
  packet to a worker thread, it does NOT keep a direct reference to the job 
  packet.  (It may or may not keep a reference to the job node containing the
  job packet.)  The worker thread ONLY modifies the "dpResult" field of the
  job packet.  Job packets are deleted in the scheduler thread after being
  returned by the worker threads.
  Note:  Message job packets are treated differently that real job packets.  At
  the moment, message job packets are deleted in the main thread.
 */
struct job_packet {
	char *cpOutVar;		/* Output variable name */
	char **cppInVars;	/* Input variable names */
	int iNumInVars;		/* Number of input variables */
	/* Job stuff */
	int iJobNum;		/* Job ID */
	int iGlobal;		/* The Global flag specifies whether this job 
                         * is to be executed globally or not.
						 */
	/* SEXP sxpInputs; */
	struct data_packet **dppInputs;		/* List of inputs */
	struct data_packet *dpCall;			/* Serialized function call */
	struct data_packet *dpResult;		/* The result/answer of the job */ 
	SEXP rho;							/* The environment of the call */
};

/* Job nodes are ONLY created in the scheduler thread.  With the sole exception
	of the the "dpResult" field in the job packet of the job node, job nodes
	are considered to be read-only by the worker threads.  Job nodes are
	deleted by the scheduler thread after they have been returned by the
	worker threads.
 */
struct job_node {
	struct job_packet *job;		/* The job to be executed */ 
	struct job_node **children; /* List of children nodes */
	int iNumChildren;           /* The number of children nodes/jobs */
	int iNumParents;            /* The number of parent nodes/jobs */
	struct job_node *next;	    /* For use with various structures. */ 
};

/* Thread nodes are only used inside the scheduler thread. */
struct thread_node {
	int iSocket;				/* Socket id to communicate with the thread */
	struct thread_node *next;	/* For use in thread queue */
};

typedef struct job_packet job_packet;
typedef struct job_node job_node;
typedef struct thread_node thread_node;
typedef struct data_packet data_packet;

void *StartSchedulerLoop(void *vpArgs);
int GiveJobToWorker(job_node *jnpNode);
int CleanupJob(job_node *jnpNode);
int DeleteDataPacket(data_packet *dpPacket);

#endif
