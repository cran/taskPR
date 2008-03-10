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
/* peval.h  Parallel Evaluation header file */
/*  $Author: david $
    $Date: 2008-03-03 19:28:13 $
    $Revision: 1.11 $
 */
#ifndef _PEVAL_H_
#define _PEVAL_H_
#include <Rinternals.h>
#include "scheduler.h"

int ParallelExecute(SEXP s, int iSocket, int iGlobal, SEXP rho);
job_packet *CreateAndPopulateJobPacket(SEXP s, SEXP rho, int iGlobal);
job_packet *CreateJobPacket(void);

int SerializeDataRequest(job_packet * jppPacket, SEXP rho, int iSocket);
data_packet *CreateDataPacket(SEXP sxInput, char *cpName);

int InitializeParallelExecution(int iNumWorkers, int *ipMySocket, SEXP rho);
int CreateSocketPairs(int *ipMySocket, int *ipSchedulerSockets, int *ipWorkerSockets, int iNumWorkers, SEXP rho);
int SpawnAllThreads(int iNumWorkers, int *ipSchedulerSockets, int *ipWorkerSockets);

SEXP WaitForVariable(const char *cpName, int iSocket, SEXP rho);
int GetResultsFromScheduler(int iSocket, SEXP rho, const char *cpCmpName);
SEXP ReturnResult(data_packet *dpResult, char *cpName, SEXP rho);

SEXP CreateExtPtrObj(char *cpName, SEXP rho);


#endif
