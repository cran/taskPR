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
/* Spawn Helper.  Really should be "MPI argument helper".
 * According to the MPI standards, arguments given to an MPI program through
 * mpirun or MPI_Comm_spawn are only "significant" at root.  Therefore, some
 * implementations (i.e., not LAM/MPI) only pass those arguments to process
 * zero.
 * This program simply calls MPI_Init, copies the arguments from process zero
 * to all processes, and then all processes do an exec call on the first
 * argument, passing along all the rest of the arguments.
 */
/*  $Author: bauer $
    $Date: 2004/07/09 13:03:55 $
    $Revision: 1.3 $
 */
#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <malloc.h>
#include <string.h>

int main(int argc, char *argv[]) {
	int iNumProcs, iRank;
	char **argv2 = argv;
	int i;

	if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
		fprintf(stderr, "Failed to initialize MPI! Aborting...\n");
		return -1;
	}

	MPI_Comm_rank(MPI_COMM_WORLD, &iRank);
	MPI_Comm_size(MPI_COMM_WORLD, &iNumProcs);

	MPI_Bcast(&argc, 1, MPI_INT, 0, MPI_COMM_WORLD);

	argv2 = (char **) malloc(sizeof(char *) * argc);
	if (argv2 == NULL) {
		fprintf(stderr, "%d: Failed to allocate memory!\n", iRank);
		return -2;
	}
	/* Zero out the whole array.  In particular, the last element must be
	 * zeroed. */
	memset(argv2, 0x00, sizeof(char *) * argc);
	
	if (iRank == 0) {
		for (i = 1; i < argc; i++) argv2[i-1] = argv[i];
	}
	
	for (i = 0; i < argc - 1; i++) {
		int iSize;
		if (iRank == 0) iSize = strlen(argv2[i]);
		MPI_Bcast(&iSize, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (iRank != 0) {
			argv2[i] = (char *) malloc(sizeof(char) * iSize);
			if (argv2[i] == NULL) {
				fprintf(stderr, "%d: Failed to allocate memory!\n", iRank);
				return -3;
			}
		}
		MPI_Bcast(argv2[i], iSize + 1, MPI_CHAR, 0, MPI_COMM_WORLD);
	}

	printf("Executing \"'%s' %s %s %s\"...\n", argv2[0],
			argc > 2 ? argv2[1] : "", argc > 3 ? argv2[2] : "",
			argc > 4 ? argv2[3] : "");
	execvp(argv2[0], argv2);

	fprintf(stderr, "%d:  Call to execv failed!\n", iRank);
	perror("");
	fprintf(stderr, "Program: \"%s\"\n", argv2[0]);

	return -4;
}

