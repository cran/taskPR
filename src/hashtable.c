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
/* hashtable.c   Generic Hash Table.
 * Written by David Bauer  April 2004 for the Parallel-R task scheduler.
 * Based on code written by David Bauer  Fall 2001  for CS 2130.
 * API based on the hsearch family (POSIX 1003.1-2001), but with the
 * ENTRY structure removed.  (Actually, it is closer to the GNU version.)
 * Written because the !*@^%! standard doesn't support deleting entries!!
 */
/*  $Author: bauer $
    $Date: 2004-08-01 13:00:14 $
    $Revision: 1.12 $
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hashtable.h"

static int iNodeCounter;

/* Simple hashing function. */
int GE_hash(char *cpKey, GE_HashTbl H) {
	int iHashVal = GE_HASH_SIZE - 1;
	int i;

	for (i = 0; i < strlen(cpKey); i++) {
		iHashVal += cpKey[i] * 53 + (cpKey[i] ^ (iHashVal & 0x0ff)) + i;
	}

	return (iHashVal % GE_HASH_SIZE);
}

/* Inserts a node into the hash table.  Returns NULL upon failure.
 * If there is already a node in the table with the same key, it is over
 * writen. */
GE_node *GE_Insert(GE_node * NodeToInsert, GE_HashTbl H) {
	int iHashVal;
	GE_node *tmp;
	if (H == NULL || NodeToInsert == NULL)
		return NULL;
	if (NodeToInsert->cpKey == NULL)
		return NULL;

	iHashVal = GE_hash(NodeToInsert->cpKey, H);
	tmp = GE_FindNode(NodeToInsert->cpKey, H);
	if (tmp != NULL) {
		NodeToInsert->next = tmp->next;
		NodeToInsert->prev = tmp->prev;
		if (tmp->next != NULL)
			tmp->next->prev = NodeToInsert;
		if (tmp->prev != NULL)
			tmp->prev->next = NodeToInsert;
		else
			H[iHashVal] = NodeToInsert;
		free(tmp);		/* FreeNode would contract the linked list. */
	} else {
		NodeToInsert->prev = NULL;
		NodeToInsert->next = H[iHashVal];
		if (NodeToInsert->next != NULL)
			NodeToInsert->next->prev = NodeToInsert;
		H[iHashVal] = NodeToInsert;
	}

	return NodeToInsert;
}

/* Given a key, returns a ptr to the node that contains that key, or NULL
 * if no node was found. */
GE_node *GE_FindNode(char *cpKey, GE_HashTbl H) {
	int iHashVal = GE_hash(cpKey, H);
	GE_node *tmp = NULL;
	if (H == NULL)
		return NULL;
	tmp = H[iHashVal];
	while (tmp != NULL) {
		if (strcmp(tmp->cpKey, cpKey) == 0)
			return tmp;
		tmp = tmp->next;
	}
	return NULL;
}

/* Removes the node from the table.
 * Returns non-zero upon failure, and zero upon success.
 */
int GE_Remove(char *cpKey, GE_HashTbl H) {
	int iHashVal;
	GE_node *tmp;

	if (H == NULL || cpKey == NULL)
		return -1;
	tmp = GE_FindNode(cpKey, H);
	if (tmp == NULL)
		return -2;

	iHashVal = GE_hash(cpKey, H);
	if (tmp == H[iHashVal]) {
		/*  First item in chain. */
		H[iHashVal] = tmp->next;
	} else {
		if (tmp->prev == NULL) {
			fprintf(stderr, "ASSERT FAILED! Node was missing ");
			fprintf(stderr, "prev pointer.\n");
			return -3;
		}
		tmp->prev->next = tmp->next;
	}
	if (tmp->next != NULL) {
		tmp->next->prev = tmp->prev;
	}

	free(tmp);
	return 0;
}

/* Frees the given node.
 *  Used with GE_sentry at the end of the program to clean up. */
void FreeNode(GE_node * node) {

	if (node != NULL) {
		if (node->prev != NULL)
			node->prev->next = node->next;
		if (node->next != NULL)
			node->next->prev = node->prev;
		free(node);
	}

	return;
}

/* If the passed node is not NULL, increment the node counter.
 *  Used by GE_countNodes through GE_sentry. */
void CountNodes(GE_node * count) {
	if (count != NULL) {
		iNodeCounter++;
	}
	return;
}

/* Counts the number of nodes in the table using a global variable
 * and the GE_sentry and CountNodes functions.
 */
int GE_countNodes(GE_HashTbl H) {
	iNodeCounter = 0;
	GE_sentry(H, CountNodes);
	return iNodeCounter;
}

/* Shows the Nodes in the table along with their value */
void DispNode(GE_node * disp) {
	if (disp != NULL) {
		printf("< %s, %p >", disp->cpKey, disp->vpVal);
	}
	return;
}

/* The GE_sentry function allows some other function  (FreeNode,
 *   DispNode, CountNodes) to be called on every node in the table. */
void GE_sentry(GE_HashTbl H, void (*pfunc) (GE_node *)) {
	GE_node *nPtr = NULL;
	GE_node *tPtr = NULL;
	int i;
	if (pfunc == NULL)
		return;
	for (i = 0; i < GE_HASH_SIZE; i++) {
		nPtr = H[i];
		while (nPtr != NULL) {
			tPtr = nPtr->next;
			(*pfunc) (nPtr);
			nPtr = tPtr;
		}
	}
	return;
}

/* Insert a key into the hash table.  If that entry already exists,
 * it is overwritten.
 */
int GE_InsertKeyVal(char *cpKey, void *vpVal, GE_HashTbl H) {
	GE_node *nPtr;
	GE_node *tmp;

	nPtr = (GE_node *) malloc(sizeof(GE_node));
	if (nPtr == NULL) {
		fprintf(stderr, "%s:%d:ERROR: Failed to allocate memory!\n",
				__FILE__, __LINE__);
		return -1;
	}
	nPtr->cpKey = cpKey;
	nPtr->vpVal = vpVal;
	tmp = GE_Insert(nPtr, H);
	if (tmp == NULL) {
		fprintf(stderr, "%s:%d:ASSERT FAILED!", __FILE__, __LINE__);
		fprintf(stderr,
				"Failed to insert valid node in function GE_InsertKeyVal.\n");
		free(nPtr);
		return -1;
	}
	return 0;
}

/* Search the table for the given key.
 * Returns the value associated with the key upon success, or NULL upon failure.
 */
void *GE_FindKeyVal(char *cpKey, GE_HashTbl H) {
	GE_node *node;

	node = GE_FindNode(cpKey, H);
	if (node == NULL) return NULL;
	return (node->vpVal);
}

/* ***************************************************************************
 * ***************************************************************************
 * ***************************************************************************
 */

/* Create a hash table of the given size.
 * Returns zero upon success and non-zero upon failure.
 * Remember that this implementation uses chaining for handling collisions,
 * so the number of items in the table is not limited by the given size.
 */
int HCreate(size_t nel, GE_HashTbl *hpH) {
	GE_HashTbl H;
	int i;

	if (hpH == NULL) return -1;

	*hpH = (GE_node **) ((char *) malloc(sizeof(GE_node *) * nel +
			sizeof(size_t)) + sizeof(size_t));
	if (*hpH == NULL) {
		fprintf(stderr, "%s:%d: ERROR: Failed to allocate memory (%lu bytes)\n",
				__FILE__, __LINE__, sizeof(GE_node *) * nel + sizeof(size_t));
		return -2;
	}

	H = *hpH;	/* GE_HASH_SIZE is a macro dependent on H */
	GE_HASH_SIZE = nel;

	for (i = 0; i < nel; i++) {
		H[i] = NULL;
	}

	return 0;
}

/* If action is FIND, attempt to find the given key in the hashtable.
 * 		Return its associated value if it is found or NULL otherwise.
 * If action is ENTER, enter the given data into the hashtable.  If an
 * entry already exists with the same key, it is overwriten.
 * 		Return NULL upon error or non-NULL, BUT MEANINGLESS, upon success.
 * (This follows the behavior of the GNU implementation, and NOT the
 *  POSIX standard.)
 */
void *HSearch(char *cpKey, void *vpVal, ACTION action, GE_HashTbl H) {

	if (H == NULL) return NULL;
	if (cpKey == NULL) return NULL;

	if (action == ENTER) {
		/* A NULL pointer is not considered acceptable data.
		 * (This test is not actually necessary, since a NULL would
		 *  be returned anyway.)
		 */
		if (vpVal == NULL) return NULL;
		if (GE_InsertKeyVal(cpKey, vpVal, H) != 0) {
			return NULL;
		}
		return (void *) 1;
	}

	if (action == FIND) {
		if (cpKey == NULL) return NULL;
		return(GE_FindKeyVal(cpKey, H));
	}

	fprintf(stderr, "%s:%d: ASSERT ERROR:  Invalid tag passed to HSearch function\n",  __FILE__, __LINE__);

	return NULL;
}

/* Delete an entire hash table */
void HDestroy(GE_HashTbl H) {
	GE_sentry(H, FreeNode);
	free(((char *) H - sizeof(size_t)));
}

/* Delete the given key from the hash table.
 * Returns 0 upon success or non-zero upon failure (key not found, for example).
 */
int HDelete(char *cpKey, GE_HashTbl H) {
	return (GE_Remove(cpKey, H));
}

