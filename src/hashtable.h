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
/*  $Author: bauer $
    $Date: 2004/07/09 13:03:59 $
    $Revision: 1.4 $
 */
#ifndef _HASHTABLE_H_
#define _HASHTABLE_H_
#define __GNU_SOURCE
#include <search.h>


#define GE_HASH_SIZE (((size_t *) H)[-1])

struct GE_node {
	char *cpKey;
	void *vpVal;
	struct GE_node *prev;
	struct GE_node *next;
};

typedef struct GE_node GE_node;
typedef GE_node **GE_HashTbl;

int GE_hash(char *cpKey, GE_HashTbl H);
GE_node *GE_Insert(GE_node * NodeToInsert, GE_HashTbl H);
GE_node *GE_FindNode(char *cpKey, GE_HashTbl H);
int GE_InsertKeyVal(char *cpKey, void *vpVal, GE_HashTbl H);
void *GE_FindKeyVal(char *cpKey, GE_HashTbl H);
int GE_Remove(char *cpKey, GE_HashTbl H);

void GE_sentry(GE_HashTbl H, void (*pfunc)(GE_node *));
void FreeNode(GE_node *die);
void DispNode(GE_node *disp);
int GE_countNodes(GE_HashTbl H);

int HCreate(size_t nel, GE_HashTbl *hpH);
void *HSearch(char *cpKey, void *vpVal, ACTION action, GE_HashTbl H);
void HDestroy(GE_HashTbl H);
int HDelete(char *cpKey, GE_HashTbl H);

#endif

