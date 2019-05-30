#ifndef SEQUENCE_H
#define SEQUENCE_H

/* 
 * This header specifies the interface for the use of node sequences.
 *   
 * The functions BestKOptMove and BacktrackKOptMove are implemented 
 * by means of such sequences. 
 */

#include "LKH.h"

extern thread_local Node **t;       /* The sequence of nodes to be used in a move */
extern thread_local Node **T;       /* The currently best t's */
extern thread_local Node **tSaved;  /* For saving t when using the BacktrackKOptMove function */
extern thread_local int *p;         /* The permutation corresponding to the sequence in which
                       the t's occur on the tour */
extern thread_local int *q;         /* The inverse permutation of p */
extern thread_local int *incl;      /* Array: incl[i] == j, if (t[i], t[j]) is an inclusion edge */
extern thread_local int *cycle;     /* Array: cycle[i] is cycle number of t[i] */
extern thread_local GainType *G;    /* For storing the G-values in the BestKOptMove function */
extern thread_local int K;          /* The value K for the current K-opt move */

int FeasibleKOptMove(int k);
void FindPermutation(int k);
int Cycles(int k);

int Added(const Node * ta, const Node * tb);
int Deleted(const Node * ta, const Node * tb);

void MarkAdded(Node *ta, Node *tb);
void MarkDeleted(Node *ta, Node *tb);
void UnmarkAdded(Node *ta, Node *tb);
void UnmarkDeleted(Node *ta, Node *tb);

#endif
