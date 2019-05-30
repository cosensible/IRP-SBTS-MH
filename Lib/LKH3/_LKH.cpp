#include "LKH.h"
#include "Sequence.h"
#include "gpx.h"
#include "Genetic.h"
#include "Delaunay.h"


// LKH.h
thread_local int AscentCandidates;   /* Number of candidate edges to be associated
                        with each node during the ascent */
thread_local int Asymmetric;         /* Is the instance asymmetric? */
thread_local int BackboneTrials;     /* Number of backbone trials in each run */
thread_local int Backtracking;       /* Specifies whether backtracking is used for
                        the first move in a sequence of moves */
thread_local GainType BestCost;      /* Cost of the tour in BestTour */
thread_local GainType BestPenalty;   /* Penalty of the tour in BestTour */
thread_local int *BestTour;          /* Table containing best tour found */
thread_local GainType BetterCost;    /* Cost of the tour stored in BetterTour */
thread_local GainType BetterPenalty; /* Penalty of the tour stored in BetterTour */
thread_local int *BetterTour;        /* Table containing the currently best tour
                        in a run */
thread_local int BWTSP_B;            /* Number of black nodes in a BWTSP instance */
thread_local int BWTSP_Q;            /* Maximum number of subsequent white nodes in a
                        BWTSP instance */
thread_local int BWTSP_L;            /* Maximum length of any path between two black nodes
                        in a BTWSP instance */
thread_local int CacheMask;  /* Mask for indexing the cache */
thread_local int *CacheVal;  /* Table of cached distances */
thread_local int *CacheSig;  /* Table of the signatures of cached
                distances */
thread_local int CandidateFiles;     /* Number of CANDIDATE_FILEs */
thread_local int EdgeFiles;          /* Number of EDGE_FILEs */
thread_local int *CostMatrix;        /* Cost matrix */
thread_local GainType CurrentGain;
thread_local GainType CurrentPenalty;
thread_local int DemandDimension;    /* Number of commodities in a M-PDTSP instance */
thread_local Node *Depot;
thread_local int Dimension;          /* Number of nodes in the problem */
thread_local int DimensionSaved;     /* Saved value of Dimension */
thread_local int Dim;                /* DimensionSaved - Salesmen + 1 */
thread_local double DistanceLimit;   /* Maxixim route distance for a CVRP instance */
thread_local double Excess;          /* Maximum alpha-value allowed for any
                        candidate edge is set to Excess times the
                        absolute value of the lower bound of a
                        solution tour */
thread_local int ExtraCandidates;    /* Number of extra neighbors to be added to
                        the candidate set of each node */
thread_local Node *FirstActive, *LastActive; /* First and last node in the list
                                of "active" nodes */
thread_local Constraint *FirstConstraint; /* First constraint in the list of SOP
                             precedence constraints */
thread_local Node *FirstNode;        /* First node in the list of nodes */
thread_local Segment *FirstSegment;  /* A pointer to the first segment in the cyclic
                        list of segments */
thread_local SSegment *FirstSSegment;        /* A pointer to the first super segment in
                                the cyclic list of segments */
thread_local int Gain23Used; /* Specifies whether Gain23 is used */
thread_local int GainCriterionUsed;  /* Specifies whether L&K's gain criterion is
                        used */
thread_local double GridSize;        /* Grid size used in toroidal instances */
thread_local int GroupSize;  /* Desired initial size of each segment */
thread_local int SGroupSize; /* Desired initial size of each super segment */
thread_local int Groups;     /* Current number of segments */
thread_local int SGroups;    /* Current number of super segments */
thread_local unsigned Hash;  /* Hash value corresponding to the current tour */
thread_local Node **Heap;    /* Heap used for computing minimum spanning trees */
thread_local HashTable *HTable;      /* Hash table used for storing tours */
thread_local int InitialPeriod;      /* Length of the first period in the ascent */
thread_local int InitialStepSize;    /* Initial step size used in the ascent */
thread_local double InitialTourFraction;     /* Fraction of the initial tour to be
                                constructed by INITIAL_TOUR_FILE edges */
thread_local int Kicks;      /* Specifies the number of K-swap-kicks */
thread_local int KickType;   /* Specifies K for a K-swap-kick */
thread_local char *LastLine; /* Last input line */
thread_local double LowerBound;      /* Lower bound found by the ascent */
thread_local int M;          /* The M-value is used when solving an ATSP-
                instance by transforming it to a STSP-instance */
thread_local int MaxBreadth; /* The maximum number of candidate edges
                considered at each level of the search for
                a move */
thread_local int MaxCandidates;      /* Maximum number of candidate edges to be
                        associated with each node */
thread_local int MaxMatrixDimension; /* Maximum dimension for an explicit cost matrix */
thread_local int MaxSwaps;   /* Maximum number of swaps made during the
                search for a move */
thread_local int MaxTrials;  /* Maximum number of trials in each run */
thread_local int MergeTourFiles;     /* Number of MERGE_TOUR_FILEs */
thread_local int MoveType;   /* Specifies the sequantial move type to be used
                in local search. A value K >= 2 signifies
                that a k-opt moves are tried for k <= K */
thread_local int MoveTypeSpecial; /* A special (3- or 5-opt) move is used */
thread_local Node *NodeSet;  /* Array of all nodes */
thread_local int Norm;       /* Measure of a 1-tree's discrepancy from a tour */
thread_local int NonsequentialMoveType;      /* Specifies the nonsequential move type to
                                be used in local search. A value
                                L >= 4 signifies that nonsequential
                                l-opt moves are tried for l <= L */
thread_local GainType Optimum;       /* Known optimal tour length.
                        If StopAtOptimum is 1, a run will be
                        terminated as soon as a tour length
                        becomes equal this value */
thread_local int PatchingA;  /* Specifies the maximum number of alternating
                cycles to be used for patching disjunct cycles */
thread_local int PatchingC;  /* Specifies the maximum number of disjoint cycles to be
                patched (by one or more alternating cycles) */
thread_local GainType PenaltyGain;
thread_local int PredSucCostAvailable; /* PredCost and SucCost are available */
thread_local int POPMUSIC_InitialTour;  /* Specifies whether the first POPMUSIC tour
                           is used as initial tour for LK */
thread_local int POPMUSIC_MaxNeighbors; /* Maximum number of nearest neighbors used
                           as candidates in iterated 3-opt */
thread_local int POPMUSIC_SampleSize;   /* The sample size */
thread_local int POPMUSIC_Solutions;    /* Number of solutions to generate */
thread_local int POPMUSIC_Trials;       /* Maximum trials used for iterated 3-opt */
thread_local unsigned *Rand;           /* Table of random values */
thread_local int Recombination;        /* IPT or GPX2 */
thread_local int RestrictedSearch;     /* Specifies whether the choice of the first
                          edge to be broken is restricted */
thread_local short Reversed; /* Boolean used to indicate whether a tour has
                been reversed */
thread_local int Run;        /* Current run number */
thread_local int Runs;       /* Total number of runs */
thread_local double ServiceTime; /* Service time for a CVRP instance */
thread_local int Serial;
thread_local unsigned Seed;  /* Initial seed for random number generation */
thread_local int StopAtOptimum;      /* Specifies whether a run will be terminated if
                        the tour length becomes equal to Optimum */
thread_local int Subgradient;        /* Specifies whether the Pi-values should be
                        determined by subgradient optimization */
thread_local int SubproblemSize;     /* Number of nodes in a subproblem */
thread_local int SubsequentMoveType; /* Specifies the move type to be used for all
                        moves following the first move in a sequence
                        of moves. The value K >= 2 signifies that a
                        K-opt move is to be used */
thread_local int SubsequentMoveTypeSpecial; /* A special (3- or 5-opt) sunbsequent move
                               is used */
thread_local int SubsequentPatching; /* Species whether patching is used for
                        subsequent moves */
thread_local SwapRecord *SwapStack;  /* Stack of SwapRecords */
thread_local int Swaps;      /* Number of swaps made during a tentative move */
thread_local int OldSwaps;   /* Saved number of swaps */
thread_local double TimeLimit;       /* The time limit in seconds for each run */
thread_local int TotalDemand;        /* Sum of demands for a CVRP instance */
thread_local int TraceLevel; /* Specifies the level of detail of the output
                given during the solution process.
                The value 0 signifies a minimum amount of
                output. The higher the value is the more
                information is given */
thread_local int Trial;      /* Ordinal number of the current trial */
thread_local GainType TSPTW_CurrentMakespanCost;
thread_local int TSPTW_Makespan;

/* The following variables are read by the functions ReadParameters and
ReadProblem: */

thread_local char *ParameterFileName, *ProblemFileName, *PiFileName,
    *TourFileName, *OutputTourFileName, *InputTourFileName,
    **CandidateFileName, **EdgeFileName, *InitialTourFileName,
    *SubproblemTourFileName, **MergeTourFileName,
    *MTSPSolutionFileName, *SINTEFSolutionFileName;
thread_local char *Name, *Type, *EdgeWeightType, *EdgeWeightFormat,
*EdgeDataFormat, *NodeCoordType, *DisplayDataType;
thread_local int CandidateSetSymmetric, CandidateSetType, Capacity,
    CoordType, DelaunayPartitioning, DelaunayPure,
    ExternalSalesmen,
    ExtraCandidateSetSymmetric, ExtraCandidateSetType,
    InitialTourAlgorithm,
    KarpPartitioning, KCenterPartitioning, KMeansPartitioning,
    MTSPDepot, MTSPMinSize, MTSPMaxSize, MTSPObjective,
    MoorePartitioning,
    PatchingAExtended, PatchingARestricted,
    PatchingCExtended, PatchingCRestricted,
    ProblemType, RiskThreshold,
    RohePartitioning, Salesmen, SierpinskiPartitioning,
    SubproblemBorders, SubproblemsCompressed, WeightType, WeightFormat;

thread_local FILE *ParameterFile, *ProblemFile, *PiFile, *InputTourFile,
    *TourFile, *InitialTourFile, *SubproblemTourFile, **MergeTourFile;
thread_local CostFunction Distance, D, C, c, OldDistance;
thread_local MoveFunction BestMove, BacktrackMove, BestSubsequentMove;
thread_local PenaltyFunction Penalty;

thread_local MergeTourFunction MergeWithTour;


// Sequence.h
thread_local Node **t;       /* The sequence of nodes to be used in a move */
thread_local Node **T;       /* The currently best t's */
thread_local Node **tSaved;  /* For saving t when using the BacktrackKOptMove function */
thread_local int *p;         /* The permutation corresponding to the sequence in which
                the t's occur on the tour */
thread_local int *q;         /* The inverse permutation of p */
thread_local int *incl;      /* Array: incl[i] == j, if (t[i], t[j]) is an inclusion edge */
thread_local int *cycle;     /* Array: cycle[i] is cycle number of t[i] */
thread_local GainType *G;    /* For storing the G-values in the BestKOptMove function */
thread_local int K;          /* The value K for the current K-opt move */


// gpx.h
thread_local int n_cities, n_cand;
thread_local int n_partitions_size2, n_partitions_before_fusion,
    n_partitions_after_fusion1, n_partitions_after_fusion2,
    n_partitions_after_fusion3;
thread_local int n_partitions_after_fusion4, n_partitions_after_fusion5,
    n_partitions_after_fusionB;
thread_local Node **Map2Node;


// Genetic.h
thread_local int MaxPopulationSize; /* The maximum size of the population */
thread_local int PopulationSize;    /* The current size of the population */

thread_local CrossoverFunction Crossover;

thread_local int **Population;      /* Array of individuals (solution tours) */
thread_local GainType *PenaltyFitness;  /* The f itnesslty  (tour penalty) of each
                           i                             individual */
thread_local GainType *Fitness;     /* The fitness (tour cost) of each individual */

// Delaunay.h
thread_local point *p_array;
