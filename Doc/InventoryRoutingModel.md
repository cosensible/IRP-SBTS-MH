<p style="text-align: center; font-size: 32px; font-weight:bold;">
  Inventory Routing Problem
</p>



[TOC]



# Inventory Problem

## Known

### Set

| Set | Description                   | Size        | Element         | Remark                                                         |
| ---- | ---------------------- | ----------- | ------------ | ------------------------------------------------------------ |
| $V$ | **v**ehicle set | $[1, 3]$ | $v$ |               |
| $T$ | **t**imepoint set | $[4, 7]$ | $t, p$ | timepoint $t$ is the beginning of period $t + 1$ |
| $P$ | **p**eriod set | $T - \{0\}$ | $t, p$ | timepoint $p$ is the end of period $p$ |
| $N$ | **n**ode set | $[5, 200]$ | $n, m, s$ | $s$ is the supplier |
| $N'$ | customer set | $N - \{s\}$ | $n, m$ |  |

### Constant

| Constant | Description                    | Type | Range       | Remark |
| -------- | ------------------------------ | ---- | ----------- | ------ |
| $H_{n}$ | unit inventory **h**olding cost for node $n$ | real | $[0, 0.1]$ |  |
| $C_{n}$ | **c**apacity of node $n$ | real |  |  |
| $C_{v}$ | **c**apacity of vehicle $v$ | real |  |  |
| $I_{tn}$ | **i**nventory quantity held in node $n$ at timepoint $t$ if no delivery | real |  | also denoted as $ I_{pn}$ |


## Decision

| Variable     | Description                                                | Type | Domain     | Remark                                                     |
| -------------- | ------------------------------------------------------------ | ---- | ------------- | ------------------------------------------------------------ |
| $q_{pvn}$ | delivery **q**uantity for customer $n$ at period $p$ by vehicle $v$ | real | $[0, \textrm{C}(v, n)]$ | $n \in N'$ |
| $q_{pvs}$ | delivery **q**uantity for supplier $s$ at period $p$ by vehicle $v$ | real | $[-\textrm{C}(v, s), 0]$ | |

### Convention and Function

- define function $\textrm{C}(v, n) = \min\{C_{n}, C_{v}\}$ to indicate the min capacity.
- define function $\textrm{D}(p, n) = I_{tn} - I_{pn}, \forall t = p - 1$ to indicate the demand of node $n$ at period $p$.
- define function $\textrm{y}(t, n) = \sum\limits_{v \in V} \sum\limits_{p = 1}^{t} q_{vpn}$ to indicate the cumulative delivery quantity to node $n$ until timepoint $t$.


## Objective

### minimize the holding cost **OHC (holding cost)**

the classical objective of the sub-problem.
the initial inventory before the horizon begin should also be counted into the holding cost.

$$
\min \sum_{n \in N} \sum_{t \in T} H_{n} \cdot (I_{tn} + \textrm{y}(t, n))
$$


## Constraint

all of the following constraints must be satisfied.

- **HNC (node capacity)** the held inventory quantity should be none-negative at the end of each period and not exceed the node capacity at the beginning of each period. (it can be regarded as delivery happens before consumption at the customers, and production happens before loading at the supplier)
  $$
  \textrm{D}(p, n) \le I_{tn} + \textrm{y}(p, n) \le C_{n}, \quad \forall n \in N, \forall p \in P, \forall t \in T, t = p - 1
  $$

- **HQM (quantity matching)** the loaded and total delivered quantity of the same vehicle should be equal in each period.
  $$
  \sum_{n \in N} q_{pvn} = 0, \quad \forall v \in V, \forall p \in P
  $$

- **HMD.O (max delivery)** the delivered quantity should not exceed the node capacity in each period.
  $$
  \sum_{v \in V} q_{pvn} \le C_{n}, \quad \forall n \in N, \forall p \in P
  $$

- **HVC (vehicle capacity)** (already bounded by the domain of $q_{pvs}$) the carried inventory quantity should not exceed the vehicle capacity in any period.

- **SEQ.O (economic quantity)** the delivered quantity in each delivery should not be too small to reduce the number of delivery to reduce the routing cost.

- **SMV1.O (max visit)** the number of visit to each customer should not exceed given number (e.g. $(I_{0n} - I_{|P|n} + C_{n}) / \min\{0.8 \cdot C_{n}, 0.2 \cdot C_{v}\}$).
- **SMV2.O (max visit)** the number of visited customer (per vehicle) in each period should not exceed given number.


## Note

time point and period notation.

```
P: |  1  |  2  |  3  |
T: 0     1     2     3
```



# Routing Cost Estimation

all sets, constants, variables and constraints in the Inventory Problem should be included in this model.
since the routing for each period is independent, the dimension $p$ is omitted.

## Known

### Set

| Set | Description                   | Size        | Element         | Remark                                                         |
| ---- | ---------------------- | ----------- | ------------ | ------------------------------------------------------------ |
| $N$ | node set | $|N|$ | $n, m, l, s$ | $s$ is any chosen source of the cycle (TSP model) the virtual flow (MST model) which can simply be the supplier |

### Constant

| Constant | Description                    | Type | Range       | Remark |
| -------- | ------------------------------ | ---- | ----------- | ------ |
| $D_{nm}$ | **d**istance between node $n$ and $m$ | real |  | a.k.a. routing cost |
| $D^{+}$ | **d**istance upper bound of the route | real |  | obtained by minimum spanning tree or approximate algorithm |
| $Q^{-}$ | min **q**uantity to deliver for each visited node | real |  |  |


## Decision

| Variable     | Description                                                | Type | Domain     | Remark                                                     |
| -------------- | ------------------------------------------------------------ | ---- | ------------- | ------------------------------------------------------------ |
| $x_{vnm}$ | the edge from node $n$ to node $m$ is visited | bool | $\{0, 1\}$ | $n \ne m$. it may be relaxed to real |
| $c_{vn}$ | the cumulative delivered quantity until vehicle $v$ visiting node $n$ | real | $[0, C_{v})$ | $n \ne s$ |
| $d_{vn}$ | the cumulative distance until vehicle $v$ visiting node $n$ | real | $[0, D^{+})$ | $n \ne s$ |
| $z_{nml}$ | the node $l$ is on the $m$'s side instead of node $n$'s | bool | $\{0, 1\}$ |  |
| $f_{nm}$ | the amount of flow from node $n$ to node $m$ | real | $[0, |N| - 1]$ | |


## Objective

### minimize the total distance **OTD (total distance)**

the classical objective of the sub-problem.

$$
\min \sum_{v \in V} \sum_{n \in N} \sum_{m \in N} D_{nm} \cdot x_{vnm}
$$


## Constraint

all of the constraints in one of the following versions must be satisfied.

### Relaxed Traveling Salesman (Accepting Sub-tour)

- **HPC (path connectivity)** the in-degree should be equal to the out-degree for each node.
  $$
  \sum_{m \in N} x_{vmn} = \sum_{m \in N} x_{vnm}, \quad \forall v \in V, \forall n \in N
  $$

- **HDP (delivery precondition)** the delivery only happens to the visited nodes.
  $$
  q_{vn} \le \textrm{C}(v, n) \cdot \sum_{m \in N} x_{vnm}, \quad \forall v \in V, \forall n \in N'
  $$
  $$
  - q_{vs} \le \textrm{C}(v, s) \cdot \sum_{n \in N'} x_{vsn}, \quad \forall v \in V
  $$

- **HVP.O (visit precondition)** the visit only happens to the customers with non-trivial delivery quantity.
  $$
  q_{vn} \ge Q^{-} \cdot \sum_{m \in N} x_{vnm}, \quad \forall v \in V, \forall n \in N'
  $$

- **HTO.O (tour origin)** the tour of each vehicle begins from the supplier.
  it may cut off some solutions where no delivery happens at some periods.
  $$
  \sum_{n \in N'} x_{vsn} = 1, \quad \forall v \in V
  $$

- **HMV.O (maximal visit)** each node should be visited no more than once by each vehicle.
  it should be satisfied automatically in order to optimize the objective.
  $$
  \sum_{m \in N} x_{vnm} \le 1, \quad \forall v \in V, \forall n \in N
  $$

- **HSV.O (single visit)** each node should be visited no more than once.
  $$
  \sum_{v \in V} \sum_{m \in N} x_{vnm} \le 1, \quad \forall n \in N
  $$

### Relaxed Traveling Salesman (Complete Model / Linear Relaxation)

- **HPC (path connectivity)**, **HDC (delivery precondition)**, **HTO (tour origin)**, **HMV.O (maximal visit)**, **HSV.O (single visit)**

- **HSE.L (sub-tour exclusion)** there should be at least 1 missing edge in sub-tour $\Theta$ which does not connect to the supplier.
  $$
  \sum_{(n, m) \in \Theta} x_{nm} \le |\Theta| - 1, \quad \forall \Theta \subset L, |\Theta| \ge 1
  $$

- **HID1.O (increasing delivery)** the cumulative delivered quantity will increase after visiting edge $(n, m)$.
  the right side is optional but it may tighten the bound.
  $$
  q_{vn} - C_{v} \cdot (1 - x_{vnm}) \le c_{vm} - c_{vn} \le q_{vn} + C_{v} \cdot (1 - x_{vnm}), \quad \forall v \in V, \forall n, m \in N'
  $$

- **HID2.O (increasing distance)** the cumulative distance will increase after visiting edge $(n, m)$.
  the right side is optional but it may tighten the bound. ==(try Andrade's Q2/Q4 model)==
  $$
  D_{nm} - D^{+} \cdot (1 - x_{vnm}) \le d_{vm} - d_{vn} \le D_{nm} + D^{+} \cdot (1 - x_{vnm}), \quad \forall v \in V, \forall n, m \in N'
  $$

### Relaxed Minimum Spanning Tree

This model is applied on an undirected graph so only $x_{nm}$ with $n < m$ exist.

- **HNI (node inclusion)** every node should be included in the tree.
  $$
  \sum_{n \in N} x_{nm} \ge 1, \quad \forall m \in N
  $$

- **HEN (edge number)** there should be $|N| - 1$ edges included in the tree.
  $$
  \sum_{n \in N} \sum_{m \in N} x_{nm} = |N| - 1
  $$

### Minimum Spanning Tree (Sub-tour Elimination)

.

### Minimum Spanning Tree (Cut Elimination)

.

### Minimum Spanning Tree (Side Picking)

.

### Minimum Spanning Tree (Single-commodity Flow)

This model is applied on a directed graph.
An arbitrary source $s$ should be chosen from $N$.

- **HIF (initial flow)** initial flow from the chosen source $s$.
  $$
  \sum_{n \in N} f_{sn} = |N| - 1
  $$

- **HFD (flow decay)** passing a node will consume 1 unit of flow.
  $$
  \sum_{n \in N} f_{nm} - \sum_{n \in N} f_{mn} = 1, \quad \forall m \in N - \{s\}
  $$

- **HEV (edge visiting)** there will only be flow on visited edges.
  $$
  f_{nm} \le (|N| - 1) \cdot x_{nm}, \quad \forall n, m \in N
  $$

- **HNI.O (node inclusion)** every node should be included in the tree.
  $$
  \sum_{n \in N} f_{nm} \ge 1, \quad \forall m \in N - \{s\}
  $$

- **HEN.O (edge number)** there should be $|N| - 1$ edges included in the tree.
  $$
  \sum_{n \in N} \sum_{m \in N} x_{nm} = |N| - 1
  $$



# Route Refinement

all sets, constants, variables and constraints in the Inventory Problem should be included in this model.

## Known

### Set

| Set | Description                   | Size        | Element         | Remark                                                         |
| ---- | ---------------------- | ----------- | ------------ | ------------------------------------------------------------ |
| $N^{+}$ | visited customers | $[0, |N'|]$ | $n, m$ | $N' = N^{+} \cup N^{-}$ |
| $N^{-}$ | unvisited customers | $[0, |N'|]$ | $n, m$ | $N^{+} \cap N^{-} = \varnothing$ |


## Constraint

all of the constraints in one of the following versions must be satisfied.

- **HCE.L (combination elimination)** the combination of nodes should not be exactly the same as the given ones.
  $$
  \sum_{p \in P} \sum_{v \in V} (\sum_{n \in N^{+}} (1 - \sum_{m \in N} x_{vnm}) + \sum_{n \in N^{-}} \sum_{m \in N} x_{vnm}) \ge 1
  $$



# Notation

there are several notations about the constraints.

- hard constraints
  - prefixed with **H** in the tag
  - constraints in the original problem that must be satisfied
- soft constraints
  - prefixed with **S** in the tag
  - objectives in the original problem
  - cut-off scores to the low-priority objectives in case the high-priority objectives are over dominant
- auxiliary constraints
  - prefixed with **A** in the tag
  - additional constraints for converting non-linear constraints into linear ones
- optional constraints
  - prefixed with **.O** in the tag
  - user cuts or constraints that are very likely to be ignored when changing requirement or implementation
- lazy constraints
  - suffixed with **.L** in the tag
  - constraints which are recommended to be added lazily
