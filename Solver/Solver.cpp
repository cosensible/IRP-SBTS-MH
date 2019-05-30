#include "Solver.h"

#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <mutex>

#include <cmath>

#include "CsvReader.h"
#include "MpSolver.h"
#include "CachedTspSolver.h"

using namespace std;

namespace szx {

#pragma region Solver::Cli
	int Solver::Cli::run(int argc, char * argv[]) {
		Log(LogSwitch::Szx::Cli) << "parse command line arguments." << endl;
		Set<String> switchSet;
		Map<String, char*> optionMap({ // use string as key to compare string contents instead of pointers.
			{ InstancePathOption(), nullptr },
			{ SolutionPathOption(), nullptr },
			{ RandSeedOption(), nullptr },
			{ TimeoutOption(), nullptr },
			{ MaxIterOption(), nullptr },
			{ JobNumOption(), nullptr },
			{ RunIdOption(), nullptr },
			{ EnvironmentPathOption(), nullptr },
			{ ConfigPathOption(), nullptr },
			{ LogPathOption(), nullptr }
			});

		for (int i = 1; i < argc; ++i) { // skip executable name.
			auto mapIter = optionMap.find(argv[i]);
			if (mapIter != optionMap.end()) { // option argument.
				mapIter->second = argv[++i];
			}
			else { // switch argument.
				switchSet.insert(argv[i]);
			}
		}

		Log(LogSwitch::Szx::Cli) << "execute commands." << endl;
		if (switchSet.find(HelpSwitch()) != switchSet.end()) {
			cout << HelpInfo() << endl;
		}

		if (switchSet.find(AuthorNameSwitch()) != switchSet.end()) {
			cout << AuthorName() << endl;
		}

		Solver::Environment env;
		env.load(optionMap);
		if (env.instPath.empty() || env.slnPath.empty()) { return -1; }

		Solver::Configuration cfg;
		cfg.load(env.cfgPath);

		Log(LogSwitch::Szx::Input) << "load instance " << env.instPath << " (seed=" << env.randSeed << ")." << endl;
		Problem::Input input;
		if (!input.load(env.instPath)) { return -1; }

		Solver solver(input, env, cfg);
		solver.solve();

		pb::Submission submission;
		submission.set_thread(to_string(env.jobNum));
		submission.set_instance(env.friendlyInstName());
		//submission.set_duration(to_string(solver.timer.elapsedSeconds()) + "s");
		submission.set_duration(to_string(Timer::durationInSecond(solver.timer.getStartTime(), solver.bestSlnTime)) + "s");
		
		submission.set_obj(solver.output.totalCost);

		solver.output.save(env.slnPath, submission);
#if SZX_DEBUG
		solver.output.save(env.solutionPathWithTime(), submission);
		solver.record();
#endif // SZX_DEBUG

		return 0;
	}
#pragma endregion Solver::Cli

#pragma region Solver::Environment
	void Solver::Environment::load(const Map<String, char*> &optionMap) {
		char *str;

		str = optionMap.at(Cli::EnvironmentPathOption());
		if (str != nullptr) { loadWithoutCalibrate(str); }

		str = optionMap.at(Cli::InstancePathOption());
		if (str != nullptr) { instPath = str; }

		str = optionMap.at(Cli::SolutionPathOption());
		if (str != nullptr) { slnPath = str; }

		str = optionMap.at(Cli::RandSeedOption());
		if (str != nullptr) { randSeed = atoi(str); }

		str = optionMap.at(Cli::TimeoutOption());
		if (str != nullptr) { msTimeout = static_cast<Duration>(atof(str) * Timer::MillisecondsPerSecond); }

		str = optionMap.at(Cli::MaxIterOption());
		if (str != nullptr) { maxIter = atoi(str); }

		str = optionMap.at(Cli::JobNumOption());
		if (str != nullptr) { jobNum = atoi(str); }

		str = optionMap.at(Cli::RunIdOption());
		if (str != nullptr) { rid = str; }

		str = optionMap.at(Cli::ConfigPathOption());
		if (str != nullptr) { cfgPath = str; }

		str = optionMap.at(Cli::LogPathOption());
		if (str != nullptr) { logPath = str; }

		calibrate();
	}

	void Solver::Environment::load(const String &filePath) {
		loadWithoutCalibrate(filePath);
		calibrate();
	}

	void Solver::Environment::loadWithoutCalibrate(const String &filePath) {
		// EXTEND[szx][8]: load environment from file.
		// EXTEND[szx][8]: check file existence first.
	}

	void Solver::Environment::save(const String &filePath) const {
		// EXTEND[szx][8]: save environment to file.
	}
	void Solver::Environment::calibrate() {
		// adjust thread number.
		int threadNum = thread::hardware_concurrency();
		if ((jobNum <= 0) || (jobNum > threadNum)) { jobNum = threadNum; }

		// adjust timeout.
		msTimeout -= Environment::SaveSolutionTimeInMillisecond;
	}
#pragma endregion Solver::Environment

#pragma region Solver::Configuration
	void Solver::Configuration::load(const String &filePath) {
		// EXTEND[szx][5]: load configuration from file.
		// EXTEND[szx][8]: check file existence first.
	}

	void Solver::Configuration::save(const String &filePath) const {
		// EXTEND[szx][5]: save configuration to file.
	}
#pragma endregion Solver::Configuration

#pragma region Solver 
	bool Solver::solve() {
		init();

		int workerNum = (max)(1, env.jobNum / cfg.threadNumPerWorker);
		cfg.threadNumPerWorker = env.jobNum / workerNum;
		List<Solution> solutions(workerNum, Solution(this));
		List<bool> success(workerNum);

		Log(LogSwitch::Szx::Framework) << "launch " << workerNum << " workers." << endl;
		List<thread> threadList;
		threadList.reserve(workerNum);
		for (int i = 0; i < workerNum; ++i) {
			// TODO[szx][2]: as *this is captured by ref, the solver should support concurrency itself, i.e., data members should be read-only or independent for each worker.
			// OPTIMIZE[szx][3]: add a list to specify a series of algorithm to be used by each threads in sequence.
			threadList.emplace_back([&, i]() { success[i] = optimize(solutions[i], i); });
		}
		for (int i = 0; i < workerNum; ++i) { threadList.at(i).join(); }

		Log(LogSwitch::Szx::Framework) << "collect best result among all workers." << endl;
		int bestIndex = -1;
		double bestValue = 0;
		for (int i = 0; i < workerNum; ++i) {
			if (!success[i]) { continue; }
			Log(LogSwitch::Szx::Framework) << "worker " << i << " got " << solutions[i].totalCost << endl;
			if (solutions[i].totalCost <= bestValue) { continue; }
			bestIndex = i;
			bestValue = solutions[i].totalCost;
		}

		env.rid = to_string(bestIndex);
		if (bestIndex < 0) { return false; }
		output = solutions[bestIndex];
		return true;
	}

	void Solver::record() const {
#if SZX_DEBUG
		int generation = 0;

		ostringstream log;

		System::MemoryUsage mu = System::peakMemoryUsage();

		// load reference results.
		CsvReader cr;
		ifstream ifs(Environment::DefaultInstanceDir() + "Baseline.csv");
		if (!ifs.is_open()) { return; }
		const List<CsvReader::Row> &rows(cr.scan(ifs));
		ifs.close();
		String bestObj, refObj, refTime;
		for (auto r = rows.begin(); r != rows.end(); ++r) {
			if (env.friendlyInstName() != r->front()) { continue; }
			bestObj = (*r)[1];
			refObj = (*r)[2];
			refTime = (*r)[3];
			//double opt = stod(bestObj);
			break;
		}

		double checkerObj = -1;
		bool feasible = check(checkerObj);
		double objDiff = round(output.totalCost * Problem::CheckerObjScale - checkerObj) / Problem::CheckerObjScale;

		// record basic information.
		log << env.friendlyLocalTime() << ","
			<< env.rid << ","
			<< env.instPath << ","
			<< feasible << "," << objDiff << ","
			<< output.totalCost << ","
			<< bestObj << ","
			<< refObj << ","
			//<< timer.elapsedSeconds() << ","
			<< Timer::durationInSecond(timer.getStartTime(), bestSlnTime) << ","
			<< refTime << ","
			<< mu.physicalMemory << "," << mu.virtualMemory << ","
			<< env.randSeed << ","
			<< cfg.toBriefStr() << ","
			<< generation << "," << iteration;

		// record solution vector.
		// EXTEND[szx][2]: save solution in log.
		log << endl;

		// append all text atomically.
		static mutex logFileMutex;
		lock_guard<mutex> logFileGuard(logFileMutex);

		ofstream logFile(env.logPath, ios::app);
		logFile.seekp(0, ios::end);
		if (logFile.tellp() <= 0) {
			logFile << "Time,ID,Instance,Feasible,ObjMatch,Cost,MinCost,RefCost,Duration,RefDuration,PhysMem,VirtMem,RandSeed,Config,Generation,Iteration,Solution" << endl;
		}
		logFile << log.str();
		logFile.close();
#endif // SZX_DEBUG
	}

	bool Solver::check(double &checkerObj) const {
#if SZX_DEBUG
		enum CheckerFlag {
			IoError = 0x0,
			FormatError = 0x1,
			MultipleVisitsError = 0x2,
			UnmatchedLoadDeliveryError = 0x4,
			ExceedCapacityError = 0x8,
			RunOutOfStockError = 0x10
		};

		int errorCode = System::exec("Checker.exe " + env.instPath + " " + env.solutionPathWithTime());
		if (errorCode > 0) {
			checkerObj = errorCode;
			return true;
		}
		errorCode = ~errorCode;
		if (errorCode == CheckerFlag::IoError) { Log(LogSwitch::Checker) << "IoError." << endl; }
		if (errorCode & CheckerFlag::FormatError) { Log(LogSwitch::Checker) << "FormatError." << endl; }
		if (errorCode & CheckerFlag::MultipleVisitsError) { Log(LogSwitch::Checker) << "MultipleVisitsError." << endl; }
		if (errorCode & CheckerFlag::UnmatchedLoadDeliveryError) { Log(LogSwitch::Checker) << "UnmatchedLoadDeliveryError." << endl; }
		if (errorCode & CheckerFlag::ExceedCapacityError) { Log(LogSwitch::Checker) << "ExceedCapacityError." << endl; }
		if (errorCode & CheckerFlag::RunOutOfStockError) { Log(LogSwitch::Checker) << "RunOutOfStockError." << endl; }
		return false;
#else
		checkerObj = 0;
		return true;
#endif // SZX_DEBUG
	}

	void Solver::init() {
		nodeNum = input.nodes_size(), periodNum = input.periodnum();
		aux.routingCost.init(nodeNum, nodeNum);
		aux.routingCost.reset();
		aux.bestVisits.init(periodNum, nodeNum);
		aux.curVisits.init(periodNum, nodeNum);
		aux.curTours.init(periodNum);	//当前路由集合
		aux.tourPrices.init(periodNum);	//每个路由对应的成本
		H1.resize(BitSize); H2.resize(BitSize); H3.resize(BitSize);

		ID n = 0;
		for (auto i = input.nodes().begin(); i != input.nodes().end(); ++i, ++n) {
			ID m = 0;
			for (auto j = input.nodes().begin(); j != i; ++j, ++m) {
				double value = round(hypot(i->x() - j->x(), i->y() - j->y()));
				aux.routingCost[n][m] = aux.routingCost[m][n] = value;
			}
		}

		aux.initHoldingCost = 0;
		for (auto i = input.nodes().begin(); i != input.nodes().end(); ++i) {
			aux.initHoldingCost += i->holdingcost() * i->initquantity();
		}
	}

	//记录解对应的visits、tours、tourPrices
	void Solver::initialSln(Solution &sln) {
		aux.bestCost = sln.totalCost;
		aux.tourPrices.reset();
		aux.bestVisits.reset();
		for (ID p = 0; p < periodNum; ++p) {
			aux.bestVisits[p][0] = 1;	//仓库访问置一，对结果无影响
			aux.curTours[p].clear();
			for (ID v = 0; v < input.vehicles_size(); ++v) {
				auto &delivs(*sln.mutable_periodroutes(p)->mutable_vehicleroutes(v)->mutable_deliveries());
				if (!delivs.empty()) {
					aux.curTours[p].push_back(delivs.rbegin()->node()); // 路线中加入仓库
					for (auto n = delivs.cbegin(); n != delivs.cend(); ++n) {
						aux.bestVisits[p][n->node()] = 1;
						aux.curTours[p].push_back(n->node());
					}
				}
				else { aux.curTours[p] = { 0,0 }; }
				for (auto n = aux.curTours[p].cbegin(), m = n + 1; m != aux.curTours[p].cend(); ++n, ++m) {
					aux.tourPrices[p] += aux.routingCost[*n][*m];
				}
			}
		}
	}

	int Solver::buildMixNeigh(Arr2D<ID> &visits, Price minCost) {
		aux.mixNeigh.clear();
		List<Actor> delNeigh, movNeigh, swpNeigh;

		for (ID n = 1; n < nodeNum; ++n) {
			List<ID> P0, P1;
			for (ID p = 0; p < periodNum; ++p) {
				if (visits[p][n]) {
					P1.push_back(p);
					Actor act(ActorType::DEL, delNodeTourCost(p, n), 0.0, -1, -1, p, n);
					if (!isTabu(hashValue1, hashValue2, hashValue3, act)) {
						delNeigh.push_back(std::move(act));
					}
				}
				else { P0.push_back(p); }
			}

			for (ID &p0 : P0) {
				for (ID &p1 : P1) {
					Actor act(ActorType::MOV, movNodeTourCost(p0, n, p1, n), 0.0, p0, n, p1, n);
					if (isTabu(hashValue1, hashValue2, hashValue3, act)) { continue; }
					movNeigh.push_back(std::move(act));
				}
			}
		}

		for (ID n = 1; n < nodeNum; ++n) {
			for (ID m = n + 1; m < nodeNum; ++m) {
				List<ID> tvn, tvm;
				for (ID p = 0; p < periodNum; ++p) {
					if (visits[p][n] && !visits[p][m]) { tvn.push_back(p); }
					if (!visits[p][n] && visits[p][m]) { tvm.push_back(p); }
				}
				for (ID t1 : tvn) {
					for (ID t2 : tvm) {
						Actor act(ActorType::SWP, swpNodeTourCost(t1, n, t2, m), 0.0, t1, n, t2, m);
						if (isTabu(hashValue1, hashValue2, hashValue3, act)) { continue; }
						swpNeigh.push_back(std::move(act));
					}
				}
			}
		}

		std::sort(delNeigh.begin(), delNeigh.end(), [](Actor &a1, Actor &a2) {return a1.totalCost < a2.totalCost; });
		std::sort(movNeigh.begin(), movNeigh.end(), [](Actor &a1, Actor &a2) {return a1.totalCost < a2.totalCost; });
		std::sort(swpNeigh.begin(), swpNeigh.end(), [](Actor &a1, Actor &a2) {return a1.totalCost < a2.totalCost; });
		unsigned maxSize = 2 * periodNum * static_cast<unsigned>(sqrt(nodeNum));

		ID p1, n1, p2, n2;
		for (ID i = 0; i < maxSize && i < delNeigh.size(); ++i) {
			auto &del(delNeigh[i]);
			execTabu(hashValue1, hashValue2, hashValue3, del);
			p2 = del.p2, n2 = del.n2;
			visits[p2][n2] = 0;
			if (( del.modelCost= callModel(visits)) >= 0) {
				del.totalCost += del.modelCost;
				if (Math::strongLess(del.totalCost, minCost)) {
					aux.mixNeigh.clear();
					minCost = del.totalCost;
					aux.mixNeigh.push_back(std::move(del));
				}
				else if (Math::weakEqual(del.totalCost, minCost)) {
					aux.mixNeigh.push_back(std::move(del));
				}
			}
			visits[p2][n2] = 1;
		}
		
		for (ID i = 0; i < maxSize && i < movNeigh.size(); ++i) {
			auto &mov(movNeigh[i]);
			execTabu(hashValue1, hashValue2, hashValue3, mov);
			p1 = mov.p1, n1 = mov.n1, p2 = mov.p2, n2 = mov.n2;
			visits[p1][n1] = 1; visits[p2][n2] = 0;
			if ((mov.modelCost = callModel(visits)) >= 0) {
				mov.totalCost += mov.modelCost;
				if (Math::strongLess(mov.totalCost, minCost)) {
					aux.mixNeigh.clear();
					minCost = mov.totalCost;
					aux.mixNeigh.push_back(std::move(mov));
				}
				else if (Math::weakEqual(mov.totalCost, minCost)) {
					aux.mixNeigh.push_back(std::move(mov));
				}
			}
			visits[p1][n1] = 0; visits[p2][n2] = 1;
		}
		
		for (ID i = 0; i < maxSize && i < swpNeigh.size(); ++i) {
			auto &swp(swpNeigh[i]);
			execTabu(hashValue1, hashValue2, hashValue3, swp);
			p1 = swp.p1, n1 = swp.n1, p2 = swp.p2, n2 = swp.n2;
			visits[p1][n1] = visits[p2][n2] = 0;
			visits[p1][n2] = visits[p2][n1] = 1;
			if ((swp.modelCost = callModel(visits)) >= 0) {
				swp.totalCost += swp.modelCost;
				if (Math::strongLess(swp.totalCost, minCost)) {
					aux.mixNeigh.clear();
					minCost = swp.totalCost;
					aux.mixNeigh.push_back(std::move(swp));
				}
				else if (Math::weakEqual(swp.totalCost, minCost)) {
					aux.mixNeigh.push_back(std::move(swp));
				}
			}
			visits[p1][n1] = visits[p2][n2] = 1;
			visits[p1][n2] = visits[p2][n1] = 0;
		}
		
		return aux.mixNeigh.size();
	}

	void Solver::disturb(Arr2D<ID> &visits) {
		ID addNumber = 2 + rand.pick(2), delNumber = 1 + rand.pick(2), movNumber = 4 + rand.pick(3);
		do {
			List<ID> room, addOpts, delOpts;
			// 添加操作
			for (ID p = 0; p < periodNum; ++p) {
				for (ID n = 1; n < nodeNum; ++n) {
					if (!visits[p][n]) { room.push_back(p*nodeNum + n); }
				}
			}
			sampling(room, addOpts, addNumber);
			for (ID &vid : addOpts) { visits[vid / nodeNum][vid % nodeNum] = 1; }

			// 移动操作
			List<std::pair<ID, ID>> movRoom;
			for (ID n = 1; n < nodeNum; ++n) {
				for (ID p1 = 0; p1 < periodNum; ++p1) {
					for (ID p2 = p1 + 1; p2 < periodNum; ++p2) {
						if (visits[p1][n] && !visits[p2][n]) { movRoom.push_back({ p2*nodeNum + n,p1*nodeNum + n }); }
						if (!visits[p1][n] && visits[p2][n]) { movRoom.push_back({ p1*nodeNum + n,p2*nodeNum + n }); }
					}
				}
			}
			addOpts.clear();
			for (ID mvNum = 0; mvNum < movNumber && movRoom.size()>0;) {
				ID pos = rand.pick(movRoom.size());
				auto &mov(movRoom[pos]);
				if (find(addOpts.begin(), addOpts.end(), mov.first) == addOpts.end() &&
					find(delOpts.begin(), delOpts.end(), mov.second) == delOpts.end()) {
					visits[mov.first / nodeNum][mov.first % nodeNum] = 1;
					visits[mov.second / nodeNum][mov.second % nodeNum] = 0;
					if (callModel(visits) < 0) {
						visits[mov.first / nodeNum][mov.first % nodeNum] = 0;
						visits[mov.second / nodeNum][mov.second % nodeNum] = 1;
					}
					else {
						++mvNum;
						addOpts.push_back(mov.first);
						delOpts.push_back(mov.second);
					}
				}
				movRoom.erase(movRoom.begin() + pos);
			}

			//删除操作
			room.clear();
			for (ID p = 0; p < periodNum; ++p) {
				for (ID n = 1; n < nodeNum; ++n) {
					if (visits[p][n]) { room.push_back(p*nodeNum + n); }
				}
			}
			for (ID delNum = 0; delNum < delNumber && room.size()>0;) {
				ID pos = rand.pick(room.size());
				auto &del(room[pos]);
				visits[del / nodeNum][del % nodeNum] = 0;
				if (callModel(visits) < 0) {
					visits[del / nodeNum][del % nodeNum] = 1;
				}
				else { ++delNum; }
				room.erase(room.begin() + pos);
			}

		} while (!timer.isTimeOut() && isTabu(visits));

		Price totalCost = callModel(visits) + callLKH(visits);
		Log(LogSwitch::Szx::Search) << "After disturb, cost=" << totalCost << endl;

		if (Math::strongLess(totalCost, aux.bestCost)) {
			bestSlnTime = szx::Timer::Clock::now();
			aux.bestCost = totalCost;
			aux.bestVisits = visits;
			Log(LogSwitch::Szx::Opt) << "By disturb, opt=" << aux.bestCost << endl;
		}

		mixTabuSearch(visits, totalCost);
	}

	bool Solver::mixTabuSearch(Arr2D<ID> &visits, Price modelCost) {
		execTabu(visits, true);
		bool isImproved = false; ID mixNeighSize;
		for (ID step = 0; !timer.isTimeOut() && step < alpha && (mixNeighSize = buildMixNeigh(visits)); ++step) {
			const auto &act(aux.mixNeigh[rand.pick(mixNeighSize)]);
			if (act.actype == ActorType::SWP) {
				visits[act.p1][act.n1] = visits[act.p2][act.n2] = 0;
				visits[act.p1][act.n2] = visits[act.p2][act.n1] = 1;
			}
			else {
				if (act.n1 > 0) visits[act.p1][act.n1] = 1;
				if (act.n2 > 0) visits[act.p2][act.n2] = 0;
			}
			modelCost = act.modelCost + callLKH(visits, act.p1, act.p2);
			execTabu(act);
			if (Math::strongLess(modelCost, aux.bestCost)) {
				bestSlnTime = szx::Timer::Clock::now();
				isImproved = true;
				step = -1;
				aux.bestCost = modelCost;
				aux.bestVisits = visits;
				Log(LogSwitch::Szx::Opt) << "By TS, opt=" << aux.bestCost << endl;
			}
		}
		Log(LogSwitch::Szx::Search) << "After TS, cost = " << modelCost << "\n" << endl;
		return isImproved;
	}

	void Solver::mixFinalSearch() {
		double gamma = 0.99;	// epsilon 衰减系数
		for (int i = 0; !timer.isTimeOut(); ++i) {
			int p = 100000 * std::pow(gamma, i);	
			p = p < 50000 ? 50000 : p;	// 从最优解出发的概率不能小于 0.5
			if (rand.pick(100000) < p) { aux.curVisits = aux.bestVisits; }
			else { Log(LogSwitch::Szx::Search) << "choose current solution!!!" << endl; }
			disturb(aux.curVisits);
		}

		//aux.curVisits = aux.bestVisits;
		//disturb(aux.curVisits);
	}

	void Solver::execSearch(Solution &sln) {
		timer = Timer(2100s, timer.getStartTime());
		bestSlnTime = timer.getEndTime();

		iteratedModel(sln);

		for (ID p = 0; p < periodNum - 2; ++p) {
			getNeighWithModel(sln, aux.bestVisits, { p,p + 1,p + 2 }, 120);
		}

		for (ID i = 0; i < 2; ++i) {
			for (ID p = 0; p < periodNum - 1; ++p) {
				getNeighWithModel(sln, aux.bestVisits, { p,p + 1 });
			}
		}
		
		aux.curVisits = aux.bestVisits;
		mixTabuSearch(aux.curVisits, aux.bestCost);
		mixFinalSearch();

		getBestSln(sln, aux.bestVisits);
	}

	bool Solver::optimize(Solution &sln, ID workerId) {
		Log(LogSwitch::Szx::Framework) << "worker " << workerId << " starts." << endl;
		sln.init(periodNum, input.vehicles_size(), Problem::MaxCost);

		execSearch(sln);

		Log(LogSwitch::Szx::Framework) << "worker " << workerId << " ends." << endl;
		return true;
	}

	void Solver::iteratedModel(Solution &sln) {
		ID vehicleNum = input.vehicles_size();
		const auto &nodes(*input.mutable_nodes());
		MpSolver::Configuration mpCfg(MpSolver::InternalSolver::GurobiMip, env.timeoutInSecond(), true, false);
		MpSolver mp(mpCfg); mp.setMaxThread(4);

		// delivery[p, v, n] is the quantity delivered to node n at period p by vehicle v.
		Arr2D<Arr<Dvar>> delivery(periodNum, vehicleNum, Arr<Dvar>(nodeNum));
		// x[p, v, n, m] is true if the edge from node n to node m is visited at period p by vehicle v.
		Arr2D<Arr2D<Dvar>> x(periodNum, vehicleNum, Arr2D<Dvar>(nodeNum, nodeNum));

		// quantityLevel[n, p] is the rest quantity of node n at period p after the delivery and consumption have happened.
		Arr2D<Expr> quantityLevel(nodeNum, periodNum);
		// degrees[n] is the sum of in-coming edges.
		Arr2D<Arr<Expr>> degrees(periodNum, vehicleNum, Arr<Expr>(nodeNum));

		// add decision variables.
		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				for (ID n = 0; n < input.depotnum(); ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, -capacity, 0);
				}
				for (ID n = input.depotnum(); n < nodeNum; ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, 0, capacity);
				}
				Arr2D<Dvar> &xpv(x.at(p, v));
				for (ID n = 0; n < nodeNum; ++n) {
					for (ID m = 0; m < nodeNum; ++m) {
						if (n == m) { continue; }
						xpv.at(n, m) = mp.addVar(MpSolver::VariableType::Bool, 0, 1);
					}
				}
			}
		}

		// add constraints.
		for (ID n = 0; n < nodeNum; ++n) {
			Expr quantity = nodes[n].initquantity();
			for (ID p = 0; p < periodNum; ++p) {
				for (ID v = 0; v < vehicleNum; ++v) {
					quantity += delivery[p][v][n];
				}
				// node capacity constraint.
				mp.addConstraint(quantity <= nodes[n].capacity());
				quantity -= nodes[n].demands(p);
				mp.addConstraint(0 <= quantity);
				quantityLevel[n][p] = quantity;
			}
		}

		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Expr quantity;
				for (ID n = 0; n < nodeNum; ++n) {
					quantity += delivery[p][v][n];
				}
				// quantity matching constraint.
				mp.addConstraint(quantity == 0);
			}
		}

		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Arr2D<Dvar> &xpv(x.at(p, v));
				for (ID n = 0; n < nodeNum; ++n) {
					Expr inDegree;
					for (ID m = 0; m < n; ++m) { inDegree += xpv.at(m, n); }
					for (ID m = n + 1; m < nodeNum; ++m) { inDegree += xpv.at(m, n); }
					degrees[p][v][n] = inDegree;
					Expr outDegree;
					for (ID m = 0; m < n; ++m) { outDegree += xpv.at(n, m); }
					for (ID m = n + 1; m < nodeNum; ++m) { outDegree += xpv.at(n, m); }
					// path connectivity constraint.
					mp.addConstraint(inDegree == outDegree); // OPTIMIZE[szx][0]: use undirected graph version? (degree == 2)
					// delivery precondition constraint.
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					double quantityCoef = (n >= input.depotnum()) ? 1 : -1;
					mp.addConstraint(quantityCoef * delivery[p][v][n] <= capacity * inDegree);
					if (n >= input.depotnum()) {
						// visit precondition constraint.
						mp.addConstraint(delivery[p][v][n] >= inDegree); // OPTIMIZE[szx][2]: omit it since it will be satisfied automatically?
					}
					// maximal visit constraint.
					mp.addConstraint(inDegree <= 1); // OPTIMIZE[szx][2]: omit it since it will be satisfied automatically?
				}
			}
		}

		// add objective.
		Expr obj;
		Expr holdingCost = aux.initHoldingCost;
		for (ID n = 0; n < nodeNum; ++n) {
			for (ID p = 0; p < periodNum; ++p) {
				holdingCost += (nodes[n].holdingcost() * quantityLevel.at(n, p));
			}
		}
		Expr routingCost;
		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Arr2D<Dvar> &xpv(x.at(p, v));
				for (ID n = 0; n < nodeNum; ++n) {
					for (ID m = 0; m < nodeNum; ++m) {
						if (n == m) { continue; }
						routingCost += (aux.routingCost.at(n, m) * xpv.at(n, m));
					}
				}
			}
		}

		double tourcostFactor = 1 + 1.0*rand.pick(8, 13) / 10;
		obj = holdingCost + tourcostFactor * routingCost;
		//obj = holdingCost + 2 * routingCost;
		mp.addObjective(obj, MpSolver::OptimaOrientation::Minimize, 0, 0, 0, env.timeoutInSecond());

		// add callbacks.
		auto subTourHandler = [&](MpSolver::MpEvent &e) {
			enum EliminationPolicy { // OPTIMIZE[szx][0]: first sub-tour, best sub-tour or all sub-tours?
				NoSubTour = 0x0,
				AllSubTours = 0x1,
				FirstSubTour = 0x2,
				BestSubTour = 0x4
			};
			EliminationPolicy policy = EliminationPolicy::BestSubTour;

			List<ID> bestTour; // tour with least nodes/hops.
			List<ID> tour;
			tour.reserve(nodeNum);
			Arr<bool> visited(nodeNum);
			for (ID p = 0; p < periodNum; ++p) {
				for (ID v = 0; v < vehicleNum; ++v) {
					Arr2D<Dvar> &xpv(x.at(p, v));
					tour.clear();
					visited.reset(Arr<bool>::ResetOption::AllBits0);
					for (ID s = 0; s < nodeNum; ++s) { // check if there is a route start from each node.
						if (visited[s]) { continue; }
						ID prev = s;
						do {
							for (ID n = 0; n < nodeNum; ++n) {
								if (prev == n) { continue; }
								if (!e.isTrue(xpv.at(prev, n))) { continue; }
								if (s >= input.depotnum()) { tour.push_back(n); } // the sub-tour containing depots should not be eliminated.
								prev = n;
								visited[n] = true;
								break;
							}
						} while (prev != s);
						if (tour.empty()) { continue; }

						if (policy & (EliminationPolicy::AllSubTours | EliminationPolicy::FirstSubTour)) {
							Expr edges;
							for (auto n = tour.begin(); n != tour.end(); prev = *n, ++n) {
								edges += xpv.at(prev, *n);
							}
							e.addLazy(edges <= static_cast<double>(tour.size() - 1));
							if (policy & EliminationPolicy::FirstSubTour) { break; }
						}

						if (bestTour.empty() || (tour.size() < bestTour.size())) { swap(bestTour, tour); }
					}
					if ((policy & EliminationPolicy::BestSubTour) && !bestTour.empty()) {
						Expr edges;
						ID prev = bestTour.back();
						for (auto n = bestTour.begin(); n != bestTour.end(); prev = *n, ++n) {
							edges += xpv.at(prev, *n);
						}
						e.addLazy(edges <= static_cast<double>(bestTour.size() - 1));
					}
				}
			}
		};

		static const String TspCacheDir("TspCache/");
		System::makeSureDirExist(TspCacheDir);
		CachedTspSolver tspSolver(nodeNum, TspCacheDir + env.friendlyInstName() + ".csv");

		Solution curSln;
		curSln.init(periodNum, vehicleNum);
		auto nodeSetHandler = [&](MpSolver::MpEvent &e) {
			//if (e.getObj() > sln.totalCost) { e.stop(); } // there could be bad heuristic solutions.

			// OPTIMIZE[szx][0]: check the bound and only apply this to the optimal sln.

			lkh::CoordList2D coords; // OPTIMIZE[szx][3]: use adjacency matrix to avoid re-calculation and different rounding?
			coords.reserve(nodeNum);
			List<ID> nodeIdMap(nodeNum);
			List<bool> containNode(nodeNum);
			lkh::Tour tour;
			//Expr nodeDiff;

			curSln.totalCost = 0;
			for (ID p = 0; p < periodNum; ++p) {
				auto &periodRoute(*curSln.mutable_periodroutes(p));
				for (ID v = 0; v < vehicleNum; ++v) {
					Arr2D<Dvar> &xpv(x.at(p, v));
					coords.clear();
					fill(containNode.begin(), containNode.end(), false);
					for (ID n = 0; n < nodeNum; ++n) {
						bool visited = false;
						for (ID m = 0; m < nodeNum; ++m) {
							if (n == m) { continue; }
							if (!e.isTrue(xpv.at(n, m))) { continue; }
							nodeIdMap[coords.size()] = n;
							containNode[n] = true;
							coords.push_back(lkh::Coord2D(nodes[n].x() * Precision, nodes[n].y() * Precision));
							visited = true;
							break;
						}
						//nodeDiff += (visited ? (1 - degrees[p][v][n]) : degrees[p][v][n]);
					}
					auto &route(*periodRoute.mutable_vehicleroutes(v));
					route.clear_deliveries();
					if (coords.size() > 2) { // repair the relaxed solution.
						tspSolver.solve(tour, containNode, coords, [&](ID n) { return nodeIdMap[n]; });
					}
					else if (coords.size() == 2) { // trivial cases.
						tour.nodes.resize(2);
						tour.nodes[0] = nodeIdMap[0];
						tour.nodes[1] = nodeIdMap[1];
					}
					else {
						continue;
					}
					tour.nodes.push_back(tour.nodes.front());
					for (auto n = tour.nodes.begin(), m = n + 1; m != tour.nodes.end(); ++n, ++m) {
						auto &d(*route.add_deliveries());
						d.set_node(*m);
						d.set_quantity(lround(e.getValue(delivery[p][v][*m])));
						curSln.totalCost += aux.routingCost.at(*n, *m);
					}
				}
			}
			//e.addLazy(nodeDiff >= 1);
			subTourHandler(e);
			curSln.totalCost += e.getValue(holdingCost);

			if (Math::strongLess(curSln.totalCost, sln.totalCost)) {
				bestSlnTime = szx::Timer::Clock::now();
				Log(LogSwitch::Szx::Model) << "By model, opt=" << curSln.totalCost << endl;
				std::swap(curSln, sln);
			}
		};

		mp.setMipSlnEvent(nodeSetHandler);
		mp.optimize();

		initialSln(sln);	// 初始化 visits, bestCost and allTourCost
	}

	void Solver::getNeighWithModel(Solution &sln,const Arr2D<ID> &visits,const List<ID> &pl,double timeInSec) {
		Log(LogSwitch::Szx::Model) << "change period";
		for (ID p : pl) { Log(LogSwitch::Szx::Model) << " " << p; } Log(LogSwitch::Szx::Model) << endl;

		ID vehicleNum = input.vehicles_size(), chPNum = pl.size();
		const auto &nodes(*input.mutable_nodes());
		MpSolver::Configuration mpCfg(MpSolver::InternalSolver::GurobiMip, timeInSec, true, false);
		MpSolver mp(mpCfg); mp.setMaxThread(4);

		// delivery[p, v, n] is the quantity delivered to node n at period p by vehicle v.
		Arr2D<Arr<Dvar>> delivery(periodNum, vehicleNum, Arr<Dvar>(nodeNum));
		// x[p, v, n, m] is true if the edge from node n to node m is visited at period p by vehicle v.
		Arr2D<Arr2D<Dvar>> x(chPNum, vehicleNum, Arr2D<Dvar>(nodeNum, nodeNum));
		// quantityLevel[n, p] is the rest quantity of node n at period p after the delivery and consumption have happened.
		Arr2D<Expr> quantityLevel(nodeNum, periodNum);

		// add decision variables.
		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				for (ID n = 0; n < input.depotnum(); ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, -capacity, 0);
				}
				for (ID n = input.depotnum(); n < nodeNum; ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, 0, capacity);
				}
			}
		}

		for (ID i = 0; i < chPNum; ++i) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Arr2D<Dvar> &xpv(x.at(i, v));
				for (ID n = 0; n < nodeNum; ++n) {
					for (ID m = 0; m < nodeNum; ++m) {
						if (n == m) { continue; }
						xpv.at(n, m) = mp.addVar(MpSolver::VariableType::Bool, 0, 1);
					}
				}
			}
		}

		// add constraints.
		for (ID n = 0; n < nodeNum; ++n) {
			Expr quantity = nodes[n].initquantity();
			for (ID p = 0; p < periodNum; ++p) {
				for (ID v = 0; v < vehicleNum; ++v) {
					quantity += delivery[p][v][n];
				}
				// node capacity constraint.
				mp.addConstraint(quantity <= nodes[n].capacity());
				quantity -= nodes[n].demands(p);
				mp.addConstraint(0 <= quantity);
				quantityLevel[n][p] = quantity;
			}
		}

		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Expr quantity;
				for (ID n = 0; n < nodeNum; ++n) {
					quantity += delivery[p][v][n];
				}
				// quantity matching constraint.
				mp.addConstraint(quantity == 0);
			}
		}

		for (ID i = 0; i < chPNum; ++i) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Arr2D<Dvar> &xpv(x.at(i, v));
				for (ID n = 0; n < nodeNum; ++n) {
					Expr inDegree;
					for (ID m = 0; m < n; ++m) { inDegree += xpv.at(m, n); }
					for (ID m = n + 1; m < nodeNum; ++m) { inDegree += xpv.at(m, n); }
					Expr outDegree;
					for (ID m = 0; m < n; ++m) { outDegree += xpv.at(n, m); }
					for (ID m = n + 1; m < nodeNum; ++m) { outDegree += xpv.at(n, m); }
					// path connectivity constraint.
					mp.addConstraint(inDegree == outDegree);
					// delivery precondition constraint.
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					double quantityCoef = (n >= input.depotnum()) ? 1 : -1;
					mp.addConstraint(quantityCoef * delivery[pl[i]][v][n] <= capacity * inDegree);
					if (n >= input.depotnum()) {
						// visit precondition constraint.
						mp.addConstraint(delivery[pl[i]][v][n] >= inDegree);
					}
					// maximal visit constraint.
					mp.addConstraint(inDegree <= 1);
				}
			}
		}

		for (ID p = 0; p < periodNum; ++p) {
			if (find(pl.begin(), pl.end(), p) != pl.end()) { continue; }
			for (ID v = 0; v < vehicleNum; ++v) {
				for (ID n = 0; n < nodeNum; ++n) {
					// delivery precondition constraint.
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					double quantityCoef = (n >= input.depotnum()) ? 1 : -1;
					mp.addConstraint(quantityCoef * delivery[p][v][n] <= capacity * visits[p][n]);
				}
			}
		}
		
		// add objective.
		Expr obj;
		Expr holdingCost = aux.initHoldingCost;
		for (ID n = 0; n < nodeNum; ++n) {
			for (ID p = 0; p < periodNum; ++p) {
				holdingCost += (nodes[n].holdingcost() * quantityLevel.at(n, p));
			}
		}
		Expr routingCost;
		for (ID p = 0; p < periodNum; ++p) {
			if (find(pl.begin(), pl.end(), p) == pl.end()) { routingCost += aux.tourPrices[p]; }
		}
		for (ID i = 0; i < chPNum; ++i) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Arr2D<Dvar> &xpv(x.at(i, v));
				for (ID n = 0; n < nodeNum; ++n) {
					for (ID m = 0; m < nodeNum; ++m) {
						if (n == m) { continue; }
						routingCost += (aux.routingCost.at(n, m) * xpv.at(n, m));
					}
				}
			}
		}

		double tourcostFactor = 1 + 1.0*rand.pick(8, 13) / 10;
		obj = holdingCost + tourcostFactor * routingCost;
		//obj = holdingCost + 2 * routingCost;
		mp.addObjective(obj, MpSolver::OptimaOrientation::Minimize, 0, 0, 0, timeInSec);

		// add callbacks.
		auto subTourHandler = [&](MpSolver::MpEvent &e) {
			enum EliminationPolicy { // OPTIMIZE[szx][0]: first sub-tour, best sub-tour or all sub-tours?
				NoSubTour = 0x0,
				AllSubTours = 0x1,
				FirstSubTour = 0x2,
				BestSubTour = 0x4
			};
			EliminationPolicy policy = EliminationPolicy::BestSubTour;

			List<ID> bestTour; // tour with least nodes/hops.
			List<ID> tour;
			tour.reserve(nodeNum);
			Arr<bool> visited(nodeNum);

			for (ID i = 0; i < chPNum; ++i) {
				for (ID v = 0; v < vehicleNum; ++v) {
					Arr2D<Dvar> &xpv(x.at(i, v));
					tour.clear();
					visited.reset(Arr<bool>::ResetOption::AllBits0);
					for (ID s = 0; s < nodeNum; ++s) { // check if there is a route start from each node.
						if (visited[s]) { continue; }
						ID prev = s;
						do {
							for (ID n = 0; n < nodeNum; ++n) {
								if (prev == n) { continue; }
								if (!e.isTrue(xpv.at(prev, n))) { continue; }
								if (s >= input.depotnum()) { tour.push_back(n); } // the sub-tour containing depots should not be eliminated.
								prev = n;
								visited[n] = true;
								break;
							}
						} while (prev != s);
						if (tour.empty()) { continue; }

						if (policy & (EliminationPolicy::AllSubTours | EliminationPolicy::FirstSubTour)) {
							Expr edges;
							for (auto n = tour.begin(); n != tour.end(); prev = *n, ++n) {
								edges += xpv.at(prev, *n);
							}
							e.addLazy(edges <= static_cast<double>(tour.size() - 1));
							if (policy & EliminationPolicy::FirstSubTour) { break; }
						}

						if (bestTour.empty() || (tour.size() < bestTour.size())) { swap(bestTour, tour); }
					}
					if ((policy & EliminationPolicy::BestSubTour) && !bestTour.empty()) {
						Expr edges;
						ID prev = bestTour.back();
						for (auto n = bestTour.begin(); n != bestTour.end(); prev = *n, ++n) {
							edges += xpv.at(prev, *n);
						}
						e.addLazy(edges <= static_cast<double>(bestTour.size() - 1));
					}
				}
			}
		};

		static const String TspCacheDir("TspCache/");
		System::makeSureDirExist(TspCacheDir);
		CachedTspSolver tspSolver(nodeNum, TspCacheDir + env.friendlyInstName() + ".csv");

		Solution curSln; copySln(curSln, sln);
		//int step = 0, maxStep = 120;
		auto nodeSetHandler = [&](MpSolver::MpEvent &e) {
			lkh::CoordList2D coords;
			coords.reserve(nodeNum);
			List<ID> nodeIdMap(nodeNum);
			List<bool> containNode(nodeNum);
			lkh::Tour tour;

			curSln.totalCost = 0;
			for (ID i = 0; i < chPNum; ++i) {
				auto &periodRoute(*curSln.mutable_periodroutes(pl[i]));
				for (ID v = 0; v < vehicleNum; ++v) {
					Arr2D<Dvar> &xpv(x.at(i, v));
					coords.clear();
					fill(containNode.begin(), containNode.end(), false);
					for (ID n = 0; n < nodeNum; ++n) {
						bool visited = false;
						for (ID m = 0; m < nodeNum; ++m) {
							if (n == m) { continue; }
							if (!e.isTrue(xpv.at(n, m))) { continue; }
							nodeIdMap[coords.size()] = n;
							containNode[n] = true;
							coords.push_back(lkh::Coord2D(nodes[n].x() * Precision, nodes[n].y() * Precision));
							visited = true;
							break;
						}
					}
					auto &route(*periodRoute.mutable_vehicleroutes(v));
					route.clear_deliveries();
					if (coords.size() > 2) { // repair the relaxed solution.
						tspSolver.solve(tour, containNode, coords, [&](ID n) { return nodeIdMap[n]; });
					}
					else if (coords.size() == 2) { // trivial cases.
						tour.nodes.resize(2);
						tour.nodes[0] = nodeIdMap[0];
						tour.nodes[1] = nodeIdMap[1];
					}
					else {
						continue;
					}
					tour.nodes.push_back(tour.nodes.front());
					for (auto n = tour.nodes.begin(), m = n + 1; m != tour.nodes.end(); ++n, ++m) {
						auto &d(*route.add_deliveries());
						d.set_node(*m);
						d.set_quantity(lround(e.getValue(delivery[pl[i]][v][*m])));
						curSln.totalCost += aux.routingCost.at(*n, *m);
					}
				}
			}
			for (ID p = 0; p < periodNum; ++p) {
				if (find(pl.begin(), pl.end(), p) != pl.end()) { continue; }
				curSln.totalCost += aux.tourPrices[p];
				for (ID v = 0; v < vehicleNum; ++v) {
					auto &delivs(*curSln.mutable_periodroutes(p)->mutable_vehicleroutes(v)->mutable_deliveries());
					for (auto n = delivs.begin(); n != delivs.end(); ++n) {
						n->set_quantity(lround(e.getValue(delivery[p][v][n->node()])));
					}
				}
			}
			subTourHandler(e);
			curSln.totalCost += e.getValue(holdingCost);
			//Log(LogSwitch::Szx::Model) << "curSln=" << curSln.totalCost << ", sln=" << sln.totalCost << endl;

			//printSln(curSln);

			if (Math::strongLess(curSln.totalCost, sln.totalCost)) {
				bestSlnTime = szx::Timer::Clock::now();
				Log(LogSwitch::Szx::Model) << /*"step=" << step <<*/ ", By " << chPNum << " periods neighbor, opt=" << curSln.totalCost << endl;
				std::swap(curSln, sln);
				//step = -1;
			}

			//Log(LogSwitch::Szx::Model) << "step=" << step << endl;
			//++step;
			//if (step >= maxStep) { e.stop(); }
		};

		mp.setMipSlnEvent(nodeSetHandler);
		mp.optimize();

		initialSln(sln);
	}

	Price Solver::callModel(Arr2D<int> &visits) {
		ID vehicleNum = input.vehicles_size();
		const auto &nodes(*input.mutable_nodes());
		MpSolver::Configuration mpCfg(MpSolver::InternalSolver::GurobiMip);
		MpSolver mp(mpCfg);

		// delivery[p, v, n] is the quantity delivered to node n at period p by vehicle v.
		Arr2D<Arr<Dvar>> delivery(periodNum, vehicleNum, Arr<Dvar>(nodeNum));
		// quantityLevel[n, p] is the rest quantity of node n at period p after the delivery and consumption have happened.
		Arr2D<Expr> quantityLevel(nodeNum, periodNum);

		// add decision variables.
		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				for (ID n = 0; n < input.depotnum(); ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, -capacity, 0);
				}
				for (ID n = input.depotnum(); n < nodeNum; ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, 0, capacity);
				}
			}
		}

		// add constraints.
		for (ID n = 0; n < nodeNum; ++n) {
			Expr quantity = nodes[n].initquantity();
			for (ID p = 0; p < periodNum; ++p) {
				for (ID v = 0; v < vehicleNum; ++v) {
					quantity += delivery[p][v][n];
				}
				// node capacity constraint.
				mp.addConstraint(quantity <= nodes[n].capacity());
				quantity -= nodes[n].demands(p);
				mp.addConstraint(0 <= quantity);
				quantityLevel[n][p] = quantity;
			}
		}

		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Expr quantity;
				for (ID n = 0; n < nodeNum; ++n) {
					quantity += delivery[p][v][n];

					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					double quantityCoef = (n >= input.depotnum()) ? 1 : -1;
					mp.addConstraint(quantityCoef * delivery[p][v][n] <= capacity * visits[p][n]);
				}
				// quantity matching constraint.
				mp.addConstraint(quantity == 0);
			}
		}

		// add objective.
		Expr holdingCost = aux.initHoldingCost;
		for (ID n = 0; n < nodeNum; ++n) {
			for (ID p = 0; p < periodNum; ++p) {
				holdingCost += (nodes[n].holdingcost() * quantityLevel.at(n, p));
			}
		}
		mp.addObjective(holdingCost, MpSolver::OptimaOrientation::Minimize);

		if (mp.optimize()) {
			return mp.getObjectiveValue();
		}
		return -1;
	}

	Price Solver::callLKH(const Arr2D<ID> &visits, ID p1, ID p2) {
		const auto &nodes(*input.mutable_nodes());
		static const String TspCacheDir("TspCache/");
		System::makeSureDirExist(TspCacheDir);
		CachedTspSolver tspSolver(nodeNum, TspCacheDir + env.friendlyInstName() + ".csv");

		lkh::CoordList2D coords;
		coords.reserve(nodeNum);
		List<ID> nodeIdMap(nodeNum);
		List<bool> containNode(nodeNum);
		lkh::Tour tour;

		List<ID> periods;
		if (p1 >= 0) { periods.push_back(p1); }
		if (p2 >= 0) { periods.push_back(p2); }
		if (p1 < 0 && p2 < 0) { for (ID p = 0; p < periodNum; ++p) { periods.push_back(p); } }

		for (ID p : periods) {
			coords.clear();
			aux.curTours[p].clear();
			aux.tourPrices[p] = 0;
			fill(containNode.begin(), containNode.end(), false);
			for (ID n = 0; n < nodeNum; ++n) {
				if (visits[p][n]) {
					nodeIdMap[coords.size()] = n;
					containNode[n] = true;
					coords.push_back(lkh::Coord2D(nodes[n].x() * Precision, nodes[n].y() * Precision));
				}
			}
			if (coords.size() > 2) { // repair the relaxed solution.
				tspSolver.solve(tour, containNode, coords, [&](ID n) { return nodeIdMap[n]; });
			}
			else if (coords.size() == 2) { // trivial cases.
				tour.nodes.resize(2);
				tour.nodes[0] = nodeIdMap[0];
				tour.nodes[1] = nodeIdMap[1];
			}
			else {
				aux.curTours[p] = { 0,0 };
				continue;
			}
			tour.nodes.push_back(tour.nodes.front());
			for (auto n = tour.nodes.begin(), m = n + 1; m != tour.nodes.end(); ++n, ++m) {
				aux.tourPrices[p] += aux.routingCost.at(*n, *m);
			}
			swap(aux.curTours[p], tour.nodes);
		}
		Price tourCost = 0.0;
		for (auto &price : aux.tourPrices) { tourCost += price; }
		return tourCost;
	}

	Price Solver::callLKH4Cost(const Arr2D<ID> &visits,ID p1,ID p2) {
		const auto &nodes(*input.mutable_nodes());
		static const String TspCacheDir("TspCache/");
		System::makeSureDirExist(TspCacheDir);
		CachedTspSolver tspSolver(nodeNum, TspCacheDir + env.friendlyInstName() + ".csv");

		lkh::CoordList2D coords;
		coords.reserve(nodeNum);
		List<ID> nodeIdMap(nodeNum);
		List<bool> containNode(nodeNum);
		lkh::Tour tour;

		List<ID> periods;
		if (p1 >= 0) { periods.push_back(p1); }
		if (p2 >= 0) { periods.push_back(p2); }
		if (p1 < 0 && p2 < 0) { for (ID p = 0; p < periodNum; ++p) { periods.push_back(p); } }

		Price tourCost = 0.0;
		for (ID p = 0; p < periodNum; ++p) {
			if (find(periods.begin(), periods.end(), p) != periods.end()) { continue; }
			tourCost += aux.tourPrices[p];
		}

		for (ID p : periods) {
			coords.clear();
			fill(containNode.begin(), containNode.end(), false);
			for (ID n = 0; n < nodeNum; ++n) {
				if (visits[p][n]) {
					nodeIdMap[coords.size()] = n;
					containNode[n] = true;
					coords.push_back(lkh::Coord2D(nodes[n].x() * Precision, nodes[n].y() * Precision));
				}
			}
			if (coords.size() > 2) { // repair the relaxed solution.
				tspSolver.solve(tour, containNode, coords, [&](ID n) { return nodeIdMap[n]; });
			}
			else if (coords.size() == 2) { // trivial cases.
				tour.nodes.resize(2);
				tour.nodes[0] = nodeIdMap[0];
				tour.nodes[1] = nodeIdMap[1];
			}
			else {
				continue;
			}
			tour.nodes.push_back(tour.nodes.front());
			for (auto n = tour.nodes.begin(), m = n + 1; m != tour.nodes.end(); ++n, ++m) {
				tourCost += aux.routingCost.at(*n, *m);
			}
		}
		return tourCost;
	}

	void Solver::getBestSln(Solution &sln, const Arr2D<ID> &visits) {
		ID vehicleNum = input.vehicles_size();
		const auto &nodes(*input.mutable_nodes());
		MpSolver::Configuration mpCfg(MpSolver::InternalSolver::GurobiMip);
		MpSolver mp(mpCfg);

		// delivery[p, v, n] is the quantity delivered to node n at period p by vehicle v.
		Arr2D<Arr<Dvar>> delivery(periodNum, vehicleNum, Arr<Dvar>(nodeNum));
		// quantityLevel[n, p] is the rest quantity of node n at period p after the delivery and consumption have happened.
		Arr2D<Expr> quantityLevel(nodeNum, periodNum);

		// add decision variables.
		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				for (ID n = 0; n < input.depotnum(); ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, -capacity, 0);
				}
				for (ID n = input.depotnum(); n < nodeNum; ++n) {
					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					delivery[p][v][n] = mp.addVar(MpSolver::VariableType::Real, 0, capacity);
				}
			}
		}

		// add constraints.
		for (ID n = 0; n < nodeNum; ++n) {
			Expr quantity = nodes[n].initquantity();
			for (ID p = 0; p < periodNum; ++p) {
				for (ID v = 0; v < vehicleNum; ++v) {
					quantity += delivery[p][v][n];
				}
				// node capacity constraint.
				mp.addConstraint(quantity <= nodes[n].capacity());
				quantity -= nodes[n].demands(p);
				mp.addConstraint(0 <= quantity);
				quantityLevel[n][p] = quantity;
			}
		}

		for (ID p = 0; p < periodNum; ++p) {
			for (ID v = 0; v < vehicleNum; ++v) {
				Expr quantity;
				for (ID n = 0; n < nodeNum; ++n) {
					quantity += delivery[p][v][n];

					Quantity capacity = min(input.vehicles(v).capacity(), nodes[n].capacity());
					double quantityCoef = (n >= input.depotnum()) ? 1 : -1;
					mp.addConstraint(quantityCoef * delivery[p][v][n] <= capacity * visits[p][n]);
				}
				// quantity matching constraint.
				mp.addConstraint(quantity == 0);
			}
		}

		// add objective.
		Expr holdingCost = aux.initHoldingCost;
		for (ID n = 0; n < nodeNum; ++n) {
			for (ID p = 0; p < periodNum; ++p) {
				holdingCost += (nodes[n].holdingcost() * quantityLevel.at(n, p));
			}
		}
		mp.addObjective(holdingCost, MpSolver::OptimaOrientation::Minimize);

		if (mp.optimize()) {
			sln.totalCost = callLKH(visits) + mp.getObjectiveValue();
			for (ID p = 0; p < periodNum; ++p) {
				for (ID v = 0; v < vehicleNum; ++v) {
					auto &route(*sln.mutable_periodroutes(p)->mutable_vehicleroutes(v));
					route.clear_deliveries();
					if (aux.curTours[p].size() > 2) {
						for (auto n = aux.curTours[p].begin() + 1; n != aux.curTours[p].end(); ++n) {
							auto &d(*route.add_deliveries());
							d.set_node(*n);
							d.set_quantity(lround(mp.getValue(delivery[p][v][*n])));
						}
					}
				}
			}
		}
	}

	Price Solver::addNodeTourCost(ID pid, ID nid) {
		Price curCost, minCost = Problem::MaxCost;
		for (auto n = aux.curTours[pid].cbegin(), m = n + 1; m != aux.curTours[pid].cend(); ++n, ++m) {
			curCost = aux.routingCost[*n][nid] + aux.routingCost[nid][*m] - aux.routingCost[*n][*m];
			if (Math::strongLess(curCost, minCost)) { minCost = curCost; }
		}
		return minCost;
	}

	Price Solver::delNodeTourCost(ID pid, ID nid) {
		auto pos = find(aux.curTours[pid].begin(), aux.curTours[pid].end(), nid);
		auto pre = pos - 1, succ = pos + 1;
		//for (auto &n : aux.curTours[pid]) { cout << n << " "; }cout << endl;
		//cout << "pid=" << pid << ", nid=" << nid << ", pre=" << *pre << ", succ=" << *succ << endl;
		Price delta = aux.routingCost[*pre][*succ] - aux.routingCost[nid][*pre] - aux.routingCost[nid][*succ];
		return delta;
	}

	Price Solver::movNodeTourCost(ID apid, ID anid, ID dpid, ID dnid) {
		return addNodeTourCost(apid, anid) + delNodeTourCost(dpid, dnid);
	}

	Price Solver::swpNodeTourCost(ID p1, ID n1, ID p2, ID n2) {
		return delNodeTourCost(p1, n1) + delNodeTourCost(p2, n2)
			+ addNodeTourCost(p2, n1) + addNodeTourCost(p1, n2);
	}

	unsigned Solver::hash(const Arr2D<ID> &visits, double gamma) {
		unsigned long long sum = 0;
		for (ID p = 0; p < periodNum; ++p) {
			for (ID n = 0; n < nodeNum; ++n) {
				if (visits[p][n]) {
					sum += static_cast<unsigned>(std::pow(p*nodeNum + n, gamma));
				}
			}
		}
		return sum % BitSize;
	}

	bool Solver::isTabu(unsigned long hv1, unsigned long hv2, unsigned long hv3, const Actor& act) {
		if (act.actype == ActorType::SWP) {
			hv1 = hv1 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma1)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma1))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma1)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma1));
			hv2 = hv2 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma2)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma2))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma2)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma2));
			hv3 = hv3 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma3)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma3))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma3)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma3));
		}
		else {
			ID vid1 = act.p1*nodeNum + act.n1, vid2 = act.p2*nodeNum + act.n2;
			switch (act.actype) {
			case ADD:vid2 = 0; break;
			case DEL:vid1 = 0; break;
			}
			hv1 += (static_cast<unsigned>(std::pow(vid1, gamma1)) - static_cast<unsigned>(std::pow(vid2, gamma1)));
			hv2 += (static_cast<unsigned>(std::pow(vid1, gamma2)) - static_cast<unsigned>(std::pow(vid2, gamma2)));
			hv3 += (static_cast<unsigned>(std::pow(vid1, gamma3)) - static_cast<unsigned>(std::pow(vid2, gamma3)));
		}
		hv1 %= BitSize, hv2 %= BitSize, hv3 %= BitSize;
		return (H1[hv1] && H2[hv2] && H3[hv3]);
	}

	bool Solver::isTabu(const Arr2D<ID> &visits) {
		unsigned hv1 = hash(visits, gamma1), hv2 = hash(visits, gamma2), hv3 = hash(visits, gamma3);
		return (H1[hv1] && H2[hv2] && H3[hv3]);
	}

	// 每次执行 tabu 不改变 hashValue
	void Solver::execTabu(unsigned long hv1, unsigned long hv2, unsigned long hv3, const Actor& act) {
		if (act.actype == ActorType::SWP) {
			hv1 = hv1 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma1)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma1))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma1)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma1));
			hv2 = hv2 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma2)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma2))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma2)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma2));
			hv3 = hv3 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma3)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma3))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma3)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma3));
		}
		else {
			ID vid1 = act.p1*nodeNum + act.n1, vid2 = act.p2*nodeNum + act.n2;
			switch (act.actype) {
			case ADD:vid2 = 0; break;
			case DEL:vid1 = 0; break;
			}
			hv1 += (static_cast<unsigned>(std::pow(vid1, gamma1)) - static_cast<unsigned>(std::pow(vid2, gamma1)));
			hv2 += (static_cast<unsigned>(std::pow(vid1, gamma2)) - static_cast<unsigned>(std::pow(vid2, gamma2)));
			hv3 += (static_cast<unsigned>(std::pow(vid1, gamma3)) - static_cast<unsigned>(std::pow(vid2, gamma3)));
		}
		hv1 %= BitSize, hv2 %= BitSize, hv3 %= BitSize;
		H1[hv1] = H2[hv2] = H3[hv3] = 1;
	}

	// 每次执行 tabu 都会改变 hashValue
	void Solver::execTabu(const Actor& act) {
		if (act.actype == ActorType::SWP) {
			hashValue1 = hashValue1 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma1)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma1))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma1)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma1));
			hashValue2 = hashValue2 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma2)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma2))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma2)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma2));
			hashValue3 = hashValue3 - static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n1, gamma3)) - static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n2, gamma3))
				+ static_cast<unsigned>(std::pow(act.p1*nodeNum + act.n2, gamma3)) + static_cast<unsigned>(std::pow(act.p2*nodeNum + act.n1, gamma3));
		}
		else {
			ID vid1 = act.p1*nodeNum + act.n1, vid2 = act.p2*nodeNum + act.n2;
			switch (act.actype) {
			case ADD:vid2 = 0; break;
			case DEL:vid1 = 0; break;
			}
			hashValue1 += (static_cast<unsigned>(std::pow(vid1, gamma1)) - static_cast<unsigned>(std::pow(vid2, gamma1)));
			hashValue2 += (static_cast<unsigned>(std::pow(vid1, gamma2)) - static_cast<unsigned>(std::pow(vid2, gamma2)));
			hashValue3 += (static_cast<unsigned>(std::pow(vid1, gamma3)) - static_cast<unsigned>(std::pow(vid2, gamma3)));
		}
		hashValue1 %= BitSize, hashValue2 %= BitSize, hashValue3 %= BitSize;
		H1[hashValue1] = H2[hashValue2] = H3[hashValue3] = 1;
	}

	void Solver::execTabu(const Arr2D<ID> &visits, bool change) {
		if (change) {
			hashValue1 = hash(visits, gamma1), hashValue2 = hash(visits, gamma2), hashValue3 = hash(visits, gamma3);
			H1[hashValue1] = H2[hashValue2] = H3[hashValue3] = 1;
		}
		else {
			unsigned hv1 = hash(visits, gamma1), hv2 = hash(visits, gamma2), hv3 = hash(visits, gamma3);
			H1[hv1] = H2[hv2] = H3[hv3] = 1;
		}
	}

	template<typename T>
	void Solver::sampling(const List<T> &pool, List<T> &res, ID K) {
		ID N = pool.size();
		K = N > K ? K : N;
		res.resize(K);
		// 前 K 个元素直接放入数组中
		for (int i = 0; i < K; ++i) { res[i] = pool[i]; }
		// K + 1 个元素开始进行概率采样
		for (int i = K; i < N; ++i) {
			int r = rand.pick(i + 1);
			if (r < K) { res[r] = pool[i]; }
		}
	}

	void Solver::copySln(Solution &lhs, Solution &rhs) {
		if (&lhs != &rhs) {
			lhs.init(periodNum, input.vehicles_size());
			lhs.totalCost = rhs.totalCost;
			for (ID p = 0; p < periodNum; ++p) {
				for (ID v = 0; v < input.vehicles_size(); ++v) {
					auto &route(*lhs.mutable_periodroutes(p)->mutable_vehicleroutes(v));
					route.clear_deliveries();
					const auto &delivs(*rhs.mutable_periodroutes(p)->mutable_vehicleroutes(v)->mutable_deliveries());
					for (auto n = delivs.cbegin(); n != delivs.cend(); ++n) {
						auto &d(*route.add_deliveries());
						d.set_node(n->node());
						d.set_quantity(n->quantity());
					}
				}
			}
		}
	}

	void Solver::printSln(Solution &sln) {
		cout << "-------- print solution ---------" << endl;
		for (ID p = 0; p < periodNum; ++p) {
			Price routingCost = 0.0;
			cout << "p=" << p << "\nrout: ";
			for (ID v = 0; v < input.vehicles_size(); ++v) {
				const auto &delivs(*sln.mutable_periodroutes(p)->mutable_vehicleroutes(v)->mutable_deliveries());
				if (!delivs.empty()) {
					ID s = delivs.rbegin()->node(); cout << s << "-";
					routingCost += aux.routingCost[s][delivs.begin()->node()];
					for (auto n = delivs.cbegin(), m = n + 1; m != delivs.cend(); ++n, ++m) {
						routingCost += aux.routingCost[n->node()][m->node()];
						cout << n->node() << "-";
					}
				}
				cout << "\ntourCost=" << routingCost << endl;
			}
		}
	}

	void Solver::printInfo() {
		cout << "-------- print info ---------" << endl;
		for (ID p = 0; p < periodNum; ++p) {
			cout << "p=" << p << "\nrout: ";
			for (auto n : aux.curTours[p]) { cout << n << "-"; }
			cout << "\ntourCost=" << aux.tourPrices[p] << endl;
		}
	}

#pragma endregion Solver

}