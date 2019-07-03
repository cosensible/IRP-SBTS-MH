#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <random>

#include <cstring>

#include "Simulator.h"
#include "ThreadPool.h"

using namespace std;

namespace szx {

	// EXTEND[szx][5]: read it from InstanceList.txt.
	static const vector<String> instList({
		//"abs.v1h3c1n5.1", "abs.v1h3c1n5.2", "abs.v1h3c1n5.3", "abs.v1h3c1n5.4", "abs.v1h3c1n5.5",
		//"abs.v1h3c1n10.1", "abs.v1h3c1n10.2", "abs.v1h3c1n10.3", "abs.v1h3c1n10.4", "abs.v1h3c1n10.5",
		//"abs.v1h3c1n15.1", "abs.v1h3c1n15.2", "abs.v1h3c1n15.3", "abs.v1h3c1n15.4", "abs.v1h3c1n15.5",
		//"abs.v1h3c1n20.1", "abs.v1h3c1n20.2", "abs.v1h3c1n20.3", "abs.v1h3c1n20.4", "abs.v1h3c1n20.5",
		//"abs.v1h3c1n25.1", "abs.v1h3c1n25.2", "abs.v1h3c1n25.3", "abs.v1h3c1n25.4", "abs.v1h3c1n25.5",
		//"abs.v1h3c1n30.1", "abs.v1h3c1n30.2", "abs.v1h3c1n30.3", "abs.v1h3c1n30.4", "abs.v1h3c1n30.5",
		//"abs.v1h3c1n35.1", "abs.v1h3c1n35.2", "abs.v1h3c1n35.3", "abs.v1h3c1n35.4", "abs.v1h3c1n35.5",
		//"abs.v1h3c1n40.1", "abs.v1h3c1n40.2", "abs.v1h3c1n40.3", "abs.v1h3c1n40.4", "abs.v1h3c1n40.5",
		//"abs.v1h3c1n45.1", "abs.v1h3c1n45.2", "abs.v1h3c1n45.3", "abs.v1h3c1n45.4", "abs.v1h3c1n45.5",
		//"abs.v1h3c1n50.1", "abs.v1h3c1n50.2", "abs.v1h3c1n50.3", "abs.v1h3c1n50.4", "abs.v1h3c1n50.5",

		//"abs.v1h3c2n5.1", "abs.v1h3c2n5.2", "abs.v1h3c2n5.3", "abs.v1h3c2n5.4", "abs.v1h3c2n5.5",
		//"abs.v1h3c2n10.1", "abs.v1h3c2n10.2", "abs.v1h3c2n10.3", "abs.v1h3c2n10.4", "abs.v1h3c2n10.5",
		//"abs.v1h3c2n15.1", "abs.v1h3c2n15.2", "abs.v1h3c2n15.3", "abs.v1h3c2n15.4", "abs.v1h3c2n15.5",
		//"abs.v1h3c2n20.1", "abs.v1h3c2n20.2", "abs.v1h3c2n20.3", "abs.v1h3c2n20.4", "abs.v1h3c2n20.5",
		//"abs.v1h3c2n25.1", "abs.v1h3c2n25.2", "abs.v1h3c2n25.3", "abs.v1h3c2n25.4", "abs.v1h3c2n25.5",
		//"abs.v1h3c2n30.1", "abs.v1h3c2n30.2", "abs.v1h3c2n30.3", "abs.v1h3c2n30.4", "abs.v1h3c2n30.5",
		//"abs.v1h3c2n35.1", "abs.v1h3c2n35.2", "abs.v1h3c2n35.3", "abs.v1h3c2n35.4", "abs.v1h3c2n35.5",
		//"abs.v1h3c2n40.1", "abs.v1h3c2n40.2", "abs.v1h3c2n40.3", "abs.v1h3c2n40.4", "abs.v1h3c2n40.5",
		//"abs.v1h3c2n45.1", "abs.v1h3c2n45.2", "abs.v1h3c2n45.3", "abs.v1h3c2n45.4", "abs.v1h3c2n45.5",
		//"abs.v1h3c2n50.1", "abs.v1h3c2n50.2", "abs.v1h3c2n50.3", "abs.v1h3c2n50.4", "abs.v1h3c2n50.5",

		//"abs.v1h6c1n5.1", "abs.v1h6c1n5.2", "abs.v1h6c1n5.3", "abs.v1h6c1n5.4", "abs.v1h6c1n5.5",
		//"abs.v1h6c1n10.1", "abs.v1h6c1n10.2", "abs.v1h6c1n10.3", "abs.v1h6c1n10.4", "abs.v1h6c1n10.5",
		//"abs.v1h6c1n15.1", "abs.v1h6c1n15.2", "abs.v1h6c1n15.3", "abs.v1h6c1n15.4", "abs.v1h6c1n15.5",
		//"abs.v1h6c1n20.1", "abs.v1h6c1n20.2", "abs.v1h6c1n20.3", "abs.v1h6c1n20.4", "abs.v1h6c1n20.5",
		//"abs.v1h6c1n25.1", "abs.v1h6c1n25.2", "abs.v1h6c1n25.3", "abs.v1h6c1n25.4", "abs.v1h6c1n25.5",
		//"abs.v1h6c1n30.1", "abs.v1h6c1n30.2", "abs.v1h6c1n30.3", "abs.v1h6c1n30.4", "abs.v1h6c1n30.5",

		//"abs.v1h6c2n5.1", "abs.v1h6c2n5.2", "abs.v1h6c2n5.3", "abs.v1h6c2n5.4", "abs.v1h6c2n5.5",
		//"abs.v1h6c2n10.1", "abs.v1h6c2n10.2", "abs.v1h6c2n10.3", "abs.v1h6c2n10.4", "abs.v1h6c2n10.5",
		//"abs.v1h6c2n15.1", "abs.v1h6c2n15.2", "abs.v1h6c2n15.3", "abs.v1h6c2n15.4", "abs.v1h6c2n15.5",
		//"abs.v1h6c2n20.1", "abs.v1h6c2n20.2", "abs.v1h6c2n20.3", "abs.v1h6c2n20.4", "abs.v1h6c2n20.5",
		//"abs.v1h6c2n25.1", "abs.v1h6c2n25.2", "abs.v1h6c2n25.3", "abs.v1h6c2n25.4", "abs.v1h6c2n25.5",
		//"abs.v1h6c2n30.1", "abs.v1h6c2n30.2", "abs.v1h6c2n30.3", "abs.v1h6c2n30.4", "abs.v1h6c2n30.5",

		//"abs.v1h6c1n50.1", "abs.v1h6c1n50.2", "abs.v1h6c1n50.3", "abs.v1h6c1n50.4", "abs.v1h6c1n50.5", "abs.v1h6c1n50.6", "abs.v1h6c1n50.7", "abs.v1h6c1n50.8", "abs.v1h6c1n50.9", "abs.v1h6c1n50.10",
		//"abs.v1h6c1n100.1", "abs.v1h6c1n100.2", "abs.v1h6c1n100.3", "abs.v1h6c1n100.4", "abs.v1h6c1n100.5", "abs.v1h6c1n100.6", "abs.v1h6c1n100.7", "abs.v1h6c1n100.8", "abs.v1h6c1n100.9", "abs.v1h6c1n100.10",
		//"abs.v1h6c1n200.1", "abs.v1h6c1n200.2", "abs.v1h6c1n200.3", "abs.v1h6c1n200.4", "abs.v1h6c1n200.5", "abs.v1h6c1n200.6", "abs.v1h6c1n200.7", "abs.v1h6c1n200.8", "abs.v1h6c1n200.9", "abs.v1h6c1n200.10",
		//"abs.v1h6c2n50.1", "abs.v1h6c2n50.2", "abs.v1h6c2n50.3", "abs.v1h6c2n50.4", "abs.v1h6c2n50.5", "abs.v1h6c2n50.6", "abs.v1h6c2n50.7", "abs.v1h6c2n50.8", "abs.v1h6c2n50.9", "abs.v1h6c2n50.10",
		//"abs.v1h6c2n100.1", "abs.v1h6c2n100.2", "abs.v1h6c2n100.3", "abs.v1h6c2n100.4", "abs.v1h6c2n100.5", "abs.v1h6c2n100.6", "abs.v1h6c2n100.7", "abs.v1h6c2n100.8", "abs.v1h6c2n100.9", "abs.v1h6c2n100.10",
		//"abs.v1h6c2n200.1", "abs.v1h6c2n200.2", "abs.v1h6c2n200.3", "abs.v1h6c2n200.4", "abs.v1h6c2n200.5", "abs.v1h6c2n200.6", "abs.v1h6c2n200.7", "abs.v1h6c2n200.8", "abs.v1h6c2n200.9", "abs.v1h6c2n200.10"

		"abs.v1h6c2n200.1", "abs.v1h6c2n200.2", "abs.v1h6c2n200.3", "abs.v1h6c2n200.4", "abs.v1h6c2n200.5", "abs.v1h6c2n200.6", "abs.v1h6c2n200.8", "abs.v1h6c2n200.9",
		"abs.v1h6c2n100.4", //"abs.v1h6c2n100.6",
		"abs.v1h6c1n200.1", "abs.v1h6c1n200.2", "abs.v1h6c1n200.6",
		"abs.v1h6c1n100.2", "abs.v1h6c1n100.3",
		"abs.v1h6c1n50.6"

		});

	void Simulator::initDefaultEnvironment() {
		Solver::Environment env;
		env.save(Env::DefaultEnvPath());

		Solver::Configuration cfg;
		cfg.save(Env::DefaultCfgPath());
	}

	void Simulator::exe(const Task &task) {
		System::makeSureDirExist(SolutionDir());

		ostringstream oss;
		oss << ProgramName()
			<< " " << Cmd::InstancePathOption() << " " << InstanceDir() << task.instanceName()
			<< " " << Cmd::SolutionPathOption() << " " << SolutionDir() << task.solutionName();

		auto addOption = [&](const String &key, const String &value) {
			if (!value.empty()) { oss << " " << key << " " << value; }
		};

		addOption(Cmd::RandSeedOption(), task.randSeed);
		addOption(Cmd::TimeoutOption(), task.timeout);
		addOption(Cmd::MaxIterOption(), task.maxIter);
		addOption(Cmd::JobNumOption(), task.jobNum);
		addOption(Cmd::RunIdOption(), task.runId);
		addOption(Cmd::ConfigPathOption(), task.cfgPath);
		addOption(Cmd::LogPathOption(), task.logPath);

		System::exec(oss.str());
	}

	void Simulator::run(const Task &task) {
		System::makeSureDirExist(SolutionDir());

		char argBuf[Cmd::MaxArgNum][Cmd::MaxArgLen];
		char *argv[Cmd::MaxArgNum];
		for (int i = 0; i < Cmd::MaxArgNum; ++i) { argv[i] = argBuf[i]; }
		strcpy(argv[ArgIndex::ExeName], ProgramName().c_str());

		int argc = ArgIndex::ArgStart;

		strcpy(argv[argc++], Cmd::InstancePathOption().c_str());
		strcpy(argv[argc++], (InstanceDir() + task.instanceName()).c_str());
		strcpy(argv[argc++], Cmd::SolutionPathOption().c_str());
		strcpy(argv[argc++], (SolutionDir() + task.solutionName()).c_str());

		auto addOption = [&](const String &key, const String &value) {
			if (!value.empty()) {
				strcpy(argv[argc++], key.c_str());
				strcpy(argv[argc++], value.c_str());
			}
		};

		addOption(Cmd::RandSeedOption(), task.randSeed);
		addOption(Cmd::TimeoutOption(), task.timeout);
		addOption(Cmd::MaxIterOption(), task.maxIter);
		addOption(Cmd::JobNumOption(), task.jobNum);
		addOption(Cmd::RunIdOption(), task.runId);
		addOption(Cmd::ConfigPathOption(), task.cfgPath);
		addOption(Cmd::LogPathOption(), task.logPath);

		Cmd::run(argc, argv);
	}

	void Simulator::run(const String &envPath) {
		char argBuf[Cmd::MaxArgNum][Cmd::MaxArgLen];
		char *argv[Cmd::MaxArgNum];
		for (int i = 0; i < Cmd::MaxArgNum; ++i) { argv[i] = argBuf[i]; }
		strcpy(argv[ArgIndex::ExeName], ProgramName().c_str());

		int argc = ArgIndex::ArgStart;

		strcpy(argv[argc++], Cmd::EnvironmentPathOption().c_str());
		strcpy(argv[argc++], envPath.c_str());

		Cmd::run(argc, argv);
	}

	void Simulator::debug() {
		Task task;
		task.instSet = "";
		task.instId = "abs.v1h6c1n100.10";
		task.timeout = "360";
		task.randSeed = "1559429277";
		//task.randSeed = to_string(Random::generateSeed());
		task.jobNum = "1";
		task.cfgPath = Env::DefaultCfgPath();
		task.logPath = Env::DefaultLogPath();
		task.runId = "0";

		run(task);
	}

	vector<string> split(const string &str, const string &pattern)
	{
		//const char* convert to char*
		char * strc = new char[strlen(str.c_str()) + 1];
		strcpy(strc, str.c_str());
		vector<string> resultVec;
		char* tmpStr = strtok(strc, pattern.c_str());
		while (tmpStr != NULL)
		{
			resultVec.push_back(string(tmpStr));
			tmpStr = strtok(NULL, pattern.c_str());
		}

		delete[] strc;

		return resultVec;
	}

	void Simulator::benchmark(int repeat) {
	/*	Task task;
		task.instSet = "";
		task.timeout = "360";
		task.jobNum = "1";
		task.cfgPath = Env::DefaultCfgPath();
		task.logPath = Env::DefaultLogPath();

		random_device rd;
		mt19937 rgen(rd());
		//for (int i = 0; i < repeat; ++i) {
		//    //shuffle(instList.begin(), instList.end(), rgen);
		//    for (auto inst = instList.rbegin(); inst != instList.rend(); ++inst) {
		//        task.instId = *inst;
		//        task.randSeed = to_string(Random::generateSeed());
		//        task.runId = to_string(i);
		//        run(task);
		//    }
		//}
		fstream inst_list;
		inst_list.open("list.txt", ios::in);
		vector<String> all_inst;
		String tmp;
		getline(inst_list, tmp);
		all_inst = split(tmp, ",");
		for (auto a = all_inst.begin(); a != all_inst.end(); a++)cout << *a << endl;

		for (int i = 0; i < repeat; ++i) {
			//shuffle(instList.begin(), instList.end(), rgen);
			for (auto inst = all_inst.begin(); inst != all_inst.end(); ++inst) {
				task.instId = *inst;
				task.randSeed = to_string(Random::generateSeed());
				task.runId = to_string(i);
				run(task);
			}
		}*/

		Task task;
		task.instSet = "";

		task.instId = "abs.v1h6c1n100.8";
		task.randSeed = "1559477260";

		//task.instId = "abs.v1h6c1n100.9";
		//task.randSeed = "1559427260";

		//task.instId = "abs.v1h6c1n100.10";
		//task.randSeed = "1559429277";

		task.timeout = "360";
		task.jobNum = "1";
		task.cfgPath = Env::DefaultCfgPath();
		task.logPath = Env::DefaultLogPath();
		for (int i = 0; i < repeat; ++i) {
			//task.randSeed = to_string(Random::generateSeed());
			task.runId = to_string(i);
			run(task);
			this_thread::sleep_for(1s);
		}
	}

	//void Simulator::parallelBenchmark(int repeat) {
	//	Task task;
	//	task.instSet = "";
	//	task.timeout = "360";
	//	task.jobNum = "1";
	//	task.cfgPath = Env::DefaultCfgPath();
	//	task.logPath = Env::DefaultLogPath();
	//	ThreadPool<> tp(15);
	//	random_device rd;
	//	mt19937 rgen(rd());
	//	for (int i = 0; i < repeat; ++i) {
	//		for (auto inst = instList.begin(); inst != instList.end(); ++inst) {
	//			//for (auto inst = instList.rbegin(); inst != instList.rend(); ++inst) {
	//			task.instId = *inst;
	//			task.runId = to_string(i);
	//			task.randSeed = to_string(Random::generateSeed());
	//			//tp.push([=]() { run(task); });
	//			tp.push([=]() { exe(task); });
	//			this_thread::sleep_for(1s);
	//		}
	//	}
	//}

	void Simulator::parallelrun(Task task, int repeat) {
		for (int i = 0; i < repeat; ++i) {
			task.runId = to_string(i);
			task.randSeed = to_string(Random::generateSeed());
			exe(task);
			this_thread::sleep_for(2s);
		}
	}

	void Simulator::parallelBenchmark(int repeat) {
		Task task;
		task.instSet = "";
		task.timeout = "3000";
		task.jobNum = "1";
		task.cfgPath = Env::DefaultCfgPath();
		task.logPath = Env::DefaultLogPath();

		ThreadPool<> tp(15);
		random_device rd;
		mt19937 rgen(rd());
		for (auto inst = instList.begin(); inst != instList.end(); ++inst) {
		//for (auto inst = instList.rbegin(); inst != instList.rend(); ++inst) {
			task.instId = *inst;
			//tp.push([=]() { run(task); });
			tp.push([&, task, repeat]() { parallelrun(task, repeat); });
			this_thread::sleep_for(10s);
		}
	}

	void Simulator::generateInstance(const InstanceTrait &trait) {
		Random rand;

		Problem::Input input;

		// EXTEND[szx][5]: generate random instances.

		ostringstream path;
		path << InstanceDir() << "rand.h" << input.periodnum()
			<< "c" << trait.holdingCostScale
			<< "n" << input.nodes().size() << ".json";
		save(path.str(), input);
	}

	void Simulator::convertInstanceToPb(const String &fileName, const InstanceTrait &trait) {
		ifstream ifs(InstanceDir() + "abs/" + fileName + ".txt");
		if (!ifs.is_open()) { return; }

		Problem::Input input;

		int nodeNum, periodNum, vehicleCapacity;
		ifs >> nodeNum >> periodNum >> vehicleCapacity;
		input.set_periodnum(periodNum);
		input.set_depotnum(trait.depotNum);

		vehicleCapacity /= trait.vehicleNum; // all vehicles share the capacity.
		for (int v = 0; v < trait.vehicleNum; ++v) {
			auto &vehicle(*input.add_vehicles());
			vehicle.set_capacity(vehicleCapacity);
		}

		int id;
		double x;
		double y;
		int initialQuantity;
		int capacity;
		int minLevel = 0;
		double holdingCost;
		int unitDemand;

		auto setNodeInformation = [&](pb::Node &node) {
			node.set_x(x);
			node.set_y(y);
			node.set_initquantity(initialQuantity);
			node.set_capacity(capacity);
			node.set_minlevel(minLevel);
			node.set_holdingcost(holdingCost);
			for (int p = 0; p < periodNum; ++p) { node.add_demands(unitDemand); }
		};

		// supplier.
		auto &supplier(*input.add_nodes());
		ifs >> id >> x >> y >> initialQuantity >> unitDemand >> holdingCost;
		capacity = initialQuantity + unitDemand * periodNum;
		unitDemand = -unitDemand;
		setNodeInformation(supplier);
		// customers.
		for (int i = 1; i < nodeNum; ++i) {
			auto &node(*input.add_nodes());
			ifs >> id >> x >> y >> initialQuantity >> capacity >> minLevel >> unitDemand >> holdingCost;
			setNodeInformation(node);
		}
		ifs.close();

		ostringstream path;
		path << InstanceDir() << fileName << ".json";
		save(path.str(), input);
	}

	void Simulator::convertAllInstancesToPb(const InstanceTrait &trait) {
		for (auto i = instList.begin(); i != instList.end(); ++i) {
			convertInstanceToPb(*i, trait);
		}
	}

}
