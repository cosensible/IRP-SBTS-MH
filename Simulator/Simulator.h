////////////////////////////////
/// usage : 1.  prepare the input, call the solver through command line interface
///             or class library interface, and then collect output for analysis.
///
/// note :  1.
////////////////////////////////

#ifndef SIMULATOR_H
#define SIMULATOR_H


#include <string>
#include <vector>

#include "../Solver/Solver.h"
#include "../Solver/Problem.h"
#include "../Solver/Utility.h"


namespace szx {

	using Cmd = Solver::Cli;
	using Env = Solver::Environment;

	class Simulator {
#pragma region Type
	public:
		// TODO[szx][4]: make it be able to run with serveral env and cfg.
		struct Task {
			String instanceName() const { return instSet + instId + ".json"; }
			String solutionName() const { return instanceName(); }


			String instSet;
			String instId;
			String randSeed;
			String timeout;
			String maxIter;
			String jobNum;
			String cfgPath;
			String logPath;
			String runId;
		};

		struct InstanceTrait {
			int depotNum = 1;
			int vehicleNum = 1;
			int holdingCostScale = 1;
		};
#pragma endregion Type

#pragma region Constant
	public:
		static String InstanceDir() { return Env::DefaultInstanceDir(); }
		static String SolutionDir() { return Env::DefaultSolutionDir(); }

		//static String ProgramName() { return "Simulator.exe";  }
		static String ProgramName() { return "Solver.exe"; }

		enum ArgIndex { ExeName = 0, ArgStart };
#pragma endregion Constant

#pragma region Constructor
	public:
#pragma endregion Constructor

#pragma region Method
	public:
		static void initDefaultEnvironment();

		// invoke solver with arguments explicitly specifying running information.
		// invoke solver by ABI with arguments explicitly specifying running information.
		void exe(const Task &task);

		// invoke solver by API with arguments explicitly specifying running information.
		void run(const Task &task);
		// invoke solver with single argument of environment file path.
		void run(const String &envPath = Env::DefaultEnvPath());

		// utility for debugging solver with certain arguments.
		void debug();
		// utility for testing all instances.
		void benchmark(int repeat = 1);
		// utility for testing all instances using a thread pool.
		void parallelBenchmark(int repeat);


		void generateInstance(const InstanceTrait &trait = InstanceTrait());
		static void convertInstanceToPb(const String &filePath, const InstanceTrait &trait = InstanceTrait());
		static void convertAllInstancesToPb(const InstanceTrait &trait = InstanceTrait());
#pragma endregion Method

#pragma region Field
	public:
#pragma endregion Field
	};

}


#endif  // SIMULATOR_H
