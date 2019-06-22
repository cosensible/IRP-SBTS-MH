#include "Simulator.h"

using namespace std;
using namespace szx;

int main() {
    //Simulator::initDefaultEnvironment();

    Simulator sim;
    //sim.debug();
    //sim.benchmark(2);
	sim.parallelBenchmark(6);
    //sim.generateInstance();

    return 0;
}