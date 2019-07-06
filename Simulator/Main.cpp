#include "Simulator.h"

using namespace std;
using namespace szx;

int main() {
    //Simulator::initDefaultEnvironment();

    Simulator sim;
    //sim.debug();
    //sim.benchmark(12);
	sim.parallelBenchmark(4);
    //sim.generateInstance();

    return 0;
}