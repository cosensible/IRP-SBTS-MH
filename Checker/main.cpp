#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

#include "Visualizer.h"

#include "../Solver/PbReader.h"
#include "../Solver/InventoryRouting.pb.h"


using namespace std;
using namespace szx;
using namespace pb;

int main(int argc, char *argv[]) {
    enum CheckerFlag {
        IoError = 0x0,
        FormatError = 0x1,
        MultipleVisitsError = 0x2,
        UnmatchedLoadDeliveryError = 0x4,
        ExceedCapacityError = 0x8,
        RunOutOfStockError = 0x10
    };

    string inputPath;
    string outputPath;

    if (argc > 1) {
        inputPath = argv[1];
    } else {
        cerr << "input path: " << flush;
        cin >> inputPath;
    }

    if (argc > 2) {
        outputPath = argv[2];
    } else {
        cerr << "output path: " << flush;
        cin >> outputPath;
    }

    pb::InventoryRouting::Input input;
    if (!load(inputPath, input)) { return ~CheckerFlag::IoError; }

    pb::InventoryRouting::Output output;
    ifstream ifs(outputPath);
    if (!ifs.is_open()) { return ~CheckerFlag::IoError; }
    string submission;
    getline(ifs, submission); // skip the first line.
    ostringstream oss;
    oss << ifs.rdbuf();
    jsonToProtobuf(oss.str(), output);

    // check solution.
    int error = 0;

    int nodeNum = input.nodes_size();
    const auto &periodRoutes(output.periodroutes());
    const auto &nodes(*input.mutable_nodes());
    
    // check multiple visits.
    int p = 0;
    for (auto pr = periodRoutes.begin(); pr != periodRoutes.end(); ++pr, ++p) {
        vector<int> visitedTimes(nodeNum, 0);
        for (auto vr = pr->vehicleroutes().begin(); vr != pr->vehicleroutes().end(); ++vr) {
            for (auto dl = vr->deliveries().begin(); dl != vr->deliveries().end(); ++dl) {
                ++visitedTimes[dl->node()];
            }
        }
        for (int i = input.depotnum(); i < nodeNum; ++i) {
            if (visitedTimes[i] <= 1) { continue; }
            error |= CheckerFlag::MultipleVisitsError;
            cerr << "visit node " << i << " at period " << p << " for " << visitedTimes[i] << " times." << endl;
        }
    }

    // check load-deliver equality.
    p = 0;
    for (auto pr = periodRoutes.begin(); pr != periodRoutes.end(); ++pr, ++p) {
        for (auto vr = pr->vehicleroutes().begin(); vr != pr->vehicleroutes().end(); ++vr) {
            int totalQuantity = 0;
            for (auto dl = vr->deliveries().begin(); dl != vr->deliveries().end(); ++dl) {
                totalQuantity += dl->quantity();
            }
            if (totalQuantity == 0) { continue; }
            error |= CheckerFlag::UnmatchedLoadDeliveryError;
            cerr << "delivery - load = " << totalQuantity << " at period " << p << endl;
        }
    }

    // check rest quantity.
    vector<int> restQuantity(nodeNum, 0);
    // check holding cost.
    double holdingCost = 0;
    int n = 0;
    for (auto i = nodes.begin(); i != nodes.end(); ++i, ++n) {
        restQuantity[n] = i->initquantity();
        holdingCost += i->holdingcost() * i->initquantity();
    }
    p = 0;
    for (auto pr = periodRoutes.begin(); pr != periodRoutes.end(); ++pr, ++p) {
        for (auto vr = pr->vehicleroutes().begin(); vr != pr->vehicleroutes().end(); ++vr) {
            for (auto dl = vr->deliveries().begin(); dl != vr->deliveries().end(); ++dl) {
                restQuantity[dl->node()] += dl->quantity();
            }
        }
        int n = 0;
        for (auto i = nodes.begin(); i != nodes.end(); ++i, ++n) {
            if (lround(restQuantity[n]) > i->capacity()) { error |= CheckerFlag::ExceedCapacityError; }
            restQuantity[n] -= i->demands(p);
            if (lround(restQuantity[n]) < i->minlevel()) { error |= CheckerFlag::RunOutOfStockError; }
            holdingCost += i->holdingcost() * restQuantity[n];
        }
    }

    // check routing cost.
    auto distance = [](const pb::Node &i, const pb::Node &j) {
        return round(hypot(i.x() - j.x(), i.y() - j.y()));
    };
    double routingCost = 0;
    for (auto pr = periodRoutes.begin(); pr != periodRoutes.end(); ++pr) {
        for (auto vr = pr->vehicleroutes().begin(); vr != pr->vehicleroutes().end(); ++vr) {
            if (vr->deliveries_size() <= 1) { continue; }
            int prevNode = 0;
            for (auto dl = vr->deliveries().begin(); dl != vr->deliveries().end(); ++dl) {
                routingCost += distance(nodes[prevNode], nodes[dl->node()]);
                prevNode = dl->node();
            }
        }
    }

    static constexpr double ObjScale = 1000;
    double obj = routingCost + holdingCost;
    int returnCode = (error == 0) ? static_cast<int>(obj * ObjScale) : ~error;
    cout << ((error == 0) ? obj : returnCode) << endl;
    return returnCode;
}
