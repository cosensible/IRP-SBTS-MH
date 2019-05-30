////////////////////////////////
/// usage : 1.	read csv file into an jagged array.
/// 
/// note  : 1.	
////////////////////////////////

#ifndef SMART_SZX_INVENTORY_ROUTING_CSV_READER_H
#define SMART_SZX_INVENTORY_ROUTING_CSV_READER_H


#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <set>
#include <vector>


namespace szx {

class CsvReader {
public:
    using Row = std::vector<char*>;


    const std::vector<Row>& scan(std::ostringstream &oss) {
        oss << '\n'; // add a sentinel so that end of file check is only necessary in onNewLine().
        data = oss.str();
        cur = const_cast<char*>(data.data());  // the const cast can be omitted since C++17.
        end = cur + data.size();

        //onNewLine();
        onNewLine_opt();

        return rows;
    }
    const std::vector<Row>& scan(std::ifstream &ifs) {
        std::ostringstream oss;
        oss << ifs.rdbuf();
        return scan(oss);
    }

protected:
    void onNewLine() {
        while ((cur != end) && (NewLineChars.find(*cur) != NewLineChars.end())) { ++cur; } // remove empty lines.
        if (cur == end) { return; }

        rows.push_back(Row());

        onSpace();
    }

    void onSpace() {
        while (SpaceChars.find(*cur) != SpaceChars.end()) { ++cur; } // trim spaces.

        onValue();
    }

    void onValue() {
        rows.back().push_back(cur);

        char c = *cur;
        if (EndCellChars.find(c) == EndCellChars.end()) {
            while (EndCellChars.find(*(++cur)) == EndCellChars.end()) {}
            c = *cur;

            char *space = cur;
            while (SpaceChars.find(*(space - 1)) != SpaceChars.end()) { --space; }
            *space = 0; // trim spaces and remove comma or line ending.
        } else { // empty cell.
            *cur = 0;
        }

        ++cur;
        (NewLineChars.find(c) != NewLineChars.end()) ? onNewLine() : onSpace();
    }

    // in case there is no Tail-Call Optimization which leads to the stack overflow.
    void onNewLine_opt() {
    Label_OnNewLine:
        while ((cur != end) && (NewLineChars.find(*cur) != NewLineChars.end())) { ++cur; } // remove empty lines.
        if (cur == end) { return; }

        rows.push_back(Row());
        //goto Label_OnSpace;

    Label_OnSpace:
        while (SpaceChars.find(*cur) != SpaceChars.end()) { ++cur; } // trim spaces.
        //goto Label_OnValue;

    //Label_OnValue:
        rows.back().push_back(cur);

        char c = *cur;
        if (EndCellChars.find(c) == EndCellChars.end()) {
            while (EndCellChars.find(*(++cur)) == EndCellChars.end()) {}
            c = *cur;

            char *space = cur;
            while (SpaceChars.find(*(space - 1)) != SpaceChars.end()) { --space; }
            *space = 0; // trim spaces and remove comma or line ending.
        } else { // empty cell.
            *cur = 0;
        }

        ++cur;
        if (NewLineChars.find(c) != NewLineChars.end()) {
            goto Label_OnNewLine;
        } else {
            goto Label_OnSpace;
        }
    }

    // TODO[szx][2]: handle quote (comma will not end cell).
    // EXTEND[szx][5]: make trim space configurable.

    static const std::set<char> NewLineChars;
    static const std::set<char> SpaceChars;
    static const std::set<char> EndCellChars;

    char *cur; // the cursor and current char.
    const char *end;

    std::string data;
    std::vector<Row> rows;
};

}


#endif // SMART_SZX_INVENTORY_ROUTING_CSV_READER_H
