#include <iostream>
#include <vector>
#include <string>

#include "state/system_state.h"
#include "io/event_parser.h"
#include "scheduler/baseline_scheduler.h"

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    EventParser parser;
    
    parser.readStartupConfig();
    parser.readTaskTimeTable();

    SystemState state(parser.params.K);

    BaselineScheduler scheduler;

    while (true) {
        bool game_running = parser.readNextFrame(state);
        
        if (!game_running) {
            break; 
        }

        std::vector<std::string> assignments = scheduler.scheduleTasks(state, parser.params);

        std::cout << assignments.size() << "\n";
        for (const std::string& assignment : assignments) {
            std::cout << assignment << "\n";
        }
        
        std::cout << std::flush;
    }

    return 0;
}
