/**
 * @file main.cpp
 * @brief Primary entry point for the scheduling orchestration engine.
 * @author Authored by: Peeyush Maurya
 */

#include <iostream>
#include <vector>
#include <string>

#include "state/system_state.h"
#include "io/event_parser.h"
#include "scheduler/advanced_scheduler.h"

/**
 * @brief Bootstraps the application and manages the primary event loop.
 * @return Exit status code indicating execution success or failure.
 */
int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    EventParser parser;
    parser.readStartupConfig();
    parser.readTaskTimeTable();

    SystemState state(parser.params.K);
    AdvancedScheduler scheduler;

    while (true) {
        bool game_running = parser.readNextFrame(state);
        
        if (!game_running) {
            break; 
        }

        std::vector<std::string> assignments = scheduler.scheduleTasks(state, parser);

        std::cout << assignments.size() << "\n";
        for (const std::string& assignment : assignments) {
            std::cout << assignment << "\n";
        }
        
        std::cout << std::flush;
    }

    return 0;
}
