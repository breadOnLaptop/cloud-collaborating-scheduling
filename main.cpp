#include <iostream>
#include <vector>
#include <string>

#include "state/system_state.h"
#include "io/event_parser.h"
#include "scheduler/baseline_scheduler.h"

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    // 1. Create our "Ears"
    EventParser parser;

    // Read the initial configuration sent by the interactor
    parser.readStartupConfig();
    parser.readTaskTimeTable();

    // 2. Create our "Memory" (Initialized with K, the number of cloud servers)
    SystemState state(parser.params.K);

    // 3. Create our "Brain"
    BaselineScheduler scheduler;

    // 4. THE HEARTBEAT LOOP
    while (true) {

        // STEP A: Listen to the interactor
        // This will parse the ARR, TDN, and XDN events and instantly update the SystemState queues.
        bool game_running = parser.readNextFrame(state);

        if (!game_running) {
            // The interactor sent "END", the simulation is over.
            break;
        }

        // STEP B: Think
        // Pass the updated queues to the scheduler so it can decide which tasks to assign to FREE servers.
        std::vector<std::string> assignments = scheduler.scheduleTasks(state, parser.params);

        // STEP C: Speak
        // Print the number of assignments, followed by the exact assignment strings.
        std::cout << assignments.size() << "\n";
        for (const std::string& assignment : assignments) {
            std::cout << assignment << "\n";
        }

        std::cout << std::flush;
    }

    return 0;
}
