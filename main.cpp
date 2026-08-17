/**
 * @file main.cpp
 * @brief Primary entry point for the scheduling orchestration engine.
 * @author Authored by: opt1mal
 */

#include <iostream>
#include <vector>
#include <string>
#include <charconv>

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
    // Fix 6: Pre-reserve request storage to prevent repeated resize reallocs
    state.all_request.reserve(100000);

    AdvancedScheduler scheduler;

    // Fix 4: Single reusable output buffer — zero allocations in the hot loop
    std::string out_buf;
    out_buf.reserve(4096);

    while (true) {
        bool game_running = parser.readNextFrame(state);
        
        if (!game_running) {
            break; 
        }

        out_buf.clear();
        int assignment_count = 0;
        scheduler.scheduleTasks(state, parser, out_buf, assignment_count);

        // Single write: count line + all assignment lines already in out_buf
        char count_buf[16];
        auto r = std::to_chars(count_buf, count_buf + sizeof(count_buf), assignment_count);
        std::cout.write(count_buf, r.ptr - count_buf);
        std::cout.put('\n');
        if (assignment_count > 0) {
            std::cout.write(out_buf.data(), out_buf.size());
        }
        std::cout.flush();
    }

    return 0;
}
