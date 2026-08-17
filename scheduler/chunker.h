/**
 * @file chunker.h
 * @brief Adaptive segmentation logic for heavy computational tasks.
 * @author Authored by: opt1mal
 */

#pragma once

#include <algorithm>
#include "../io/event_parser.h"

class Chunker {
public:
    static int getNextChunkEnd(int ls, const SystemParams& params, size_t decode_queue_size = 0) {
        int chunk_size;
        
        if (decode_queue_size > 0) {
            // Extreme yielding: If ANY decode task is waiting, shrink the chunk size to 2 layers.
            // This prevents P_PROC from blocking the server and causing SLO2 (Token Latency) failures.
            chunk_size = 2;
        } else {
            // Cap burst to 16 to avoid pathological single-task stalls that starve frames
            chunk_size = std::min(params.num_layers, 16);
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers; // Always finish if 1 layer is left to prevent stragglers
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
