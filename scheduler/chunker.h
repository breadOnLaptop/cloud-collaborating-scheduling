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
        int chunk_size = params.num_layers; // Default to full processing to save setup penalty S
        
        if (decode_queue_size > 32) {
            chunk_size = 2; // Severe congestion: yield almost instantly
        } else if (decode_queue_size > 0) {
            chunk_size = 4; // Moderate congestion: small chunks to let decodes interleave
        } else {
            chunk_size = 16; // No decodes: large burst to maximize throughput
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
