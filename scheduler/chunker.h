#pragma once

#include <algorithm>
#include "../io/event_parser.h"

class Chunker {
public:
    // Determine the end layer `le` for a P PROC task.
    // We slice the huge prefill computation into smaller chunks. 
    // This frees the Cloud up regularly so it can run fast Decode batches between Prefill chunks.
    static int getNextChunkEnd(int ls, const SystemParams& params) {
        // A standard approach is to process up to 4 layers at a time.
        // In advanced versions, this can be mathematically calculated using the Task-Time table.
        int chunk_size = 4; 
        
        int remaining = params.num_layers - ls;
        
        // If there are only a few layers left, just finish the job to avoid another setup penalty (S)
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
