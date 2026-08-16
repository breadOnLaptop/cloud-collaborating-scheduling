#pragma once

#include <algorithm>
#include "../io/event_parser.h"

class Chunker {
public:
    static int getNextChunkEnd(int ls, const SystemParams& params) {
        int chunk_size = 4; 
        
        int remaining = params.num_layers - ls;
        
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
