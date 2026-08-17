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
        int chunk_size = 4;
        
        // If decodes are waiting, yield IMMEDIATELY (smallest possible chunk)
        if (decode_queue_size > 0) {
            chunk_size = 1;
        } else {
            // Otherwise, burst large chunks to avoid setup penalties
            chunk_size = 16;
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
