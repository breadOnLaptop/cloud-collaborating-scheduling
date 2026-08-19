/**
 * @file chunker.h
 * @brief Adaptive segmentation logic for prefill layer processing.
 * @author Authored by: opt1mal
 */

#pragma once

#include <algorithm>
#include "../io/event_parser.h"

/**
 * @class Chunker
 * @brief Determines how many layers to process per P_PROC call.
 *
 * chunk=8 under decode pressure balances setup overhead vs responsiveness.
 * Larger chunks (8) give faster overall prefill → more requests reach decode
 * sooner → larger natural batches → better amortization.
 */
class Chunker {
public:
    static int getNextChunkEnd(int ls, const SystemParams& params, size_t decode_queue_size = 0) {
        int chunk_size;
        
        if (decode_queue_size > 0) {
            chunk_size = std::min(params.num_layers, 8);
        } else {
            chunk_size = params.num_layers;
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
