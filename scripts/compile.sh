#!/bin/bash

cd "$(dirname "$0")/.." || exit

python3 scripts/bundle.py

if [ ! -f "build/upload.cpp" ]; then
    exit 1
fi

g++ -O3 -std=c++17 build/upload.cpp -o build/solution
