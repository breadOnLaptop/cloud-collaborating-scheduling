#!/usr/bin/env python3
import os

files_in_order = [
    "entity/request.h",
    "entity/server.h",
    "state/system_state.h",
    "io/event_parser.h",
    "scheduler/chunker.h",
    "scheduler/batcher.h",
    "scheduler/advanced_scheduler.h",
    "main.cpp"
]

output_file = "build/upload.cpp"
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

system_includes = set()
bundled_code = []

for file_path in files_in_order:
    full_path = os.path.join(project_root, file_path)
    if not os.path.exists(full_path):
        continue

    with open(full_path, 'r') as f:
        for line in f:
            if line.startswith("#pragma once"):
                continue
            if line.startswith("#include <"):
                system_includes.add(line.strip())
                continue
            if line.startswith('#include "') or line.startswith('#include "../'):
                continue
            
            bundled_code.append(line)
        bundled_code.append("\n")

out_path = os.path.join(project_root, output_file)
os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, 'w') as out:
    out.write("// Authored by: opt1mal\n")
    out.write("// Full Implementation: https://github.com/breadOnLaptop/cloud-collaborating-scheduling\n")
    out.write("// NOTE: Refer to Link after contest ends\n\n")
    for inc in sorted(list(system_includes)):
        out.write(inc + "\n")
    out.write("\n")
    
    for line in bundled_code:
        out.write(line)
