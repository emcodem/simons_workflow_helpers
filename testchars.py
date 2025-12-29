import json
import os
import builtins
import sys

import builtins, json, sys, io

# # 1. Fix the IO Stream (The Pipe)
# if isinstance(sys.stdout, io.TextIOWrapper):
#     sys.stdout.reconfigure(encoding='utf-8', errors='replace')
# if isinstance(sys.stderr, io.TextIOWrapper):
#     sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# # 2. Fix Object Representation (Affects print, logging, pprint)
# _old_repr = builtins.repr
# def unicode_repr(obj):
#     try:
#         return _old_repr(obj).encode('utf-8').decode('unicode-escape')
#     except:
#         return _old_repr(obj)
# builtins.repr = unicode_repr

# # 3. Fix JSON Serialization (Affects thousands of libraries)
# _orig_dumps = json.dumps
# json.dumps = lambda *a, **k: _orig_dumps(*a, **{**k, 'ensure_ascii': False})
# _orig_dump = json.dump
# json.dump = lambda *a, **k: _orig_dump(*a, **{**k, 'ensure_ascii': False})

# # 3. Inject it into the heart of the interpreter
# builtins.repr = unicode_repr
# The data that usually gets escaped
data = ["ö"]

# 1. Test JSON File Writing
# If the global patch works, this will be raw UTF-8
report_file = "C:\\temp\\test_report.json"
with open(report_file, 'w', encoding='utf-8', ) as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

# 2. Test Standard Output
# This checks if the TTY/Stream redirection is working
print(f"JSON Dumps Output: {json.dumps(data,ensure_ascii=False)}")
print(f"Raw List Output: {data}")

print(f"Check the file: {os.path.abspath(report_file)}")