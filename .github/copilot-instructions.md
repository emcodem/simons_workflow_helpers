# FFAStrans Workflow Helpers - AI Assistant Instructions

This codebase contains Python utilities for FFAStrans video processing workflows, focusing on media file management and GPU-accelerated encoding.

## Core Architecture

**FFAStrans Integration**: These tools are designed as external processors for FFAStrans workflows, following the pattern:
- Input via command-line arguments (file paths, configuration)
- Processing with specific return codes (0=success, 1=extraction error, 2=already exists, 3=unexpected error)
- Output to stdout/stderr based on success/failure

**Folder Structure Pattern**: All tools expect a strict 4-level hierarchy: `watchfolder/DEVICE/DATE/CARDNAME/files...`
- DATE format: YYYYMMDD (input) → YYYY_MM_DD (output)
- Example: `C:\watch\CameraA\20251106\CARD123\media\clip001.mov`

## Key Components

### `simons_folderstructure_checker.py`
- **Purpose**: Creates output directories based on input file hierarchy
- **Pattern**: Extracts DEVICE/DATE/CARDNAME from input path, creates reformatted structure
- **Error Handling**: Returns specific codes for missing hierarchy (1), existing dirs (2), other errors (3)
- **Testing**: Uses `tempfile.TemporaryDirectory()` for isolated filesystem tests

### `findfiles.py`
- **Purpose**: Recursive file discovery with pattern-based filtering
- **Pattern**: Separate include/exclude filters for files vs folders using `fnmatch`
- **Usage**: Both library (`list_files()`) and CLI with comma-separated patterns
- **Key Feature**: Case-insensitive matching, handles single files or directory trees

### `gpu_encoding_cmd.py`
- **Purpose**: FFmpeg command transformation for GPU acceleration
- **Pattern**: Read command from file → apply regex rules → show diff → execute
- **Transformation Rules**: `-c:v libx264` → `-c:v h264_nvenc`, `-g <number>` → `-g 50`
- **Debugging**: Color-coded word-level diffs using `difflib.Differ()`

## Development Patterns

**Error Handling**: Use tuple returns `(code, message)` for CLI tools, exceptions for library functions
**Testing**: 
- Run with `pytest -v` for comprehensive output
- Use `tempfile` for filesystem isolation
- Test both success and error cases with specific return codes
**CLI Design**: All tools use `argparse` with `--input`, `--out_root`, `--recursed` pattern

## Debugging Setup

VS Code launch configurations in `.vscode/launch.json`:
- `gpu_encoding_cmd`: Debug with sample FFAStrans command file
- `simons_folderstructure_checker_test`: Run test suite directly

**Important**: When working with paths, always use `os.path.normpath()` and `os.path.abspath()` for Windows compatibility.

## Integration Points

- **FFAStrans Workflow**: Tools expect to be called from FFAStrans processors with specific argument patterns
- **File System**: All operations assume Windows-style paths and NTFS filesystem
- **External Dependencies**: FFmpeg for video processing, subprocess for command execution