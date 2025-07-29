# Enhanced Git Workflow

This project uses an enhanced git workflow with automatic code formatting.

## Quick Start

Instead of `git add`, use any of these methods:

**Option 1: Symlink (recommended)**
```bash
./gadd <files>
```

**Option 2: Direct path**
```bash
./.devcontainer/gadd <files>
```

**Option 3: Git alias**
```bash
git gadd <files>
git add-format <files>
```

This automatically:
- ✅ Stages your files
- ✅ Runs ruff to fix code issues
- ✅ Formats code with ruff
- ✅ Fixes trailing whitespace
- ✅ Adds end-of-file newlines
- ✅ Re-stages the cleaned files

## Examples

```bash
# Add and format a single file (multiple ways)
./gadd main.py
./.devcontainer/gadd main.py
git gadd main.py

# Add and format multiple files
./gadd src/*.py

# Add everything (only formats Python files)
./gadd .
```

## Manual Formatting

Run manual formatting tools:
```bash
# Run all manual hooks
pre-commit run --hook-stage manual --all-files

# Run specific hooks
pre-commit run --hook-stage manual ruff --all-files
pre-commit run --hook-stage manual ruff-format --all-files
```

## Regular Commit Workflow

1. Make your changes
2. Use any of these instead of `git add`:
   - `./gadd <files>` (symlink)
   - `./.devcontainer/gadd <files>` (direct)
   - `git gadd <files>` (alias)
3. Commit as normal: `git commit -m "your message"`
4. Push: `git push`

## Benefits

- 🚫 No more pre-commit conflicts with VSCode
- 🎯 Automatic code formatting on add
- 🧹 Consistent code style
- ⚡ Fast and reliable workflow
