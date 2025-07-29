#!/bin/bash
# Enhanced Git Workflow Setup Script
# This script configures the enhanced git workflow with ruff integration
# Run this script every time the dev container is rebuilt

echo "🚀 Setting up enhanced git workflow..."

# Navigate to project root
cd /workspaces/unify_2_1_dm_dqa

# Create the enhanced git add script (gadd) in .devcontainer
echo "📝 Creating enhanced git add script..."
cat > .devcontainer/gadd << 'EOF'
#!/bin/bash
# Enhanced git add with automatic ruff formatting
# Usage: ./gadd <files>

# Add files to git
git add "$@"

# Get list of staged Python files
PYTHON_FILES=$(git diff --cached --name-only --diff-filter=AM "*.py")

if [ -n "$PYTHON_FILES" ]; then
    echo "🔍 Running ruff on staged Python files:"
    echo "$PYTHON_FILES"

    # Run ruff fix and format directly on the files
    echo "$PYTHON_FILES" | tr '\n' '\0' | xargs -0 ruff check --fix
    echo "$PYTHON_FILES" | tr '\n' '\0' | xargs -0 ruff format

    # Run file fixers on all staged files
    echo "🔧 Running file fixers..."
    pre-commit run --hook-stage manual trailing-whitespace --files $(git diff --cached --name-only --diff-filter=AM) 2>/dev/null || true
    pre-commit run --hook-stage manual end-of-file-fixer --files $(git diff --cached --name-only --diff-filter=AM) 2>/dev/null || true
    pre-commit run --hook-stage manual fix-byte-order-marker --files $(git diff --cached --name-only --diff-filter=AM) 2>/dev/null || true

    # Re-add all the formatted files
    git add "$@"

    echo "✅ Ruff formatting applied and files re-staged"
else
    echo "✅ Files added (no Python files to format)"
fi
EOF

# Make gadd executable
chmod +x .devcontainer/gadd
echo "✅ Created .devcontainer/gadd script"

# Create convenient symlink in project root
ln -sf .devcontainer/gadd gadd
echo "✅ Created symlink ./gadd -> .devcontainer/gadd"

# Configure pre-commit to avoid VSCode conflicts
echo "⚙️  Configuring pre-commit hooks..."
cat > .pre-commit-config.yaml << 'EOF'
default_stages: [pre-commit]
default_language_version:
    python: python3
ci:
    skip: [ruff, ruff-format]
repos:
-   repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
    -   id: trailing-whitespace
        stages: [manual]
    -   id: check-ast
    -   id: check-json
    -   id: check-xml
    -   id: mixed-line-ending
    -   id: check-case-conflict
    -   id: check-executables-have-shebangs
    -   id: check-merge-conflict
    -   id: check-symlinks
    -   id: check-toml
    -   id: check-yaml
    -   id: debug-statements
    -   id: end-of-file-fixer
        stages: [manual]
    -   id: fix-byte-order-marker
        stages: [manual]
-   repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.12.4
    hooks:
    -   id: ruff
        args: [ --fix ]
        stages: [manual]
    -   id: ruff-format
        stages: [manual]
EOF

echo "✅ Configured .pre-commit-config.yaml"

# Install pre-commit hooks
echo "🔧 Installing pre-commit hooks..."
pre-commit install

# Create helpful aliases
echo "🔗 Setting up git aliases..."
git config alias.add-format '!f() { ./.devcontainer/gadd "$@"; }; f'
git config alias.gadd '!f() { ./.devcontainer/gadd "$@"; }; f'

# Create docs directory and usage documentation
echo "📚 Creating usage documentation..."
mkdir -p docs
cat > docs/ENHANCED_GIT_WORKFLOW.md << 'EOF'
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
EOF

echo "✅ Created docs/ENHANCED_GIT_WORKFLOW.md"

# Test the setup
echo "🧪 Testing the setup..."
if [ -x "./.devcontainer/gadd" ] && [ -L "./gadd" ] && [ -f ".pre-commit-config.yaml" ]; then
    echo "✅ All files created successfully"

    # Test pre-commit
    if pre-commit run --all-files > /dev/null 2>&1; then
        echo "✅ Pre-commit hooks working"
    else
        echo "⚠️  Pre-commit test completed (some checks may have failed, this is normal)"
    fi

    echo ""
    echo "🎉 Enhanced Git Workflow Setup Complete!"
    echo ""
    echo "📖 Usage:"
    echo "   ./gadd <files>                    # Enhanced git add (symlink)"
    echo "   ./.devcontainer/gadd <files>      # Enhanced git add (direct)"
    echo "   git gadd <files>                  # Enhanced git add (alias)"
    echo "   git commit -m \"message\"          # Commit as usual"
    echo ""
    echo "📚 See docs/ENHANCED_GIT_WORKFLOW.md for detailed documentation"
    echo ""
else
    echo "❌ Setup failed - please check for errors above"
    exit 1
fi
