#!/bin/bash
set -e


curl -sS https://raw.githubusercontent.com/diogocavilha/fancy-git/master/install.sh | sh
cp /tmp/app_config ~/.fancy-git/app_config
git config --global credential.helper store
git config --global credential.interactive auto
git config --global credential.useHttpPath true
git config --global user.email $USERNAME@police.tas.gov.au
git config --global user.name $USERNAME
git config --global --add safe.directory $(pwd)
git config --global http.proxy http://proxy.police.tas.gov.au:8080
git config --global https.proxy http://proxy.police.tas.gov.au:8080
git config --global http.noProxy localhost,127.0.0.1,.local  # Added line to bypass proxy for local addresses

# echo "-----------------------START load repos"
# cd /workspaces
# git clone https://emstas@dev.azure.com/emstas/Program%20Unify/_git/unify_2_1_dm_niche_rms_build_d9
# git clone https://emstas@dev.azure.com/emstas/Program%20Unify/_git/unify_2_1_dm_dqa
# cd $(pwd)
# ln -sf /workspaces/unify_2_1_dm_niche_rms_build_d9 ./unify_2_1_dm_niche_rms_build_d9
# ln -sf /workspaces/unify_2_1_dm_dqa ./unify_2_1_dm_dqa
# echo "-----------------------END load repos"


SSH_CONFIG="$HOME/.ssh/config"
HOST_TO_CHECK="ssh.dev.azure.com"
echo "-----------------------START write_ssh_config"
# Function to write SSH config
write_ssh_config() {
    echo "Host $HOST_TO_CHECK" >> "$SSH_CONFIG"
    echo "    IdentityFile ~/.ssh/id_rsa" >> "$SSH_CONFIG"
    echo "    IdentitiesOnly yes" >> "$SSH_CONFIG"
    echo "    HostkeyAlgorithms +ssh-rsa" >> "$SSH_CONFIG"
    echo "    PubkeyAcceptedKeyTypes=ssh-rsa" >> "$SSH_CONFIG"
    echo "    ProxyCommand corkscrew inthaproxy.ems.tas.gov.au 8080 %h %p" >> "$SSH_CONFIG"
}

# Create SSH config directory if it doesn't exist
#mkdir -p "$HOME/.ssh"

# If config file doesn't exist, create it and write config
if [ ! -e "$SSH_CONFIG" ]; then
    write_ssh_config
else
    # Check if Host entry exists
    if ! grep -q "^Host $HOST_TO_CHECK\$" "$SSH_CONFIG"; then
        write_ssh_config
    fi
fi
# Set appropriate permissions
#chmod 700 "$SSH_CONFIG"
echo "-----------------------END write_ssh_config"

# Add SSH agent management with session persistence to avoid repeated authentication
cat >> ~/.bashrc << 'EOF'

# SSH Agent management - only start if not already running
if [ -z "$SSH_AUTH_SOCK" ] || [ ! -S "$SSH_AUTH_SOCK" ]; then
    # Check if there's already an agent running
    if [ -f ~/.ssh/agent-environment ]; then
        source ~/.ssh/agent-environment > /dev/null
    fi

    # Test if the agent is still valid
    if ! ssh-add -l > /dev/null 2>&1; then
        # Start new agent and save environment
        eval "$(ssh-agent -s)" > /dev/null
        echo "export SSH_AUTH_SOCK=$SSH_AUTH_SOCK" > ~/.ssh/agent-environment
        echo "export SSH_AGENT_PID=$SSH_AGENT_PID" >> ~/.ssh/agent-environment

        # Only add key if it exists and is not already loaded
        if [ -f ~/.ssh/id_rsa ] && ! ssh-add -l | grep -q ~/.ssh/id_rsa; then
            ssh-add ~/.ssh/id_rsa 2>/dev/null || true
        fi
    fi
fi
EOF

# Add useful aliases
echo "alias ll='ls -l'" >> ~/.bashrc
echo "alias la='ls -A'" >> ~/.bashrc
echo "alias l='ls -CF'" >> ~/.bashrc
echo "alias python=python3" >> ~/.bashrc
echo "alias pip=pip3" >> ~/.bashrc

# Add MCP GitHub integration to bashrc
cat >> ~/.bashrc << 'EOF'

# MCP GitHub Integration
export MCP_GITHUB_ENABLED=true
export MCP_GITHUB_ORG="emstas"
export MCP_GITHUB_PROJECT="Program Unify"
export MCP_ADO_ENABLED=true
export MCP_STATUS="active"

# Custom function to show MCP status in prompt
show_mcp_status() {
    if [ "$MCP_GITHUB_ENABLED" = "true" ]; then
        echo " [MCP:GitHub+ADO]"
    fi
}

# MCP-powered git commit message generator
generate_mcp_commit_message() {
    # Get git diff for context
    local git_diff=$(git diff --cached --name-only 2>/dev/null)
    local git_status=$(git status --porcelain 2>/dev/null)

    if [ -z "$git_diff" ] && [ -z "$git_status" ]; then
        echo "feat: update project files"
        return
    fi

    # Simple commit message generation based on changed files
    local message=""
    if echo "$git_diff $git_status" | grep -q "\.py$"; then
        message="feat: update Python implementation"
    elif echo "$git_diff $git_status" | grep -q "\.yml$\|\.yaml$"; then
        message="ci: update pipeline configuration"
    elif echo "$git_diff $git_status" | grep -q "\.md$"; then
        message="docs: update documentation"
    elif echo "$git_diff $git_status" | grep -q "\.json$"; then
        message="config: update configuration files"
    elif echo "$git_diff $git_status" | grep -q "test\|spec"; then
        message="test: update test implementation"
    else
        message="feat: update project files"
    fi

    echo "$message"
}

# Override fancy-git commit commands with MCP integration
alias gcmm='git add . && git commit -m "$(generate_mcp_commit_message)"'
alias gg='git add . && git commit -m "$(generate_mcp_commit_message)" && git push'

# Add MCP info to existing fancy-git prompt
if command -v fancy-git >/dev/null 2>&1; then
    # Override PS1 to include MCP status
    export PS1_ADDITION="$(show_mcp_status)"
fi
EOF

# Create SSH directory if needed
mkdir -p ~/.ssh
chmod 700 ~/.ssh

echo "-----------------------START add_requirements_to_uv"
# Ensure we're in the correct directory and have a uv project
cd $(pwd)
if [ ! -f "pyproject.toml" ]; then
    echo "No pyproject.toml found. Skipping uv package installation."
    echo "Installing packages with pip instead..."
    pip install -r ./.devcontainer/requirements.txt
else
    chmod +x ./.devcontainer/add_requirements_to_uv.sh
    ./.devcontainer/add_requirements_to_uv.sh ./.devcontainer/requirements.txt
fi
echo "-----------------------END add_requirements_to_uv"
echo "-----------------------START enhanced git workflow setup"
if [ -f ".devcontainer/setup_enhanced_git.sh" ]; then
    echo "Running enhanced git workflow setup..."
    ./.devcontainer/setup_enhanced_git.sh
else
    echo "Enhanced git setup script not found, running standard pre-commit setup..."
    pre-commit install
    pre-commit run --all-files
fi

echo "-----------------------END enhanced git workflow setup"

# Initialize Context7 for better code understanding
echo "-----------------------START setup context7"
if command -v npx > /dev/null 2>&1; then
    echo "Setting up Context7 project indexing..."
    cd $(pwd)

    # Create a simple project summary for Context7
    echo "Initializing Context7 with project context..."

    # Set environment variables for Context7
    export CONTEXT7_PROJECT_PATH="$(pwd)"
    export CONTEXT7_CONFIG_FILE="$(pwd)/.vscode/.context7.json"

    echo "Context7 environment configured"
else
    echo "npm/npx not available, skipping Context7 setup"
fi
echo "-----------------------END setup context7"

# MCP Local Development Setup
echo "-----------------------START MCP Local Setup"
# Install Node.js and npm if not present
if ! command -v npm &> /dev/null; then
    echo "Installing Node.js and npm..."
    curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
    apt-get install -y nodejs
fi

# Set up MCP servers locally
cd $(pwd)/.vscode
if [ ! -d "local_servers" ]; then
    echo "Setting up local MCP servers..."

    # Install MCP servers globally
    npm install -g \
        @modelcontextprotocol/server-sequential-thinking \
        @wonderwhy-er/desktop-commander \
        @modelcontextprotocol/server-memory \
        @modelcontextprotocol/server-filesystem \
        @upstash/context7-mcp \
        @azure-devops/mcp \
        @azure/mcp || echo "Some MCP packages may have failed to install"

    # Create local servers directory and scripts
    mkdir -p local_servers

    # Create wrapper scripts for each server
    cat > local_servers/ado-server.js << 'EOF'
#!/usr/bin/env node
const { spawn } = require('child_process');
const server = spawn('npx', ['@azure-devops/mcp', 'emstas'], {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
EOF

    cat > local_servers/sequential-thinking-server.js << 'EOF'
#!/usr/bin/env node
const { spawn } = require('child_process');
const server = spawn('npx', ['@modelcontextprotocol/server-sequential-thinking'], {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
EOF

    cat > local_servers/memory-server.js << 'EOF'
#!/usr/bin/env node
const { spawn } = require('child_process');
const server = spawn('npx', ['@modelcontextprotocol/server-memory'], {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
EOF

    cat > local_servers/filesystem-server.js << 'EOF'
#!/usr/bin/env node
const { spawn } = require('child_process');
const args = ['@modelcontextprotocol/server-filesystem'].concat(process.argv.slice(2));
const server = spawn('npx', args, {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
EOF

    cat > local_servers/context7-server.js << 'EOF'
#!/usr/bin/env node
const { spawn } = require('child_process');
const server = spawn('npx', ['@upstash/context7-mcp'], {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
EOF

    cat > local_servers/desktop-commander-server.js << 'EOF'
#!/usr/bin/env node
const { spawn } = require('child_process');
const server = spawn('npx', ['@wonderwhy-er/desktop-commander'], {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
EOF

    cat > local_servers/azure-server.js << 'EOF'
#!/usr/bin/env node
const { spawn } = require('child_process');
const server = spawn('npx', ['@azure/mcp', 'server', 'start'], {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
EOF

    # Make scripts executable
    chmod +x local_servers/*.js

    echo "Local MCP server scripts created successfully!"
else
    echo "Local MCP servers already configured"
fi

cd $(pwd)
echo "-----------------------END MCP Local Setup"

# Initialize Kerberos authentication if needed
if command -v kinit > /dev/null 2>&1; then
    echo "Initializing Kerberos authentication..."
    kinit "${USERNAME}@POLICE.TAS.GOV.AU" || echo "Kerberos initialization failed or not required"
else
    echo "kinit command not found, skipping Kerberos authentication"
fi

source ~/.bashrc
claude
