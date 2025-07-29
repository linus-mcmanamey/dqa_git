#!/usr/bin/env node
const { spawn } = require('child_process');
const server = spawn('npx', ['@upstash/context7-mcp'], {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
