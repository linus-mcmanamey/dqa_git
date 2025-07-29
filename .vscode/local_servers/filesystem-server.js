#!/usr/bin/env node
const { spawn } = require('child_process');
const args = ['@modelcontextprotocol/server-filesystem'].concat(process.argv.slice(2));
const server = spawn('npx', args, {
  stdio: 'inherit',
  env: { ...process.env }
});
server.on('exit', (code) => process.exit(code));
