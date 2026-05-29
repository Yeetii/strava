#!/usr/bin/env node

import { promises as fs } from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const brunoRoot = path.join(repoRoot, 'bruno');
const outputFile = path.join(repoRoot, 'API', 'openapi', 'openapi.source.json');
const sourceUrl = process.env.OPENAPI_SOURCE_URL || 'http://localhost:7075/api/openapi/v3.json';

async function listBruFiles(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listBruFiles(fullPath)));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith('.bru')) {
      files.push(fullPath);
    }
  }

  return files;
}

function normalizePath(rawUrl) {
  let value = rawUrl.trim();

  value = value.replace(/^\{\{baseUrl\}\}/, '');

  if (value.startsWith('http://') || value.startsWith('https://')) {
    const parsed = new URL(value);
    value = parsed.pathname;
  }

  const queryIndex = value.indexOf('?');
  if (queryIndex >= 0) {
    value = value.slice(0, queryIndex);
  }

  if (!value.startsWith('/')) {
    value = `/${value}`;
  }

  if (value === '/api') {
    return '/';
  }

  if (value.startsWith('/api/')) {
    value = value.slice('/api'.length);
  }

  return value
    .replace(/\{\{([^}]+)\}\}/g, '{$1}')
    .replace(/:([a-zA-Z0-9_]+)/g, '{$1}');
}

function isApiFacingBrunoUrl(rawUrl) {
  const value = rawUrl.trim();
  return value.includes('{{baseUrl}}') || value.startsWith('/api/') || value.startsWith('/');
}

function sanitizeOperationId(input) {
  return input
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 120);
}

function inferTag(relativeBruPath) {
  const parts = relativeBruPath.split(path.sep);
  return parts.length > 1 ? parts[0] : 'Bruno';
}

function parseBruRequest(content) {
  const methodMatch = content.match(/^\s*(get|post|put|patch|delete|options|head)\s*\{/im);
  const urlMatch = content.match(/^\s*url:\s*(.+)$/im);
  const nameMatch = content.match(/^\s*name:\s*(.+)$/im);

  if (!methodMatch || !urlMatch) {
    return null;
  }

  return {
    method: methodMatch[1].toLowerCase(),
    rawUrl: urlMatch[1].trim(),
    name: nameMatch ? nameMatch[1].trim() : null
  };
}

async function fetchCodeOpenApi(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch code-generated OpenAPI from ${url}. HTTP ${response.status}`);
  }

  return response.json();
}

function ensureOpenApiDocumentShape(document) {
  if (!document.paths || typeof document.paths !== 'object') {
    document.paths = {};
  }

  if (!document.info || typeof document.info !== 'object') {
    document.info = {
      title: 'Strava API',
      version: '1.0.0'
    };
  }
}

async function main() {
  const codeDocument = await fetchCodeOpenApi(sourceUrl);
  ensureOpenApiDocumentShape(codeDocument);

  codeDocument.info.title = 'Strava API';
  codeDocument.info.description = 'Canonical OpenAPI generated from API code and augmented with Bruno request coverage.';
  codeDocument['x-source-of-truth'] = 'code+bruno';
  codeDocument['x-generated-at'] = new Date().toISOString();

  const bruFiles = await listBruFiles(brunoRoot);
  let addedOperations = 0;
  let linkedOperations = 0;

  for (const filePath of bruFiles) {
    const content = await fs.readFile(filePath, 'utf8');
    const parsed = parseBruRequest(content);
    if (!parsed) {
      continue;
    }

    if (!isApiFacingBrunoUrl(parsed.rawUrl)) {
      continue;
    }

    const normalizedPath = normalizePath(parsed.rawUrl);
    const relativeBruPath = path.relative(brunoRoot, filePath);

    if (!codeDocument.paths[normalizedPath]) {
      codeDocument.paths[normalizedPath] = {};
    }

    const existingOperation = codeDocument.paths[normalizedPath][parsed.method];
    if (existingOperation) {
      existingOperation['x-bruno-request-file'] = relativeBruPath.replace(/\\/g, '/');
      linkedOperations += 1;
      continue;
    }

    const inferredTag = inferTag(relativeBruPath);
    const defaultName = parsed.name || `${parsed.method.toUpperCase()} ${normalizedPath}`;

    codeDocument.paths[normalizedPath][parsed.method] = {
      tags: [inferredTag],
      summary: defaultName,
      operationId: sanitizeOperationId(`${parsed.method}_${normalizedPath}`),
      responses: {
        '200': {
          description: 'Success response'
        }
      },
      'x-source': 'bruno',
      'x-bruno-request-file': relativeBruPath.replace(/\\/g, '/')
    };

    addedOperations += 1;
  }

  const sortedPaths = Object.keys(codeDocument.paths)
    .sort((a, b) => a.localeCompare(b))
    .reduce((acc, key) => {
      acc[key] = codeDocument.paths[key];
      return acc;
    }, {});

  codeDocument.paths = sortedPaths;

  await fs.mkdir(path.dirname(outputFile), { recursive: true });
  await fs.writeFile(outputFile, `${JSON.stringify(codeDocument, null, 2)}\n`, 'utf8');

  console.log(`OpenAPI source written to ${outputFile}`);
  console.log(`Linked existing operations with Bruno files: ${linkedOperations}`);
  console.log(`Added Bruno-only operations: ${addedOperations}`);
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
