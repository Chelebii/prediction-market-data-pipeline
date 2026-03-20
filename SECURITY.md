# Security Policy

## Supported Scope

This repository is a research and data-pipeline project. Security-sensitive areas include:

- secrets and local `.env` handling
- Git ignore boundaries for runtime artifacts
- scheduler and startup scripts
- process supervision and lock handling
- dataset integrity and backup behavior

## Reporting a Vulnerability

Please do **not** open a public GitHub issue for suspected secrets exposure, credential leaks, or vulnerabilities that could affect a live collection machine.

Instead, report privately to the repository owner through GitHub security reporting or direct contact if one is available on the profile.

When reporting, include:

- a clear summary of the issue
- affected files or paths
- impact
- reproduction steps if safe to share
- whether any secrets or live machine details may be exposed

## Secrets Handling

Never commit:

- real `.env` files
- API keys or bot tokens
- certificates or private keys
- live runtime databases
- backup artifacts
- lock files or logs from a live system

If a secret is ever committed or shared accidentally:

1. rotate it immediately
2. remove it from tracked files
3. review recent history and downstream copies

## Operational Security Notes

- prefer repo-relative runtime paths over personal absolute paths
- do not weaken health checks, backup validation, or single-instance protections without a strong reason
- treat Windows Task Scheduler and Startup changes as security-relevant operational changes

## Safe Public Contributions

Public contributions should avoid:

- publishing machine-specific usernames or personal filesystem paths
- shipping default credentials
- relaxing ignore rules around runtime artifacts
- reducing logging or monitoring in ways that hide failures

## Disclosure Expectations

After a valid report is received, the goal is to confirm scope, contain obvious risk, and ship a fix or mitigation before broad public discussion where possible.
