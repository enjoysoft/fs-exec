# Threat model and safety policy

## Intended use

`fs-exec` is an authorized relay between identities and machines that already share a filesystem. It is **not** a firewall bypass, authentication bypass, remote administration boundary, sandbox escape mechanism, or replacement for SSH/MFA. Do not mount the relay share into an untrusted sandbox and assume a manifest policy makes that sandbox trusted.

## Protected assets and attackers

Assets are the target account's files/processes, command input/output, artifacts, and availability. Consider an accidental client, a compromised client with share write access, a malicious command, stale/reordered filesystem observations, and watcher crashes. OS account compromise, malicious target administrators, filesystem-server compromise, kernel escape, and perfect output secret detection are out of scope.

## Controls

* Per-target OS ACL and least-privilege service account; no listener or discovery routing.
* Executable/runtime allowlists, denied `sudo`/`su`/`doas` and nested `fs-exec`, cwd roots, environment allowlist, queue expiry, and runtime/output/upload/artifact limits.
* The watcher does not inherit arbitrary service environment. It passes only launch essentials plus explicitly allowlisted request values, and rejects credential-like variable names. Target-local tools should obtain credentials from a dedicated least-privilege local mechanism.
* Exact argv with `shell=False`; Bash/PowerShell only through explicit shell mode.
* Staged publication, immutable commit/final markers, checksums, exclusive claims, and no replay after ambiguous start.
* Optional owner-controlled `approval_hook` receives verified manifest JSON on stdin and must exit zero. A production hook can reject shell mode, non-read-only database flags, or commands outside a change window.
* `read_only = true` sets `FS_EXEC_READ_ONLY=1` for cooperating programs. **It is a profile signal, not write confinement.** Enforce production read-only with OS permissions, read-only DB roles, filesystem mounts, containers, seccomp/AppLocker/WDAC, and the approval hook.

An allowlisted executable may itself launch another process or interpret code (for example Python, a DB client `\copy`, or `bash`). Do not allow such tools in a high-trust profile unless their full capability is intended. “No sudo” means the relay does not invoke or permit named privilege tools; it cannot constrain a service account that already has excessive rights.

## Residual risks

* Anyone able to create valid files in a target inbox can request its allowlisted capabilities. Checksums are integrity framing, not signatures.
* A process killed at timeout may already have changed state. Process descendants that detach from their process group/job tree may survive.
* Shell scripts are difficult to classify as read-only. Disable shell runtimes in production where possible.
* Stdout/stderr may contain secrets. The relay prevents inherited credential export but does not heuristically rewrite output; protect and expire the share.
* NFS root squashing, UID mapping, SMB ACL translation, antivirus, backup/indexing software, and client caches can change practical behavior. Test the deployment.
* Disk-full and share-loss can leave an ambiguous job. Never infer failure from absence and never blindly resubmit.
