# Threat model and safety policy

## Intended use

`fs-exec` is an authorized relay between identities and machines that already share a filesystem. It is **not** a firewall bypass, authentication bypass, remote administration boundary, sandbox escape mechanism, or replacement for SSH/MFA. Do not mount the relay share into an untrusted sandbox and assume a manifest policy makes that sandbox trusted.

## Protected assets and attackers

Assets are the target account's files/processes, command input/output, artifacts, and availability. Consider an accidental client, a compromised client with share write access, a malicious command, stale/reordered filesystem observations, and watcher crashes. OS account compromise, malicious target administrators, filesystem-server compromise, kernel escape, and perfect output secret detection are out of scope.

## Controls

* Directional per-target OS ACLs: clients write incoming jobs/cancel/pings only, never claims/results/heartbeat/lock or their structural parents. Clients do not provision the namespace. See the tested two-UID POSIX ACL recipe in [operations](operations.md); same-subtree client Modify access is unsafe.
* Canonical absolute owner-configured executables (optional SHA-256 pins), runtime allowlists, cwd roots, deny-by-default environment, bounded preparation age, and runtime/output/upload/artifact limits. Empty allowlists deny access. A basename cannot redirect execution to a client-selected path.
* The watcher does not inherit arbitrary service environment. Loader, startup-file, interpreter-path and relay-control variables are denied even when allowlisted; credential-like names are denied. This is defense in depth, not an exhaustive classification of every program's environment. Target-local tools should obtain credentials from a dedicated least-privilege mechanism. Owner-controlled binary directories, runtime configuration and cwd remain required.
* Exact argv with `shell=False`; Bash/PowerShell only through explicit shell mode.
* Staged no-replace publication, FINAL-selected immutable result generations, bounded hash verification, exclusive claims, and no automatic replay of any pre-existing claim. File contents and parent directories are flushed where supported; see the explicit durability/fencing assumptions in [architecture](architecture.md).
* Regular-file input checks reject symlinks/reparse points, Windows device names and ADS-style path components. POSIX reads pin parent directories using descriptor-relative O_NOFOLLOW traversal; bounded copies use exclusive files. Artifact downloads verify before installing without replacing an existing destination. Windows checks are best effort and are not equivalent to race-proof handle traversal. These checks do not turn arbitrary execution/cwd into a filesystem sandbox.
* Optional owner-controlled `approval_hook` receives verified manifest JSON on stdin and must exit zero. A production hook can reject shell mode, non-read-only database flags, or commands outside a change window.
* `read_only = true` sets `FS_EXEC_READ_ONLY=1` for cooperating programs. **It is a profile signal, not write confinement.** Enforce production read-only with OS permissions, read-only DB roles, filesystem mounts, containers, seccomp/AppLocker/WDAC, and the approval hook.

An allowlisted executable may itself launch another process or interpret code (for example Python, a DB client `\copy`, or `bash`). Do not allow such tools in a high-trust profile unless their full capability is intended. “No sudo” means the relay does not invoke or permit named privilege tools; it cannot constrain a service account that already has excessive rights.

## Residual risks

* Anyone able to create valid files in a target inbox can request its allowlisted capabilities. Checksums are integrity framing, not signatures. If clients can write watcher-owned namespaces or change their ACLs, they can forge results, suppress execution or destroy locks. Do not deploy without writer isolation; signatures/authenticated gateways are not implemented.
* A process killed at timeout may already have changed state. Process descendants that detach from their process group/job tree may survive.
* Shell scripts are difficult to classify as read-only. Disable shell runtimes in production where possible.
* Stdout/stderr may contain secrets. Live output is provisional until compared against FINAL-bound chunk hashes; already-displayed bytes cannot be retracted. The relay does not heuristically rewrite output; protect and expire the share.
* NFS root squashing, UID mapping, SMB ACL translation, antivirus, backup/indexing software, and client caches can change practical behavior. Test the deployment.
* Disk-full and share-loss can leave an ambiguous job. Never infer failure from absence and never blindly resubmit.
* No server-enforced distributed fencing or live lock takeover exists. Cached file checks cannot revoke another host's active process. Retain claims and stop all old writers before manual lock removal. Server rollback, unsupported directory fsync and volatile NAS caches invalidate crash duplicate-suppression guarantees.
* Per-job limits do not bound total queue, inode or probe accumulation. Enforce filesystem quotas and admin retention; the relay has no global quota/retention worker.
* Tests exercise Linux/local-filesystem behavior and two-UID POSIX ACLs. Native Windows, NFS/SMB servers, ACL translation, reparse races and actual power-loss durability have not been integration-tested. Windows taskkill is not a Job Object; detached POSIX descendants may also escape. Use OS containers/cgroups/job containment when that guarantee is required.
