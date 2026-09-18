# Run as a distinct least-privilege fs-rally account, never as the watcher/client.
# Use UNC share paths in rally.toml, not session-local mapped drives.
# For startup, use Task Scheduler or a reviewed WinSW/NSSM service wrapper.
# Plain python.exe is NOT a Windows SCM service executable.
$ErrorActionPreference = 'Stop'
$Python = 'C:\Program Files\fs-exec\venv\Scripts\python.exe'
$Config = 'C:\ProgramData\fs-exec\rally.toml'
& $Python -m fs_exec.rally --config $Config run
exit $LASTEXITCODE
