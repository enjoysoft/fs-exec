$ErrorActionPreference = 'Stop'
$Venv = 'C:\Program Files\fs-exec\venv'
$Root = '\\fileserver\relay\windows'
$Policy = 'C:\ProgramData\fs-exec\policy.toml'
$Journal = 'C:\ProgramData\fs-exec\journal.jsonl'

# Run this foreground command under a dedicated service account. Configure the
# Windows service wrapper or Task Scheduler to restart it after failure.
& "$Venv\Scripts\fs-exec-watcher.exe" --root $Root --policy $Policy --platform windows --journal $Journal
exit $LASTEXITCODE
