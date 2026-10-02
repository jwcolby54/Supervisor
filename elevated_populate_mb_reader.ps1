# Elevated: copy the interactive user's MusicBrainz reader credential into
# LocalSystem's Windows Credential Manager entry used by the supervised MCP.
# The bridge exists only for the few seconds needed by the SYSTEM task.
$py = "C:\Users\jwcol\AppData\Local\Programs\Python\Python310\python.exe"
$wiki = "D:\MusicBrainz\musicbrainz-wiki"
$dir = "E:\DevPython\DataSourceQueue\Supervisor"
$bridge = "$dir\.mb_reader_bridge.json"
$log = "$dir\logs\populate_mb_reader.log"
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $log) | Out-Null
"$(Get-Date -Format o) START" | Set-Content -LiteralPath $log

try {
    & $py "$wiki\tools\mirror_access\export_mb_reader.py" $bridge 2>&1 | Add-Content -LiteralPath $log
    if ($LASTEXITCODE -ne 0) { throw "MusicBrainz reader export failed" }

    $tn = "MMSupervisorPopulateMBReader"
    $tr = "`"$py`" `"$dir\system_keyring_populate.py`" `"$bridge`""
    schtasks /create /tn $tn /tr $tr /sc ONCE /st 00:00 /ru SYSTEM /rl HIGHEST /f | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "could not create SYSTEM bootstrap task" }
    schtasks /run /tn $tn | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "could not run SYSTEM bootstrap task" }
    Start-Sleep -Seconds 6
    "$(Get-Date -Format o) SYSTEM task completed" | Add-Content -LiteralPath $log
}
catch {
    "$(Get-Date -Format o) ERROR $($_.Exception.Message)" | Add-Content -LiteralPath $log
    throw
}
finally {
    schtasks /delete /tn $tn /f 2>$null | Out-Null
    Remove-Item $bridge -Force -ErrorAction SilentlyContinue
}

"$(Get-Date -Format o) DONE" | Add-Content -LiteralPath $log
Write-Output "MusicBrainz reader credential populated into LocalSystem keyring; bridge removed"
