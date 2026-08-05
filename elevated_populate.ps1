# Elevated: copy the Vault token + unseal key from the interactive user's keyring
# into LocalSystem's keyring. Runs as elevated jwcolby to read the source, then a
# temporary /RU SYSTEM scheduled task to write the destination. The bridge file
# holds the plaintext secrets only for the few seconds between, then is deleted.
$py = "C:\Users\jwcol\AppData\Local\Programs\Python\Python310\python.exe"
$dir = "E:\DevPython\DataSourceQueue\Supervisor"
$bridge = "$dir\.secrets_bridge.json"

& $py "$dir\export_secrets.py" $bridge

$tn = "MMSupervisorPopulate"
$tr = "`"$py`" `"$dir\system_keyring_populate.py`" `"$bridge`""
schtasks /create /tn $tn /tr $tr /sc ONCE /st 00:00 /ru SYSTEM /rl HIGHEST /f
schtasks /run /tn $tn
Start-Sleep -Seconds 6
schtasks /delete /tn $tn /f

Remove-Item $bridge -Force -ErrorAction SilentlyContinue
Write-Output "populate flow done; bridge file removed"
