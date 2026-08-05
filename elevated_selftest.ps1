# Elevated: run the SYSTEM-keyring self-test as LocalSystem via a temporary
# scheduled task, then remove the task. Requires admin (creating a /RU SYSTEM
# task). The python script writes its own result log.
$py = "C:\Users\jwcol\AppData\Local\Programs\Python\Python310\python.exe"
$script = "E:\DevPython\DataSourceQueue\Supervisor\system_keyring_selftest.py"
$tn = "MMSupervisorSelftest"
$tr = "`"$py`" `"$script`""
schtasks /create /tn $tn /tr $tr /sc ONCE /st 00:00 /ru SYSTEM /rl HIGHEST /f
schtasks /run /tn $tn
Start-Sleep -Seconds 6
schtasks /delete /tn $tn /f
