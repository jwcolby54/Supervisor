# Elevated: start the MusicAppSupervisor service (recover after the restart left
# it stopped). Idempotent -- starting an already-running service is harmless.
$nssm = "E:\DevPython\DataSourceQueue\Supervisor\tools\nssm.exe"
& $nssm start MusicAppSupervisor
Start-Sleep -Seconds 2
& $nssm status MusicAppSupervisor
