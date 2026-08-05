# Elevated: disable Windows Fast Startup (hybrid shutdown) so that a normal
# "Shut down" performs a true cold boot instead of hibernating and restoring
# session-0 services. Reversible: set HiberbootEnabled back to 1 to re-enable.
$path = 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Power'
Set-ItemProperty -Path $path -Name HiberbootEnabled -Value 0 -Type DWord
$v = (Get-ItemProperty -Path $path -Name HiberbootEnabled).HiberbootEnabled
"HiberbootEnabled=$v (0 = Fast Startup disabled)" | Out-File -FilePath "E:\DevPython\DataSourceQueue\Supervisor\logs\fast_startup_disable.log" -Encoding utf8
