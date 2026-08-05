# Elevated: switch the service to the keyring-only model and restart it.
#  - drop the plaintext VAULT_TOKEN from the service env (keep only VAULT_ADDR,
#    which is not a secret); the queues now read the token from LocalSystem's
#    keyring, and the supervisor auto-unseals Vault with the keyring unseal key.
#  - restart so the new gated + auto-unseal supervisor code takes over.
$nssm = "E:\DevPython\DataSourceQueue\Supervisor\tools\nssm.exe"
$svc = "MusicAppSupervisor"
& $nssm set $svc AppEnvironmentExtra "VAULT_ADDR=http://127.0.0.1:18200"
& $nssm restart $svc
Write-Output "token dropped from service env; service restarted"
