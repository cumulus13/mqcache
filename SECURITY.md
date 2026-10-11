# Security policy

## Reporting a vulnerability

Please report privately by e-mail to **cumulus13@gmail.com** (subject `MQCache security`) instead of opening a public issue. Include version, configuration (without secrets) and reproduction steps. You will receive an acknowledgement within a few days.

## Scope notes

* MSMQ messages are not encrypted unless `queue-encryption` is used (requires Active Directory/certificates). `requirepass` is transmitted inside the message.
* Deployments should restrict queue permissions (`queue-allow`, `queue-admin`), firewall MSMQ ports and avoid exposing MSMQ to untrusted networks.
* All wire input is length-checked and fuzz-tested in the unit tests; malformed messages are parked in the poison queue.
