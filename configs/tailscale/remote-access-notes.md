# Tailscale Access Notes

Use this file to track final remote-access decisions:

- mini PC hostname in tailnet
- ACL group allowed to reach dashboard
- test users/devices approved for demo

## Suggested Baseline

- expose only dashboard/API on mini PC over tailnet
- keep OpenWrt firewall segmentation unchanged
- avoid opening inbound router port-forward rules for dashboard

## Validation

- from remote tailnet client, load `http://<tailnet-ip>:8080`
- from non-tailnet external network, service remains unreachable

