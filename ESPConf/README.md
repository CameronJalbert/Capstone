# ESP32 Config Staging

This folder stores ESP32 sketch/config snapshots used during capstone iterations.

## Structure

- `InitialTest/`: early camera test artifacts
- `LegacyTestingphase/`: archived Arduino CameraWebServer testing phase files

## Credential Safety

- `LegacyTestingphase/CameraWebServer.ino` is local-only and ignored by Git because it may contain real Wi-Fi credentials.
- `LegacyTestingphase/CameraWebServer.example.ino` is the safe tracked template for sharing/commit history.
