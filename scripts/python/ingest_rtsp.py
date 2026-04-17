from __future__ import annotations

"""Record a configured camera stream to segmented local video files."""

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def resolve_path(value: str) -> Path:
    """Resolve relative project paths against repository root."""
    p = Path(value)
    return p if p.is_absolute() else ROOT / p


def load_config(config_path: Path) -> dict:
    """Load JSON configuration from disk."""
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_ffmpeg_command(config: dict) -> list[str]:
    """Build FFmpeg command for segmented recording of the configured stream."""
    camera = config["camera"]
    recording = config["recording"]

    stream_url = str(camera.get("stream_url", "")).strip()
    rtsp_url = str(camera.get("rtsp_url", "")).strip()
    input_url = stream_url or rtsp_url
    if not input_url:
        raise ValueError("No camera stream URL configured. Set camera.stream_url or camera.rtsp_url.")

    ffmpeg_path = recording.get("ffmpeg_path", "ffmpeg")
    segment_seconds = int(recording.get("segment_seconds", 60))
    output_pattern = str(resolve_path(recording["output_pattern"]))

    output_parent = Path(output_pattern).parent
    output_parent.mkdir(parents=True, exist_ok=True)

    cmd = [ffmpeg_path]
    if input_url.lower().startswith("rtsp://"):
        cmd.extend(["-rtsp_transport", "tcp"])
    cmd.extend(
        [
            "-i",
            input_url,
            "-c",
            "copy",
            "-f",
            "segment",
            "-segment_time",
            str(segment_seconds),
            "-reset_timestamps",
            "1",
            "-strftime",
            "1",
            output_pattern,
        ]
    )
    return cmd


def main() -> int:
    """Run FFmpeg recorder and mirror console output into a log file."""
    parser = argparse.ArgumentParser(
        description="Network stream ingest (RTSP or HTTP MJPEG) and local recording via FFmpeg."
    )
    parser.add_argument(
        "--config",
        default=str(ROOT / "configs" / "app" / "settings.local.json"),
        help="Path to settings JSON (defaults to settings.local.json).",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        config_path = ROOT / "configs" / "app" / "settings.example.json"
    config = load_config(config_path)

    log_file = resolve_path(config["recording"]["log_file"])
    log_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = build_ffmpeg_command(config)

    print("Starting recorder:")
    print(" ".join(cmd))
    print(f"Logging FFmpeg output to: {log_file}")

    with log_file.open("a", encoding="utf-8") as log:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        try:
            for line in process.stdout:
                print(line.rstrip())
                log.write(line)
        except KeyboardInterrupt:
            print("Stopping recorder...")
            process.terminate()
            process.wait(timeout=8)

        return process.returncode or 0


if __name__ == "__main__":
    raise SystemExit(main())
