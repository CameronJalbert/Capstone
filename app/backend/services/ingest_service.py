from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Generator

import cv2
import numpy as np


@dataclass
class IngestConfig:
    """Holds stream ingest runtime settings."""

    source_type: str = "network"
    stream_url: str = ""
    usb_index: int = 0
    frame_width: int = 1280
    frame_height: int = 720
    capture_fps: float = 12.0
    output_fps: float = 10.0
    reconnect_delay_seconds: float = 2.0
    stale_frame_seconds: float = 5.0


class CameraIngestService:
    """Owns one camera connection and exposes shared frame access."""

    def __init__(self) -> None:
        """Initialize internal ingest state and thread controls."""
        self._config = IngestConfig()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._capture: cv2.VideoCapture | None = None
        self._latest_frame: np.ndarray | None = None
        self._latest_jpeg: bytes | None = None
        self._latest_frame_ts: float | None = None
        self._latest_frame_seq = 0
        self._frame_count = 0
        self._consecutive_failures = 0
        self._active_stream_clients = 0

        self._placeholder_jpeg = self._build_placeholder_jpeg()

    def configure_from_settings(self, settings: dict[str, Any]) -> None:
        """Load ingest behavior from app settings."""
        camera_cfg = settings.get("camera", {})
        ingest_cfg = settings.get("ingest", {})

        source_type = str(camera_cfg.get("source_type", "rtsp")).lower()
        if source_type == "usb":
            resolved_source_type = "usb"
        else:
            resolved_source_type = "network"

        stream_url = str(camera_cfg.get("stream_url", "")).strip()
        rtsp_url = str(camera_cfg.get("rtsp_url", "")).strip()
        input_url = stream_url or rtsp_url

        new_config = IngestConfig(
            source_type=resolved_source_type,
            stream_url=input_url,
            usb_index=int(camera_cfg.get("usb_index", 0)),
            frame_width=int(camera_cfg.get("live_frame_width", 1280)),
            frame_height=int(camera_cfg.get("live_frame_height", 720)),
            capture_fps=float(ingest_cfg.get("capture_fps", 12.0)),
            output_fps=float(ingest_cfg.get("output_fps", 10.0)),
            reconnect_delay_seconds=float(ingest_cfg.get("reconnect_delay_seconds", 2.0)),
            stale_frame_seconds=float(ingest_cfg.get("stale_frame_seconds", 5.0)),
        )

        with self._lock:
            self._config = new_config

    def start(self) -> None:
        """Start the background ingest loop if it is not already running."""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="camera-ingest")
        self._thread.start()

    def stop(self) -> None:
        """Stop ingest thread and release camera resources."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._thread = None
        self._release_capture()

    def get_latest_frame_copy(self) -> np.ndarray | None:
        """Return a copy of the newest frame for analysis consumers."""
        with self._lock:
            if self._latest_frame is None:
                return None
            return self._latest_frame.copy()

    def get_latest_jpeg(self, quality: int = 85) -> bytes | None:
        """Return latest JPEG bytes for HTTP responses."""
        with self._lock:
            if quality == 85 and self._latest_jpeg is not None:
                return self._latest_jpeg

        frame = self.get_latest_frame_copy()
        if frame is None:
            return None

        ok, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), int(quality)])
        if not ok:
            return None
        return encoded.tobytes()

    def generate_mjpeg_stream(self) -> Generator[bytes, None, None]:
        """Yield multipart JPEG chunks for backend live stream proxy."""
        boundary = b"--frame\r\n"
        header = b"Content-Type: image/jpeg\r\n"
        last_seq = -1
        last_emit_ts = 0.0

        with self._lock:
            self._active_stream_clients += 1

        try:
            while not self._stop_event.is_set():
                with self._lock:
                    seq = self._latest_frame_seq
                    output_fps = max(self._config.output_fps, 1.0)
                min_interval_seconds = 1.0 / output_fps
                now = time.time()
                if seq == last_seq and (now - last_emit_ts) < min_interval_seconds:
                    time.sleep(0.01)
                    continue

                jpeg = self.get_latest_jpeg()
                if jpeg is None:
                    jpeg = self._placeholder_jpeg

                length = f"Content-Length: {len(jpeg)}\r\n\r\n".encode("utf-8")
                yield boundary + header + length + jpeg + b"\r\n"
                last_seq = seq
                last_emit_ts = now
        except GeneratorExit:
            return
        finally:
            with self._lock:
                self._active_stream_clients = max(0, self._active_stream_clients - 1)

    def status(self) -> dict[str, Any]:
        """Expose ingest status details for diagnostics and UI health."""
        with self._lock:
            frame_count = self._frame_count
            failures = self._consecutive_failures
            latest_ts = self._latest_frame_ts
            cfg = self._config
            capture_open = bool(self._capture is not None and self._capture.isOpened())
            active_stream_clients = self._active_stream_clients

        now = time.time()
        age_seconds: float | None = None
        stale = True
        if latest_ts is not None:
            age_seconds = max(0.0, now - latest_ts)
            stale = age_seconds > cfg.stale_frame_seconds

        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "source_type": cfg.source_type,
            "stream_configured": bool(cfg.stream_url or cfg.source_type == "usb"),
            "capture_open": capture_open,
            "active_stream_clients": active_stream_clients,
            "frame_count": frame_count,
            "consecutive_failures": failures,
            "last_frame_age_seconds": age_seconds,
            "stale": stale,
        }

    def _run_loop(self) -> None:
        """Run the single ingest connection loop with reconnect handling."""
        while not self._stop_event.is_set():
            start_time = time.time()
            capture = self._ensure_capture()
            if capture is None:
                time.sleep(self._config.reconnect_delay_seconds)
                continue

            ok, frame = capture.read()
            if not ok or frame is None:
                with self._lock:
                    self._consecutive_failures += 1
                    too_many_failures = self._consecutive_failures >= 5
                if too_many_failures:
                    self._release_capture()
                    time.sleep(self._config.reconnect_delay_seconds)
                else:
                    # Short backoff for transient decode hiccups to avoid visible multi-second pauses.
                    time.sleep(0.05)
                continue

            ok_jpeg, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
            with self._lock:
                self._latest_frame = frame
                if ok_jpeg:
                    self._latest_jpeg = encoded.tobytes()
                else:
                    self._latest_jpeg = None
                self._latest_frame_ts = time.time()
                self._latest_frame_seq += 1
                self._frame_count += 1
                self._consecutive_failures = 0

            capture_interval = 1.0 / max(self._config.capture_fps, 1.0)
            elapsed = time.time() - start_time
            if elapsed < capture_interval:
                time.sleep(capture_interval - elapsed)

    def _ensure_capture(self) -> cv2.VideoCapture | None:
        """Open or return the existing capture handle for the configured source."""
        with self._lock:
            cfg = self._config
            existing = self._capture

        if existing is not None and existing.isOpened():
            return existing

        if cfg.source_type == "usb":
            capture = cv2.VideoCapture(cfg.usb_index, cv2.CAP_DSHOW)
        else:
            if not cfg.stream_url:
                return None
            capture = cv2.VideoCapture(cfg.stream_url, cv2.CAP_FFMPEG)
            if not capture.isOpened():
                # Fallback for MJPEG sources where backend auto-selection is more stable than forced FFMPEG.
                capture.release()
                capture = cv2.VideoCapture(cfg.stream_url)

        if not capture.isOpened():
            capture.release()
            return None

        capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if cfg.source_type == "usb":
            # USB capture can honor local frame-shape/fps hints; network MJPEG endpoints typically ignore these.
            if cfg.frame_width > 0:
                capture.set(cv2.CAP_PROP_FRAME_WIDTH, float(cfg.frame_width))
            if cfg.frame_height > 0:
                capture.set(cv2.CAP_PROP_FRAME_HEIGHT, float(cfg.frame_height))
            if cfg.capture_fps > 0:
                capture.set(cv2.CAP_PROP_FPS, float(cfg.capture_fps))

        with self._lock:
            self._capture = capture
        return capture

    def _release_capture(self) -> None:
        """Safely release current capture handle."""
        with self._lock:
            capture = self._capture
            self._capture = None
        if capture is not None:
            capture.release()

    def _build_placeholder_jpeg(self) -> bytes:
        """Build a static fallback frame while camera stream is unavailable."""
        placeholder = np.zeros((360, 640, 3), dtype=np.uint8)
        cv2.putText(
            placeholder,
            "Waiting for camera stream...",
            (40, 180),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.8,
            (255, 255, 255),
            2,
            cv2.LINE_AA,
        )
        ok, encoded = cv2.imencode(".jpg", placeholder, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
        if not ok:
            return b""
        return encoded.tobytes()


# Global singleton used by API routes and future worker integrations.
ingest_service = CameraIngestService()
