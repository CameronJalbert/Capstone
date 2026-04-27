from __future__ import annotations

import threading
import time
import os
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
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
    clip_buffer_seconds: float = 60.0
    clip_buffer_fps: float = 12.0


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
        self._frame_history: deque[tuple[float, np.ndarray]] = deque()
        self._last_history_append_ts: float = 0.0
        self._network_stream_connected = False
        self._capture_backend = ""

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
            clip_buffer_seconds=max(45.0, float(ingest_cfg.get("clip_buffer_seconds", 60.0))),
            clip_buffer_fps=max(1.0, float(ingest_cfg.get("clip_buffer_fps", 12.0))),
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

    def get_latest_frame_packet(self) -> tuple[np.ndarray | None, int, float | None]:
        """Return latest frame copy with ingest sequence id and timestamp."""
        with self._lock:
            frame = self._latest_frame.copy() if self._latest_frame is not None else None
            return frame, int(self._latest_frame_seq), self._latest_frame_ts

    def get_recent_frames(self, seconds: float) -> list[tuple[float, np.ndarray]]:
        """Return a snapshot of buffered frames within the requested lookback window."""
        window = max(0.1, float(seconds))
        cutoff = time.time() - window
        with self._lock:
            items = [(ts, frame.copy()) for ts, frame in self._frame_history if ts >= cutoff]
        return items

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
            if cfg.source_type == "network" and self._is_http_stream_url(cfg.stream_url):
                capture_open = bool(self._network_stream_connected)
            active_stream_clients = self._active_stream_clients
            capture_backend = self._capture_backend

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
            "capture_backend": capture_backend,
            "active_stream_clients": active_stream_clients,
            "frame_count": frame_count,
            "consecutive_failures": failures,
            "last_frame_age_seconds": age_seconds,
            "stale": stale,
        }

    def _run_loop(self) -> None:
        """Run the single ingest connection loop with reconnect handling."""
        while not self._stop_event.is_set():
            with self._lock:
                cfg = self._config

            if cfg.source_type == "network" and self._is_http_stream_url(cfg.stream_url):
                self._release_capture()
                handled = self._run_http_mjpeg_session(cfg)
                if not handled:
                    time.sleep(max(0.2, cfg.reconnect_delay_seconds))
                continue

            start_time = time.time()
            capture = self._ensure_capture()
            if capture is None:
                time.sleep(self._config.reconnect_delay_seconds)
                continue

            ok, frame = capture.read()
            if not ok or frame is None:
                with self._lock:
                    self._consecutive_failures += 1
                    self._network_stream_connected = False
                    too_many_failures = self._consecutive_failures >= 5
                if too_many_failures:
                    self._release_capture()
                    time.sleep(self._config.reconnect_delay_seconds)
                else:
                    # Short backoff for transient decode hiccups to avoid visible multi-second pauses.
                    time.sleep(0.05)
                continue

            self._publish_frame(frame)
            with self._lock:
                self._network_stream_connected = True

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
            # Prefer FFMPEG with explicit network timeouts so disconnects fail fast and reconnect logic can run.
            previous_ffmpeg_opts = os.environ.get("OPENCV_FFMPEG_CAPTURE_OPTIONS", "")
            ffmpeg_opts = "rw_timeout;5000000|stimeout;5000000|timeout;5000000"
            try:
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = ffmpeg_opts
            except Exception:
                pass
            open_timeout_prop = int(getattr(cv2, "CAP_PROP_OPEN_TIMEOUT_MSEC", 53))
            read_timeout_prop = int(getattr(cv2, "CAP_PROP_READ_TIMEOUT_MSEC", 54))
            buffer_size_prop = int(getattr(cv2, "CAP_PROP_BUFFERSIZE", 38))
            try:
                capture = cv2.VideoCapture(
                    cfg.stream_url,
                    cv2.CAP_FFMPEG,
                    [
                        open_timeout_prop,
                        3000,
                        read_timeout_prop,
                        int(max(2000, cfg.stale_frame_seconds * 1000)),
                        buffer_size_prop,
                        1,
                    ],
                )
            except TypeError:
                capture = cv2.VideoCapture(cfg.stream_url, cv2.CAP_FFMPEG)
            try:
                if previous_ffmpeg_opts:
                    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = previous_ffmpeg_opts
                else:
                    os.environ.pop("OPENCV_FFMPEG_CAPTURE_OPTIONS", None)
            except Exception:
                pass
            if not capture.isOpened():
                capture.release()
                # Fallback to backend auto-select only if explicit FFMPEG open fails.
                capture = cv2.VideoCapture(cfg.stream_url)

        if not capture.isOpened():
            capture.release()
            return None

        try:
            capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        except Exception:
            pass
        if cfg.source_type != "usb":
            # Network sources can hang on read during camera power cycles; keep timeouts short.
            try:
                capture.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 3000)
            except Exception:
                pass
            try:
                capture.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, int(max(2000, cfg.stale_frame_seconds * 1000)))
            except Exception:
                pass
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
            try:
                self._capture_backend = str(capture.getBackendName())
            except Exception:
                self._capture_backend = "opencv"
        return capture

    def _release_capture(self) -> None:
        """Safely release current capture handle."""
        with self._lock:
            capture = self._capture
            self._capture = None
            self._capture_backend = ""
            self._network_stream_connected = False
        if capture is not None:
            capture.release()

    def _publish_frame(self, frame: np.ndarray, *, frame_ts: float | None = None) -> None:
        """Store frame + jpeg cache + rolling clip history."""
        ts = float(frame_ts) if frame_ts is not None else time.time()
        ok_jpeg, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
        with self._lock:
            self._latest_frame = frame
            if ok_jpeg:
                self._latest_jpeg = encoded.tobytes()
            else:
                self._latest_jpeg = None
            self._latest_frame_ts = ts
            self._latest_frame_seq += 1
            self._frame_count += 1
            self._consecutive_failures = 0
            min_history_interval = 1.0 / max(self._config.clip_buffer_fps, 1.0)
            should_append_history = (
                not self._frame_history
                or self._last_history_append_ts <= 0.0
                or (self._latest_frame_ts - self._last_history_append_ts) >= min_history_interval
            )
            if should_append_history:
                self._frame_history.append((self._latest_frame_ts, frame.copy()))
                self._last_history_append_ts = self._latest_frame_ts
            cutoff = self._latest_frame_ts - max(5.0, float(self._config.clip_buffer_seconds))
            while self._frame_history and self._frame_history[0][0] < cutoff:
                self._frame_history.popleft()

    def _is_http_stream_url(self, url: str) -> bool:
        """Return true when source URL is HTTP(S) stream."""
        parsed = urllib.parse.urlparse(str(url).strip())
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

    def _run_http_mjpeg_session(self, cfg: IngestConfig) -> bool:
        """Read MJPEG frames directly over HTTP so reconnects survive camera power cycles."""
        if not cfg.stream_url:
            with self._lock:
                self._network_stream_connected = False
            return False

        timeout_seconds = max(3.0, float(cfg.stale_frame_seconds) + 1.0)
        request = urllib.request.Request(
            cfg.stream_url,
            headers={
                "User-Agent": "LAN-Cam-Ingest/1.0",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache",
            },
            method="GET",
        )
        frame_interval = 1.0 / max(cfg.capture_fps, 1.0)
        last_publish_ts = 0.0
        session_had_frames = False
        buffered = bytearray()

        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                with self._lock:
                    self._network_stream_connected = True
                    self._capture_backend = "http-mjpeg"

                while not self._stop_event.is_set():
                    chunk = response.read(8192)
                    if not chunk:
                        raise ConnectionError("mjpeg stream closed by remote camera")
                    buffered.extend(chunk)
                    if len(buffered) > 8_000_000:
                        # Keep trailing data only; protects memory during malformed boundary cases.
                        del buffered[: len(buffered) - 2_000_000]

                    while True:
                        start_idx = buffered.find(b"\xff\xd8")
                        if start_idx < 0:
                            break
                        end_idx = buffered.find(b"\xff\xd9", start_idx + 2)
                        if end_idx < 0:
                            if start_idx > 0:
                                del buffered[:start_idx]
                            break

                        jpeg_bytes = bytes(buffered[start_idx : end_idx + 2])
                        del buffered[: end_idx + 2]

                        now_ts = time.time()
                        if last_publish_ts > 0.0 and (now_ts - last_publish_ts) < frame_interval:
                            continue
                        frame = cv2.imdecode(np.frombuffer(jpeg_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                        if frame is None:
                            continue

                        self._publish_frame(frame, frame_ts=now_ts)
                        with self._lock:
                            self._network_stream_connected = True
                        last_publish_ts = now_ts
                        session_had_frames = True
        except (urllib.error.URLError, TimeoutError, OSError, ConnectionError, ValueError):
            with self._lock:
                self._consecutive_failures += 1
                self._network_stream_connected = False
            return session_had_frames
        except Exception:
            with self._lock:
                self._consecutive_failures += 1
                self._network_stream_connected = False
            return session_had_frames

        with self._lock:
            self._network_stream_connected = False
        return session_had_frames

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
