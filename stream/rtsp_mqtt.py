#!/app/venv/bin/python

# mediamtx
import argparse
import asyncio
import contextlib
import gc
import hashlib
import json
import logging
import random
import re
import shutil
import signal
import string
import sys
import threading
import time
import traceback
import weakref
from os import path
from typing import Any, Optional

# from aiohttp import web
import paho.mqtt.client as paho
import yaml
from aiortc import (
    MediaStreamError,
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCRtpSender,
    RTCSessionDescription,
    VideoStreamTrack,
)
from aiortc.contrib.media import MediaPlayer, MediaRelay
from aiortc.sdp import candidate_from_sdp
from av import VideoFrame

APP_DIR = path.abspath(path.dirname(path.dirname(__file__)))

# david cam1
# rtsp://192.168.137.164/axis-media/media.amp
# david cam2
# rtsp://192.168.137.152/axis-media/media.amp
# utlx
# rtsp://172.21.114.30/profile2/media.smp

# Interval in seconds for sending /status messages via MQTT
# This controls how frequently the script publishes status updates
STATUS_INTERVAL = 20

exit_event = threading.Event()
logger = logging.getLogger("rtsp_mqtt")
mqtt_pub = None
main_loop = None


def handle_signal(signum, frame):
    logger.info(f"[main  ] Signal {signum} received")
    exit_event.set()


def init_log(log_level: str | None = None) -> logging.Logger:
    choices = {
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }

    log_level = log_level.lower() if log_level is not None else None
    if log_level is not None and log_level not in choices:
        print(
            f"ERROR: Invalid log level. Available options: {', '.join(choices.keys())}"
        )
        sys.exit(1)

    if log_level is None:
        logger = logging.getLogger("noop")
        logger.addHandler(logging.NullHandler())
        return logger

    levels = logging.getLevelNamesMapping()
    level = log_level.upper()
    level = levels[level] if level in levels else logging.INFO

    logger = logging.getLogger("rtsp_mqtt")
    logger.setLevel(logging.DEBUG)

    log_console = logging.StreamHandler(sys.stdout)
    log_console.setLevel(level)
    log_console.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname).1s] %(message)s",
            datefmt="%y-%m-%d %H:%M:%S",
        )
    )

    logger.addHandler(log_console)

    return logger


def load_config(config_path: str | None = None):
    """
    nacitaj hodnoty z configu

    Raises:
        ValueError: nespravna hodnota v configu
    """

    config = {}
    default_config = {}
    config_path = config_path.strip() if config_path is not None else None

    if not config_path:
        config_path = "config.yaml"
    else:
        if not config_path.endswith(".yaml"):
            config_path += ".yaml"

    # load the user-provided config
    if not path.isabs(config_path):
        base_name = path.basename(config_path)
        config_path = path.abspath(f"{APP_DIR}/config/{base_name}")

    # default config path
    config_path_default = path.abspath(f"{APP_DIR}/config/config.default.yaml")

    # load the default config
    config_loaded = False

    if path.isfile(config_path_default):
        with open(config_path_default, "r") as file:
            if not file or file == "":
                raise Exception(f"Config file {config_path_default} is empty.")

            default_config = yaml.safe_load(file)
            config_loaded = True

        if not path.isfile(config_path):
            shutil.copy(config_path_default, config_path)

    if path.isfile(config_path):
        with open(config_path, "r") as file:
            if not file or file == "":
                raise Exception(f"Config file {config_path} is empty.")

            config = yaml.safe_load(file)
            config_loaded = True
    else:
        shutil.copyfile(config_path_default, config_path)
        config_loaded = True

    if not config_loaded:
        raise Exception(f"Config file {config_path} not exists or is not readable.")

    # merge configs
    config = merge_dicts(default_config, config)

    return config


def merge_dicts(default, config):
    """Recursively merges two dictionaries."""
    if not isinstance(config, dict):
        config = {}

    for key, value in config.items():
        if isinstance(value, dict) and key in default:
            default[key] = merge_dicts(default.get(key, {}), value)
        else:
            default[key] = value
    return default


def random_hex_string(length: int = 16):
    hex_chars = string.digits + "abcdef"
    return "".join(random.choice(hex_chars) for _ in range(length))


def build_mqtt_settings(cfg, rtsp_url: str) -> dict:

    # configuration
    if not isinstance(cfg, dict):
        logger.error("[config]: Invalid configuration.")
        sys.exit(1)

    # mqtt configuration
    if "mqtt" not in cfg or not isinstance(cfg, dict):
        logger.error(
            "[config]: Invalid MQTT configuration. Please check configuration file."
        )
        sys.exit(1)

    mqtt_cfg = cfg["mqtt"]

    for k in ("host", "port", "username"):
        if k not in mqtt_cfg:
            logger.error(f"[config]: Missing key 'mqtt.{k}' in config file.")
            sys.exit(1)

    # host
    broker = mqtt_cfg.get("host")

    # port
    try:
        port = int(mqtt_cfg.get("port"))
    except Exception:
        logger.error("[config]: Invalid 'mqtt.port' value in config, must be int.")
        sys.exit(1)

    transport = mqtt_cfg.get("transport", "tcp")
    username = mqtt_cfg.get("username")
    password = mqtt_cfg.get("password", "")
    ws_path = mqtt_cfg.get("ws_path", "/mqtt")

    # protocol version
    try:
        keepalive = int(mqtt_cfg.get("keepalive", 20))
    except Exception:
        logger.error("[config]: Invalid 'mqtt.keepalive' value in config, must be int.")
        sys.exit(1)

    # protocol version
    try:
        protocol = int(mqtt_cfg.get("protocol", paho.MQTTv5))
    except Exception:
        logger.error("[config]: Invalid 'mqtt.protocol' value in config, must be int.")
        sys.exit(1)

    md5 = hashlib.md5(rtsp_url.encode("utf-8")).hexdigest()[:16]
    client_id = md5
    device_id = md5

    if not device_id:
        logger.error("[config]: Could not determine 'mqtt.client_id', exiting.")
        sys.exit(104)

    return dict(
        broker=broker,
        port=port,
        username=username,
        password=password,
        keepalive=keepalive,
        ws_path=ws_path,
        protocol=protocol,
        transport=transport,
        client_id=client_id,
        device_id=device_id,
    )


def build_ice_servers(cfg: dict) -> list:
    """
    Build RTCIceServer list from config.

    Returns:
        List of RTCIceServer objects
    """
    ice_servers = []
    add_default = False

    if "ice_servers" in cfg and isinstance(cfg["ice_servers"], list):
        if len(cfg["ice_servers"]):
            for _server in cfg["ice_servers"]:
                if "urls" not in _server:
                    logger.warning("[webrtc] ICE server entry missing 'urls', skipping")
                    continue

                urls = _server["urls"]
                username = _server.get("username")
                credential = _server.get("credential")
                credential_type = _server.get("credentialType", "password")

                try:
                    # Build kwargs conditionally based on whether auth is provided
                    kwargs = {"urls": urls}
                    if username and credential:
                        kwargs["username"] = username
                        kwargs["credential"] = credential
                        kwargs["credentialType"] = credential_type

                    ice_server = RTCIceServer(**kwargs)
                    ice_servers.append(ice_server)
                    logger.debug(f"[webrtc] added ICE server: {urls}")
                except Exception as e:
                    logger.warning(
                        f"[webrtc] failed to create ICE server from {urls}: {e}"
                    )
        else:
            add_default = True

    if add_default:
        # Default fallback ICE servers
        url = "stun:stun.l.google.com:19302"
        ice_servers.append(RTCIceServer(urls=[url]))
        logger.debug("[webrtc] no 'ice_servers' found in config")
        logger.debug(f"[webrtc] added default ICE server {url}")

    return ice_servers


def force_codec(pc: RTCPeerConnection, sender: RTCRtpSender, forced_codec: str) -> None:
    kind = forced_codec.split("/")[0]
    codecs = RTCRtpSender.getCapabilities(kind).codecs
    transceiver = next(t for t in pc.getTransceivers() if t.sender == sender)
    transceiver.setCodecPreferences(
        [codec for codec in codecs if codec.mimeType == forced_codec]
    )


class NonBufferedVideoTrack(VideoStreamTrack):
    """
    Wrapper around a video track that prevents frame accumulation.
    Always returns the latest frame, dropping old ones to prevent memory leaks.
    """

    def __init__(self, source_track):
        super().__init__()
        self.source_track = source_track
        self._latest_frame: Optional[VideoFrame] = None
        self._lock: asyncio.Lock = None  # type: ignore
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    async def _init_lock(self):
        """Initialize lock after event loop is available"""
        if self._lock is None:
            self._lock = asyncio.Lock()

    async def start(self):
        """Start background task to continuously fetch frames"""
        await self._init_lock()
        self._task = asyncio.create_task(self._fetch_frames())

    async def _fetch_frames(self):
        """Continuously fetch frames from source and keep only the latest"""
        await self._init_lock()
        frame_count = 0
        try:
            while not self._stopping:
                try:
                    frame = await self.source_track.recv()
                    async with self._lock:  # type: ignore
                        # Drop old frame immediately
                        self._latest_frame = frame

                    # Periodic garbage collection to ensure frames are released
                    frame_count += 1
                    if frame_count % 100 == 0:
                        gc.collect(generation=0)  # Quick generation 0 collection
                except MediaStreamError as e:
                    if not self._stopping:
                        logger.warning(f"[track ] MediaStreamError: {e}")
                    break
                except Exception as e:
                    if not self._stopping:
                        logger.warning(f"[track ] Error fetching frame: {e}")
                    break
        except asyncio.CancelledError:
            pass
        finally:
            # Clear frame reference on exit
            self._latest_frame = None

    async def recv(self) -> VideoFrame:
        """Return the latest available frame"""
        await self._init_lock()

        # Wait for first frame if needed
        while self._latest_frame is None and not self._stopping:
            await asyncio.sleep(0.01)

        if self._latest_frame is None:
            raise Exception("Track stopped before receiving frames")

        async with self._lock:  # type: ignore
            # Return the frame - WebRTC will handle it
            # Don't copy, just return the reference
            return self._latest_frame

    def stop(self):
        """Stop the track"""
        self._stopping = True
        if self._task and not self._task.done():
            self._task.cancel()
        # Clear frame reference to help GC
        self._latest_frame = None
        try:
            super().stop()
        except Exception:
            pass  # Ignore errors during stop


class PeerContext:
    def __init__(self, pc, remote_id: str):
        self.pc = pc
        self.remote_id = remote_id
        self.pending_ice = []
        # Track the relay track so we can stop it on disconnect
        self.relay_track: Optional[Any] = None


class SharedRTSPPlayer:
    """
    RTSP Player that shares a single stream to multiple WebRTC connections.
    Uses MediaRelay to efficiently distribute frames without decoding/encoding for each client.
    """

    def __init__(
        self, rtsp_url: str, device_id: str, config: dict, reconnect_interval: int = 30
    ):
        """
        Initialize the shared RTSP player.

        Args:
            rtsp_url: RTSP URL or v4l2 device path
            device_id: Unique identifier for this player instance
            reconnect_interval: Seconds between connection health checks
        """
        logger.debug("[cam   ] ~~~ new SharedRTSPPlayer()")

        self.rtsp_url = rtsp_url
        self.device_id = device_id

        self.player: Optional[MediaPlayer] = None

        self._active_clients = 0
        self._lock = asyncio.Lock()
        self._stopping = False

        self._watchdog_task: Optional[asyncio.Task] = None
        self._last_frame_time = time.time()
        self._ready = asyncio.Event()

        # configuration
        cfg = (
            config["camera"]
            if "camera" in config and isinstance(config["camera"], dict)
            else {}
        )

        try:
            self.reconnect_interval = int(cfg.get("reconnect_interval", 30))
        except Exception:
            logger.error(
                "[config]: Invalid 'camera.reconnect_interval' value in config, must be int."
            )
            sys.exit(1)

        try:
            self.video_width = int(cfg.get("video_width", 1024))
        except Exception:
            logger.error(
                "[config]: Invalid 'camera.video_width' value in config, must be int."
            )
            sys.exit(1)

        try:
            self.video_height = int(cfg.get("video_height", 768))
        except Exception:
            logger.error(
                "[config]: Invalid 'camera.video_height' value in config, must be int."
            )
            sys.exit(1)

        try:
            self.fps = int(cfg.get("fps", 15))
        except Exception:
            logger.error("[config]: Invalid 'camera.fps' value in config, must be int.")
            sys.exit(1)

    def _create_player(self):
        """Create the MediaPlayer with appropriate settings for the source type."""
        resolution = "{}x{}".format(str(self.video_width), str(self.video_height))

        logger.info(
            "[cam   ] ~~~ creating player: %s (resolution: %s)",
            self.rtsp_url,
            resolution,
        )

        self._last_frame_time = time.time()

        is_v4l2_device = self.rtsp_url.startswith("/dev/video")
        is_rtsp = self.rtsp_url.startswith("rtsp://")

        try:
            if is_v4l2_device:
                logger.info(
                    "[cam   ] %s ~~~  ^ using v4l2 device: %s",
                    self.device_id,
                    self.rtsp_url,
                )
                self.player = MediaPlayer(
                    self.rtsp_url,
                    format="v4l2",
                    options={
                        "input_format": "yuv420p",
                        "framerate": str(self.fps),
                    },
                )
            elif is_rtsp:
                logger.info("[cam   ] ~~~ ^ using RTSP stream")
                self.player = MediaPlayer(
                    self.rtsp_url,
                    # format="rtsp",
                    options={
                        # "video_size": str(resolution),
                        "rtsp_transport": "tcp",
                        # # "rtsp_flags": "prefer_tcp",
                        # "fflags": "nobuffer+flush_packets",
                        "threads": "1",
                        # "flags": "low_delay",
                        # "max_delay": "0",
                        # "buffer_size": "1024000",
                        # "timeout": "5000000",
                        # "reorder_queue_size": "0",
                        # "framerate": str(self.fps),  # Set the target FPS (e.g., 60)
                        # "probesize": "32",  # Reduce initial analysis time
                        # "analyzeduration": "0",  # Start stream immediately
                        # "preset": "ultrafast",  # Test
                        # # "tune": "zerolatency", # Test
                        # "vn": "0",  # video enabled
                        # "an": "1",  # audio disabled
                    },
                )
            else:
                logger.info("[cam   ] ~~~ ^ auto-detecting source format")
                self.player = MediaPlayer(self.rtsp_url)

            if not self.player or not self.player.video:
                raise RuntimeError("Failed to create player with video track")

        except Exception as e:
            logger.error(
                "[cam   ] !!! failed to create MediaPlayer: %s",
                str(e),
            )
            raise

        # Wrap recv to track frame timing and health
        original_recv = self.player.video.recv
        player_ref = weakref.ref(self)

        async def recv_wrapper():
            try:
                frame = await original_recv()
                player = player_ref()

                if player is not None and not player._stopping:
                    player._last_frame_time = time.time()
                    if not player._ready.is_set():
                        logger.info(
                            "[cam   ] ~~~ first frame received from RTSP stream"
                        )
                        player._ready.set()
                return frame
            except Exception as e:
                player = player_ref()

                if (
                    player is not None
                    and player._active_clients > 0
                    and not player._stopping
                ):
                    logger.error("[cam   ] !!! error receiving frame: %s", str(e))
                raise

        self.player.video.recv = recv_wrapper

    async def _start_locked(self):
        """Start the player (must be called with lock held)."""
        if self.player is None:
            self.relay = MediaRelay()

            self._create_player()
            self._start_watchdog()

    async def _stop_locked(self):
        """Stop the player and cleanup resources (must be called with lock held)."""
        self._stopping = True

        if self._watchdog_task:
            self._watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task
            self._watchdog_task = None

        if self.player:
            player = self.player

            # Clear references first
            self.player = None
            self.relay = None
            self._ready.clear()

            # Let pending frame reads complete
            await asyncio.sleep(0.1)

            try:
                if player.video:
                    try:
                        player.video.stop()
                    except Exception as e:
                        logger.error(
                            f"[cam   ] !!! {self.device_id} | exception stopping video track: {e}"
                        )

                try:
                    if hasattr(player, "_stop") and player.video:
                        player._stop(player.video)  # type: ignore
                except Exception as e:
                    logger.error(
                        f"[cam   ] !!! {self.device_id} | exception stopping player: {e}"
                    )

                await asyncio.sleep(0.05)

            except Exception as e:
                logger.error(
                    f"[cam   ] !!! {self.device_id} | unexpected exception during player cleanup: {e}"
                )
            finally:
                self.player = None
                self.relay = None
                self._ready.clear()

                gc.collect()
                logger.debug(
                    f"[cam   ] {self.device_id} ~~~ player stopped and cleaned up"
                )

        self._stopping = False

    def _start_watchdog(self):
        """Start the watchdog task for health monitoring."""
        if self._watchdog_task is None:
            self._watchdog_task = asyncio.get_running_loop().create_task(
                self._watchdog()
            )

    async def _watchdog(self):
        """Monitor stream health and reconnect if needed."""
        while True:
            # Memory monitoring on Linux
            if sys.platform != "win32":
                try:
                    with open("/proc/self/status", "r") as f:
                        for line in f:
                            if line.startswith("VmRSS:"):
                                rss_kb = int(line.split()[1])
                                logger.info(
                                    f"[mem   ] {self.device_id} ~~~ RSS: {rss_kb // 1024} MB, Active clients: {self._active_clients}"
                                )
                                break
                except Exception:
                    pass

            # Periodic garbage collection
            gc.collect()

            await asyncio.sleep(self.reconnect_interval)

            async with self._lock:
                if self._active_clients == 0 or self.player is None:
                    continue

                # Don't timeout during initial connection
                if not self._ready.is_set():
                    continue

                # Check for stream timeout
                if time.time() - self._last_frame_time > self.reconnect_interval * 3:
                    logger.warning(
                        f"[cam   ] !!! {self.device_id} | RTSP timeout detected, reconnecting..."
                    )

                    await self._stop_locked()
                    await self._start_locked()

    async def add_client(self, remote_id: str):
        """
        Add a client connection. Starts the stream if this is the first client.

        Args:
            remote_id: Unique identifier for the client
        """
        async with self._lock:
            self._active_clients += 1

            logger.info(
                f"[cam   ] {remote_id} ~~~ camera client added (active: {self._active_clients})"
            )

            if self._active_clients == 1:
                await self._start_locked()

    async def remove_client(self, remote_id: str):
        """
        Remove a client connection. Stops the stream if this was the last client.

        Args:
            remote_id: Unique identifier for the client
        """
        async with self._lock:
            self._active_clients = max(0, self._active_clients - 1)

            logger.info(
                f"[cam   ] {remote_id} ~~~ camera client removed (active: {self._active_clients})"
            )

            if self._active_clients == 0:
                logger.info(
                    f"[cam   ] {self.device_id} ~~~ no camera clients connected, stopping RTSP stream"
                )

                await self._stop_locked()

    async def get_track(self, use_relay: bool = True):
        """
        Get a video track for a WebRTC connection.

        Args:
            use_relay:
                If True, returns a relay track (allows multiple consumers).
                If False, wraps track to prevent memory accumulation.

        Returns:
            Tuple of (track, is_relay) where is_relay indicates if track needs explicit stop
        """
        async with self._lock:
            if not self.player or not self.player.video:
                logger.warning("[cam   ] ~~~ RTSP player not initialized")
                return None, False

            if use_relay and self.relay:
                # CRITICAL: Use buffered=False to prevent memory accumulation
                # This drops frames if the consumer can't keep up
                track = self.relay.subscribe(self.player.video, buffered=False)
                logger.debug("[cam   ] ~~~ created relay track with buffered=False")

                return track, True  # is_relay=True
            else:
                # Without relay, wrap the track to prevent memory accumulation
                wrapped_track = NonBufferedVideoTrack(self.player.video)
                await wrapped_track.start()
                logger.debug(
                    "[cam   ] ~~~ created non-buffered wrapper track (no relay)"
                )

                return (
                    wrapped_track,
                    True,
                )  # Return True so it gets cleaned up properly

    async def shutdown(self):
        """Shutdown the player and cleanup all resources."""
        async with self._lock:
            self._active_clients = 0
            await self._stop_locked()


class MQTTPublisher:
    SDP_OFFER_PATTERN = re.compile(r"([0-9a-zA-Z\-\_]+)/sdp/([^/]+)/offer")
    ICE_OFFER_PATTERN = re.compile(r"([0-9a-zA-Z\-\_]+)/ice/([^/]+)/offer")

    def __init__(self, cfg: dict):
        """Init"""

        self.config = cfg
        self.settings = build_mqtt_settings(cfg, rtsp_url=args.rtsp_url)
        self.peers = {}

        # Lock to prevent race conditions during cleanup
        self._cleanup_lock = asyncio.Lock()

        # Track cleanup tasks to prevent them from being destroyed while pending
        self._cleanup_tasks = set()

        self._connected = False
        self._closed = False

        self.client_id = self.settings["client_id"]
        self.device_id = self.settings["device_id"]

        logger.info(f"[mqtt  ] CLIENT_ID: {self.client_id}")
        logger.info(f"[mqtt  ] DEVICE_ID: {self.device_id}")
        logger.info(f"[mqtt  ] MQTT BROKER: {self.settings['broker']}")

        self.camera = None

        # mqtt status publishing task
        self._status_task: Optional[asyncio.Task] = None
        self._status_interval = args.status if not args.no_status else 0

        # mqtt client
        self.client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,  # type: ignore
            client_id=self.settings["client_id"],
            protocol=self.settings["protocol"],
            transport=self.settings["transport"],
        )

        if self.settings["username"]:
            self.client.username_pw_set(
                self.settings["username"],
                self.settings["password"],
            )

        if self.settings["transport"] == "websockets":
            self.client.tls_set()
            self.client.ws_set_options(path=self.settings["ws_path"])

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        # Build ICE servers from config
        ice_servers = build_ice_servers(cfg["mqtt"])
        self.webrtc_config = RTCConfiguration(
            iceServers=ice_servers,
        )

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if hasattr(reason_code, "value") and reason_code.value == 0:
            self._connected = True
            logger.info("[mqtt  ] connected successfully")
        elif reason_code == 0:
            self._connected = True
            logger.info("[mqtt  ] connected successfully (int reason_code).")
        else:
            self._connected = False
            logger.warning(f"[mqtt  ] connect failed ({reason_code})")

        self.subscribe(f"{self.device_id}/sdp/+/offer")
        self.subscribe(f"{self.device_id}/ice/+/offer")

        # Start status publishing task
        if self._status_interval > 0 and self._connected and main_loop is not None:
            asyncio.run_coroutine_threadsafe(self._start_status_task(), main_loop)

    def _on_message(self, client, userdata, msg):
        sdp_match = self.SDP_OFFER_PATTERN.match(msg.topic)
        ice_match = self.ICE_OFFER_PATTERN.match(msg.topic)

        if sdp_match:
            remote_id = sdp_match.group(2)  # group(1) is local client id
            payload = json.loads(msg.payload)

            if main_loop is None:
                logger.warning("[mqtt  ] main_loop not available")
            else:
                asyncio.run_coroutine_threadsafe(
                    self.handle_remote_offer(payload, remote_id), main_loop
                )

        elif ice_match:
            remote_id = ice_match.group(
                2
            )  # group(1) is local client id# group(1) is local client id
            payload = json.loads(msg.payload)

            if main_loop is None:
                logger.warning("[mqtt  ] main_loop not available")
            else:
                asyncio.run_coroutine_threadsafe(
                    self.handle_remote_ice(payload, remote_id), main_loop
                )

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected = False
        logger.info(f"[mqtt  ] disconnected ({reason_code})")

    def connect(self):
        """Connect mqtt client"""
        self.client.connect(
            self.settings["broker"],
            self.settings["port"],
            keepalive=self.settings["keepalive"],
        )

        self.client.loop_start()

    def subscribe(self, topic: str, qos: int = 0):
        """Subscribe the client to one or more topics."""
        if self._connected:
            logger.info(f"[mqtt  ] subscribe: {topic}")
            self.client.subscribe(topic, qos)

    def publish(self, topic: str, payload: str | dict | None):
        if not self._connected:
            logger.warning("MQTT not connected, publish skipped")
            return

        try:
            if isinstance(payload, dict):
                payload = json.dumps(payload)

            result = self.client.publish(topic, payload, qos=0)

            if result.rc != paho.MQTT_ERR_SUCCESS:
                logger.warning(f"[mqtt  ] publish failed, code: {result.rc}")
        except Exception as e:
            logger.error(f"[mqtt  ] publish exception: {e}")

    def is_peer_connected(self, remote_id: str) -> bool:
        """Check whether the client is connected or not"""
        ctx = self.peers.get(remote_id)
        if ctx is not None and ctx.pc.connectionState not in [
            "failed",
            "closed",
        ]:
            return True
        return False

    async def _cleanup_peer(self, remote_id: str, ctx: PeerContext):
        """
        Centralized cleanup for peer connections.
        Ensures relay track is stopped and resources are freed.
        """
        async with self._cleanup_lock:
            logger.debug(
                "[webrtc] %s ~~~ cleaning up peer %s",
                self.device_id,
                remote_id,
            )

            # Stop the relay track FIRST (before closing PC)
            if ctx.relay_track is not None:
                try:
                    # Check if it's a NonBufferedVideoTrack or MediaRelay track
                    if hasattr(ctx.relay_track, "stop"):
                        ctx.relay_track.stop()
                        logger.debug(
                            "[webrtc] %s ~~~ relay track stopped for peer %s",
                            self.device_id,
                            remote_id,
                        )
                except AttributeError:
                    # Track doesn't have stop method, just clear reference
                    logger.debug(
                        "[webrtc] %s ~~~ relay track has no stop method, clearing reference for peer %s",
                        self.device_id,
                        remote_id,
                    )
                except Exception as e:
                    logger.debug(
                        "[webrtc] %s ~~~ error stopping relay track for peer %s: %s (ignoring)",
                        self.device_id,
                        remote_id,
                        str(e),
                    )

                ctx.relay_track = None

            # Close the peer connection
            try:
                # Stop all transceivers before closing (releases RateBucket objects)
                try:
                    for transceiver in ctx.pc.getTransceivers():
                        try:
                            await transceiver.stop()
                        except Exception:
                            pass
                except Exception:
                    pass  # PC might already be closed

                # Close peer connection and wait for it to complete
                try:
                    await asyncio.wait_for(ctx.pc.close(), timeout=2.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        "[webrtc] !!! %s | timeout closing peer connection for %s",
                        self.device_id,
                        remote_id,
                    )

                logger.debug(
                    "[webrtc] %s ~~~ peer connection closed %s",
                    self.device_id,
                    remote_id,
                )
            except Exception as e:
                logger.warning(
                    "[webrtc] !!! %s | error closing pc (expected if already closed): %s",
                    self.device_id,
                    e,
                )

            # Clear pending ICE candidates
            ctx.pending_ice.clear()

            # Decrement camera client count AFTER closing PC
            if self.camera:
                await self.camera.remove_client(remote_id)

            # Small delay to let asyncio clean up pending callbacks
            await asyncio.sleep(0.1)

            # Force garbage collection to free RateBucket objects
            gc.collect()

            logger.debug("[webrtc] %s ~~~ cleanup complete ", remote_id)

    async def handle_remote_offer(self, payload: dict, remote_id: str):
        """Remote offer"""

        logger.debug("")

        if not remote_id:
            logger.warning(
                "[mqtt  ] %s !!! STREAM REQUESTED - unknown client id",
                "????????????????",
            )
            return

        logger.debug("[mqtt  ] %s !!! STREAM REQUESTED", remote_id)

        if not payload or not isinstance(payload, dict):
            logger.warning(
                "[mqtt  ] %s <<< !!! received invalid SDP offer from client",
                remote_id,
            )
            return

        # Properly cleanup old peer using centralized method
        old_ctx = self.peers.pop(remote_id, None)
        if old_ctx:
            await self._cleanup_peer(remote_id, old_ctx)
            # Longer delay after cleanup to let resources settle
            # This prevents rapid reconnection from causing object accumulation
            await asyncio.sleep(0.2)

        logger.debug("[mqtt  ] %s ~~~ new RTCPeerConnection()", remote_id)

        pc = RTCPeerConnection(self.webrtc_config)
        ctx = PeerContext(pc, remote_id)
        self.peers[remote_id] = ctx

        @pc.on("track")
        def on_track(track):
            logger.debug(
                "[webrtc] %s ~~~ webrtc event: track - kind=%s",
                remote_id,
                track.kind,
            )

        @pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = pc.connectionState
            logger.debug(
                "[webrtc] %s ~~~ webrtc event: connectionstatechange - %s",
                remote_id,
                state,
            )

            if state in ["failed", "closed"]:
                # Only cleanup if we're still tracking this peer
                old_peer = self.peers.pop(remote_id, None)
                if old_peer:
                    # Use create_task to avoid blocking the event handler
                    # Store task reference to prevent garbage collection
                    task = asyncio.create_task(self._cleanup_peer(remote_id, old_peer))
                    self._cleanup_tasks.add(task)
                    task.add_done_callback(self._cleanup_tasks.discard)

        @pc.on("iceconnectionstatechange")
        async def on_iceconnectionstatechange():
            logger.debug(
                "[webrtc] %s ~~~ webrtc event: iceconnectionstatechange - %s",
                remote_id,
                pc.iceConnectionState,
            )

        @pc.on("icegatheringstatechange")
        async def on_icegatheringstatechange():
            logger.debug(
                "[webrtc] %s ~~~ webrtc event: icegatheringstatechange - %s",
                remote_id,
                pc.iceGatheringState,
            )

            if pc.iceGatheringState == "complete":
                topic = f"{self.device_id}/ice/{remote_id}"
                self.publish(topic, {"candidate": None})

        # set offer from the client
        offer = RTCSessionDescription(payload["sdp"], payload["type"])

        # apply the remote offer
        await pc.setRemoteDescription(offer)

        logger.debug("[webrtc] %s >>> setRemoteDescription()", remote_id)

        # sleep
        # await asyncio.sleep(0.1)

        for c in ctx.pending_ice:
            # logger.debug(
            #     "[webrtc] %s <<< adding ICE candidate from client %s",
            #     self.client_id,
            #     remote_id,
            # )

            await pc.addIceCandidate(c)
        ctx.pending_ice.clear()

        # add local track
        try:
            if self.camera is None:
                self.camera = SharedRTSPPlayer(
                    args.rtsp_url, self.device_id, self.config
                )

            await self.camera.add_client(remote_id)

            logger.debug("[webrtc] %s >>> getting camera track", remote_id)

            # get_track now returns (track, is_relay) tuple
            track, is_relay = await self.camera.get_track(use_relay=args.use_relay)

            if track is None:
                raise RuntimeError("Failed to get track from camera")

            # Store relay track reference for cleanup
            if is_relay:
                ctx.relay_track = track

            logger.debug("[webrtc] %s >>> adding track to peer connection", remote_id)
            sender = pc.addTrack(track)

            if args.force_h264:
                # prefer H.264 by setting codec preferences on the transceiver
                try:
                    force_codec(pc, sender, "video/H264")
                    logger.debug(
                        "[webrtc] %s - set codec preferences to H.264", remote_id
                    )
                except Exception as e:
                    logger.debug(
                        "[webrtc] %s - failed to set codec preferences: %s",
                        remote_id,
                        e,
                    )

        except Exception as e:
            logger.error(
                "[webrtc] %s >>> exception during track setup: %s", remote_id, str(e)
            )
            logger.error(
                "[webrtc] %s >>> exception traceback: %s",
                remote_id,
                traceback.format_exc(),
            )

        # create and set local answer
        logger.debug("[webrtc] %s >>> createAnswer()", remote_id)
        answer = await pc.createAnswer()

        logger.debug("[webrtc] %s >>> setLocalDescription()", remote_id)
        await pc.setLocalDescription(answer)

        # send answer to the client
        topic = f"{self.device_id}/sdp/{remote_id}"
        logger.debug(
            "[mqtt  ] %s >>> sending SDP answer to the client: %s", remote_id, topic
        )

        self.publish(
            topic, {"type": pc.localDescription.type, "sdp": pc.localDescription.sdp}
        )

    # ---------------------------------------------------------------------
    # WEBRTC ICE configuration
    # ---------------------------------------------------------------------

    async def handle_remote_ice(self, payload, remote_id: str):
        if (
            not payload
            or not isinstance(payload, dict)
            or "candidate" not in payload
            or payload["candidate"] is None
        ):
            # End-of-candidates signal, ignore silently
            return

        ctx = self.peers.get(remote_id)

        if not ctx:
            # This is common during rapid reconnects, log at debug level
            logger.warning(
                "[webrtc] %s <<< got remote ICE !!! ignoring - PeerConnection() not exist",
                remote_id,
            )
            return

        if ctx.pc.connectionState in ["connected", "closed", "failed"]:
            # Expected during cleanup, log at debug level
            logger.debug(
                "[webrtc] %s <<< got remote ICE !!! ignoring - wrong connection state: %s",
                remote_id,
                ctx.pc.connectionState,
            )

            ctx.pending_ice.clear()

            return

        try:
            sdp = payload["candidate"]
            logger.debug(
                "[webrtc] %s <<< got remote ICE - %s",
                remote_id,
                sdp[:79] + "..." if len(sdp) > 79 else sdp,
            )

            if sdp.startswith("a="):
                sdp = sdp[2:]

            cand = candidate_from_sdp(sdp)
            cand.sdpMid = payload.get("sdpMid")
            cand.sdpMLineIndex = payload.get("sdpMLineIndex")

            if ctx.pc.remoteDescription is None:
                ctx.pending_ice.append(cand)
            else:
                # logger.debug(
                #     "[webrtc] %s <<< adding ICE candidate from client %s",
                #     self.client_id,
                #     remote_id,
                # )
                await ctx.pc.addIceCandidate(cand)
        except Exception as e:
            logger.debug(
                "[webrtc] %s <<< ICE parse error from client (may be expected during cleanup): %s",
                remote_id,
                e,
            )

    # ---------------------------------------------------------------------
    # MQTT /status message
    # ---------------------------------------------------------------------

    async def _start_status_task(self):
        """Start the periodic status publishing task."""
        if self._status_task is None or self._status_task.done():
            logger.info(
                "[mqtt  ] status publishing task started (interval: %ss)",
                self._status_interval,
            )

            # publish once
            await asyncio.sleep(1.5)
            await self._publish()

            # start task
            self._status_task = asyncio.create_task(self._publish_status_periodically())

    async def _publish_status_periodically(self):
        """Publish status message every N seconds."""
        while not self._closed and not exit_event.is_set():
            try:
                await asyncio.sleep(self._status_interval)
                await self._publish()

            except asyncio.CancelledError:
                logger.info("[mqtt  ] status publishing task cancelled")
                break
            except Exception as e:
                logger.error(f"[mqtt  ] error in status publishing: {e}")
                await asyncio.sleep(5)  # Wait before retrying

    async def _publish(self):
        if self._connected and not self._closed:
            status_data = {
                "device_id": self.device_id,
                "device_type": "camera",
                "ts": int(time.time()),
                "status": "alive",
            }

            # Add camera info if available
            if self.camera:
                status_data["camera_ready"] = self.camera._ready.is_set()

            topic = f"device/{self.device_id}/status"
            self.publish(topic, status_data)
            logger.debug("[mqtt  ] %s | device status - %s", self.device_id, topic)

    def close(self):
        logger.debug("[mqtt  ] %s ~~~ MQTTPublisher close()", self.device_id)

        # Stop status task
        if self._status_task and not self._status_task.done():
            self._status_task.cancel()

        self.client.disconnect()
        self.client.loop_stop()
        self._closed = True


async def run_app(args):
    global logger, main_loop, mqtt_pub

    # init logger
    logger = init_log(args.log_level)

    # load configuration
    cfg = load_config(args.config)

    # init main loop
    main_loop = asyncio.get_running_loop()

    # Custom exception handler to suppress expected aioice errors during cleanup
    original_handler = main_loop.get_exception_handler()

    def custom_exception_handler(loop, context):
        msg = context.get("message", "")
        exc = context.get("exception")
        exc_str = str(exc) if exc else ""

        # Suppress known aioice/STUN errors that occur during normal cleanup
        if "NoneType" in exc_str and (
            "sendto" in exc_str or "call_exception_handler" in exc_str
        ):
            logger.warning("[aioice] !!! suppressed expected STUN cleanup error")
            return
        if "Transaction.__retry" in msg or "send_stun" in msg:
            logger.warning("[aioice] !!! suppressed expected STUN retry error")
            return
        # Suppress RTCIceTransport closed errors (happen during rapid reconnects)
        if "RTCIceTransport is closed" in exc_str:
            logger.warning(
                "[webrtc] !!! suppressed expected ICE transport closed error"
            )
            return
        if "InvalidStateError" in exc_str:
            logger.warning(
                "[webrtc] suppressed !!! expected InvalidStateError during cleanup"
            )
            return

        # Suppress TURN channel bind errors (can occur with multiple simultaneous clients)
        if (
            "TransactionFailed" in exc_str
            or "TransactionFailed" in str(type(exc).__name__)
            or "channel_bind" in msg
            or "send_data" in msg
            or "TurnClientMixin.send_data" in msg
        ):
            logger.warning(
                "[turn  ] !!! suppressed TURN channel bind error (%s)",
                str(exc_str),
            )
            return

        # For other exceptions, use original handler or default
        if original_handler:
            original_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    main_loop.set_exception_handler(custom_exception_handler)

    # mqtt
    mqtt_pub = MQTTPublisher(cfg)
    mqtt_pub.connect()

    try:
        while not exit_event.is_set():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("[main  ] canceling tasks...")
    finally:
        logger.info("[main  ] starting shutdown sequence...")

        if mqtt_pub and mqtt_pub.camera:
            logger.info("[main  ] stopping RTSP player")
            await mqtt_pub.camera.shutdown()

        if mqtt_pub.device_id and mqtt_pub._connected and not mqtt_pub._closed:
            status_data = {
                "device_id": mqtt_pub.device_id,
                "ts": int(time.time()),
                "status": "shutdown",
            }

            topic = f"device/{mqtt_pub.device_id}/status"
            mqtt_pub.publish(topic, status_data)
            logger.debug(
                "[mqtt  ] %s | device status - sending shutdown info - %s",
                mqtt_pub.device_id,
                topic,
            )

        if mqtt_pub and mqtt_pub.peers:
            logger.debug(f"[main  ] closing {len(mqtt_pub.peers)} peer connections...")
            # Use centralized cleanup for all peers
            for remote_id, ctx in list(mqtt_pub.peers.items()):
                await mqtt_pub._cleanup_peer(remote_id, ctx)
            mqtt_pub.peers.clear()

        # Wait for any pending cleanup tasks to complete
        if mqtt_pub and mqtt_pub._cleanup_tasks:
            logger.debug(
                f"[main  ] waiting for {len(mqtt_pub._cleanup_tasks)} cleanup tasks..."
            )
            await asyncio.gather(*mqtt_pub._cleanup_tasks, return_exceptions=True)
            mqtt_pub._cleanup_tasks.clear()

        if mqtt_pub:
            mqtt_pub.close()

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in tasks:
            t.cancel()

        if tasks:
            logger.debug(f"[main  ] cleaning remaining {len(tasks)} tasks...")
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.debug("[main  ] bridge successfully closed")


if __name__ == "__main__":
    # arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        help="Configuration file (default: /app/rtsp_streamer/conf/config.json)",
        metavar="[path]",
    )
    parser.add_argument("--rtsp-url", help="RTSP url", required=True, metavar="[url]")
    parser.add_argument(
        "--status",
        help="How frequently the script publishes status updates",
        default=STATUS_INTERVAL,
        type=int,
        metavar="[seconds]",
    )
    parser.add_argument(
        "--no-status",
        help="Disable sending status messages",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--force-h264",
        help="Force H264 codec for stream",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "--use-relay",
        help="Use Media Relay",
        action="store_true",
        default=False,
    )
    parser.add_argument("--log-level", help="Log level", default="info")

    args = parser.parse_args()

    # signals
    signal.signal(signal.SIGINT, handle_signal)
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, handle_signal)

    # loop
    try:
        asyncio.run(run_app(args))
    except KeyboardInterrupt:
        exit_event.set()
    except Exception:
        print(traceback.format_exc())
