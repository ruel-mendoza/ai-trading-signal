import asyncio
import json
import logging
import time
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("trading_engine.websocket")

SEND_TIMEOUT = 5.0


class SignalBroadcaster:
    def __init__(self):
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=256)
        self._worker_task: Optional[asyncio.Task] = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._queue = asyncio.Queue(maxsize=256)
        self._worker_task = loop.create_task(self._broadcast_worker())

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
        logger.info(f"[WS] Client connected ({len(self._clients)} total)")

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._clients.discard(ws)
        logger.info(f"[WS] Client disconnected ({len(self._clients)} total)")

    async def _broadcast_worker(self):
        while True:
            try:
                message = await self._queue.get()
                await self._send_to_all(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[WS] Broadcast worker error: {e}")

    async def _send_to_all(self, message: dict):
        payload = json.dumps(message)
        async with self._lock:
            clients = list(self._clients)
        if not clients:
            return
        stale: list[WebSocket] = []
        for client in clients:
            try:
                await asyncio.wait_for(client.send_text(payload), timeout=SEND_TIMEOUT)
            except Exception:
                stale.append(client)
        if stale:
            async with self._lock:
                for s in stale:
                    self._clients.discard(s)
            logger.info(f"[WS] Removed {len(stale)} stale client(s) ({len(self._clients)} remaining)")

    def broadcast_sync(self, message: dict):
        if not self._clients:
            return
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._enqueue, message)
        except RuntimeError:
            pass

    def _enqueue(self, message: dict):
        try:
            self._queue.put_nowait(message)
        except asyncio.QueueFull:
            logger.warning("[WS] Broadcast queue full, dropping message")

    def broadcast_signal_new(self, signal: dict):
        self.broadcast_sync({
            "type": "signal:new",
            "timestamp": time.time(),
            "data": _sanitize(signal),
        })

    def broadcast_signal_closed(self, signal_id: int, exit_reason: str = "", exit_price: Optional[float] = None):
        self.broadcast_sync({
            "type": "signal:closed",
            "timestamp": time.time(),
            "data": {
                "id": signal_id,
                "exit_reason": exit_reason,
                "exit_price": exit_price,
            },
        })

    @property
    def client_count(self) -> int:
        return len(self._clients)


def _sanitize(sig: dict) -> dict:
    safe_keys = {
        "id", "strategy_name", "asset", "direction",
        "entry_price", "stop_loss", "take_profit",
        "status", "signal_timestamp", "created_at",
    }
    return {k: v for k, v in sig.items() if k in safe_keys}


broadcaster = SignalBroadcaster()
