import { useEffect, useRef, useCallback } from "react";
import { useQueryClient } from "@tanstack/react-query";

interface SignalEvent {
  type: "signal:new" | "signal:closed";
  timestamp: number;
  data: Record<string, unknown>;
}

export function useSignalStream() {
  const queryClient = useQueryClient();
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();
  const mountedRef = useRef(true);

  const invalidate = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ["/api/signals"] });
    queryClient.invalidateQueries({ queryKey: ["/api/v1/signals"] });
  }, [queryClient]);

  const connect = useCallback(() => {
    if (!mountedRef.current) return;
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${window.location.host}/ws/signals`);

    ws.onopen = () => {
      if (reconnectTimer.current) {
        clearTimeout(reconnectTimer.current);
        reconnectTimer.current = undefined;
      }
    };

    ws.onmessage = (evt) => {
      try {
        const event: SignalEvent = JSON.parse(evt.data);
        if (event.type === "signal:new" || event.type === "signal:closed") {
          invalidate();
        }
      } catch {
        // ignore malformed messages
      }
    };

    ws.onclose = () => {
      wsRef.current = null;
      if (mountedRef.current) {
        reconnectTimer.current = setTimeout(connect, 5000);
      }
    };

    ws.onerror = () => {
      ws.close();
    };

    wsRef.current = ws;
  }, [invalidate]);

  useEffect(() => {
    mountedRef.current = true;
    connect();
    return () => {
      mountedRef.current = false;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connect]);
}
