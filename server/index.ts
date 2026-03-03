import { spawn, ChildProcess } from "child_process";
import express, { type Request, Response, NextFunction } from "express";
import helmet from "helmet";
import { registerRoutes } from "./routes";
import { serveStatic } from "./static";
import { createServer } from "http";
import { createConnection } from "net";

process.on("SIGHUP", () => {});

const app = express();
const httpServer = createServer(app);

app.use(
  helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
  }),
);

declare module "http" {
  interface IncomingMessage {
    rawBody: unknown;
  }
}

app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  }),
);

app.use(express.urlencoded({ extended: false }));

export function log(message: string, source = "express") {
  const formattedTime = new Date().toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  });

  console.log(`${formattedTime} [${source}] ${message}`);
}

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      log(logLine);
    }
  });

  next();
});

let pythonProcess: ChildProcess | null = null;

async function waitForEngineReady(maxRetries = 20, delayMs = 500): Promise<boolean> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const res = await fetch("http://127.0.0.1:5001/");
      if (res.ok) return true;
    } catch {}
    await new Promise((r) => setTimeout(r, delayMs));
  }
  return false;
}

let engineRestarting = false;

function spawnPythonProcess(): ChildProcess {
  const pyProc = spawn("setsid", ["python", "-m", "trading_engine.main"], {
    env: { ...process.env, PYTHON_ENGINE_PORT: "5001" },
    stdio: ["pipe", "pipe", "pipe"],
  });

  pyProc.stdout?.on("data", (data: Buffer) => {
    const msg = data.toString().trim();
    if (msg) log(msg, "python-engine");
  });

  pyProc.stderr?.on("data", (data: Buffer) => {
    const msg = data.toString().trim();
    if (msg) log(msg, "python-engine");
  });

  pyProc.on("error", (err) => {
    log(`Python engine failed to start: ${err.message}`, "python-engine");
  });

  pyProc.on("close", (code) => {
    log(`Python engine exited with code ${code}`, "python-engine");
    pythonProcess = null;
    if (!engineRestarting) {
      engineRestarting = true;
      log("Auto-restarting Python engine in 2s...", "python-engine");
      setTimeout(async () => {
        try {
          await startPythonEngine();
          log("Python engine restarted successfully", "python-engine");
        } catch (err: any) {
          log(`Python engine restart failed: ${err.message}`, "python-engine");
        }
        engineRestarting = false;
      }, 2000);
    }
  });

  return pyProc;
}

function startPythonEngine(): Promise<void> {
  return new Promise((resolve) => {
    pythonProcess = spawnPythonProcess();

    waitForEngineReady().then((ready) => {
      if (ready) {
        log("Python engine health check passed", "python-engine");
        resolve();
      } else {
        log("Python engine health check timed out, continuing anyway", "python-engine");
        resolve();
      }
    });
  });
}

const PYTHON_ENGINE_URL = "http://127.0.0.1:5001";

for (const docPath of ["/docs", "/redoc", "/openapi.json"]) {
  app.use(docPath, async (req: Request, res: Response) => {
    try {
      const url = `${PYTHON_ENGINE_URL}${docPath}${req.url === "/" ? "" : req.url}`;
      const response = await fetch(url);
      const contentType = response.headers.get("content-type") || "text/html";
      res.status(response.status).set("content-type", contentType);
      const body = await response.text();
      res.send(body);
    } catch {
      res.status(502).json({ error: "Trading engine unavailable" });
    }
  });
}

app.use("/api/v1", async (req: Request, res: Response) => {
  try {
    const targetPath = `/api/v1${req.path || "/"}`;
    const queryString = new URLSearchParams(req.query as Record<string, string>).toString();
    const url = `${PYTHON_ENGINE_URL}${targetPath}${queryString ? "?" + queryString : ""}`;

    const fetchOptions: RequestInit = {
      method: req.method,
      headers: { "content-type": "application/json" },
    };

    if (req.method !== "GET" && req.method !== "HEAD" && req.body) {
      fetchOptions.body = JSON.stringify(req.body);
    }

    const response = await fetch(url, fetchOptions);
    const contentType = response.headers.get("content-type") || "application/json";
    res.status(response.status).set("content-type", contentType);
    const body = await response.text();
    res.send(body);
  } catch (error) {
    console.error("[v1-proxy] Error:", error);
    res.status(502).json({ error: "Trading engine unavailable" });
  }
});

app.use("/api/engine", async (req: Request, res: Response) => {
  try {
    const targetPath = req.path || "/";
    const queryString = new URLSearchParams(req.query as Record<string, string>).toString();
    const url = `${PYTHON_ENGINE_URL}${targetPath}${queryString ? "?" + queryString : ""}`;

    const headers: Record<string, string> = {};

    if (req.headers.cookie) {
      headers["cookie"] = req.headers.cookie;
    }

    const isFormPost = (req.headers["content-type"] || "").includes("application/x-www-form-urlencoded");

    if (isFormPost) {
      headers["content-type"] = "application/x-www-form-urlencoded";
    } else {
      headers["content-type"] = "application/json";
    }

    const fetchOptions: RequestInit = {
      method: req.method,
      headers,
      redirect: "manual",
    };

    if (req.method !== "GET" && req.method !== "HEAD") {
      if (isFormPost) {
        const params = new URLSearchParams();
        for (const [key, val] of Object.entries(req.body)) {
          params.append(key, String(val));
        }
        fetchOptions.body = params.toString();
      } else {
        fetchOptions.body = JSON.stringify(req.body);
      }
    }

    const response = await fetch(url, fetchOptions);
    const contentType = response.headers.get("content-type") || "";

    const setCookies = response.headers.getSetCookie?.() || [];
    for (const cookie of setCookies) {
      res.appendHeader("set-cookie", cookie);
    }

    if (response.status >= 300 && response.status < 400) {
      let location = response.headers.get("location") || "/api/engine/admin/";
      try {
        const parsed = new URL(location);
        location = parsed.pathname + parsed.search;
      } catch {}
      if (location.startsWith("/admin")) {
        location = "/api/engine" + location;
      }
      return res.redirect(response.status, location);
    }

    if (contentType.includes("text/html")) {
      const html = await response.text();
      res.status(response.status).type("html").send(html);
    } else if (contentType.includes("text/csv") || contentType.includes("application/json" ) && response.headers.get("content-disposition")) {
      const buffer = Buffer.from(await response.arrayBuffer());
      const disposition = response.headers.get("content-disposition");
      if (disposition) res.setHeader("Content-Disposition", disposition);
      res.status(response.status).type(contentType).send(buffer);
    } else {
      const data = await response.json();
      res.status(response.status).json(data);
    }
  } catch (error: any) {
    res.status(502).json({
      error: "Python trading engine unavailable",
      detail: error.message,
    });
  }
});

app.get("/api/engine-status", (_req: Request, res: Response) => {
  res.json({
    running: pythonProcess !== null && !pythonProcess.killed,
    pid: pythonProcess?.pid || null,
  });
});

(async () => {
  const { seedDatabase } = await import("./seed");
  await seedDatabase();

  try {
    log("Starting Python trading engine...", "python-engine");
    await startPythonEngine();
    log("Python trading engine started on port 5001", "python-engine");
  } catch (err: any) {
    log(`Warning: Python trading engine failed to start: ${err.message}`, "python-engine");
  }

  await registerRoutes(httpServer, app);

  app.use((err: any, _req: Request, res: Response, next: NextFunction) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";

    console.error("Internal Server Error:", err);

    if (res.headersSent) {
      return next(err);
    }

    return res.status(status).json({ message });
  });

  if (process.env.NODE_ENV === "production") {
    serveStatic(app);
  } else {
    const { setupVite } = await import("./vite");
    await setupVite(httpServer, app);
  }

  httpServer.on("upgrade", (req, socket, head) => {
    if (req.url === "/ws/signals") {
      const proxy = createConnection(5001, "127.0.0.1", () => {
        const rawHeaders = Object.entries(req.headers)
          .filter(([, v]) => v !== undefined)
          .map(([k, v]) => `${k}: ${v}`)
          .join("\r\n");
        proxy.write(
          `${req.method} ${req.url} HTTP/${req.httpVersion}\r\n${rawHeaders}\r\n\r\n`,
        );
        if (head && head.length) proxy.write(head);
        socket.pipe(proxy).pipe(socket);
      });
      proxy.on("error", () => {
        socket.end();
      });
      socket.on("error", () => {
        proxy.end();
      });
    }
  });

  const port = parseInt(process.env.PORT || "5000", 10);
  httpServer.listen(
    {
      port,
      host: "0.0.0.0",
      reusePort: true,
    },
    () => {
      log(`serving on port ${port}`);
    },
  );
})();

function killPythonEngine() {
  if (pythonProcess && pythonProcess.pid) {
    try {
      process.kill(-pythonProcess.pid, "SIGTERM");
    } catch {
      try { pythonProcess.kill(); } catch {}
    }
  }
}

process.on("exit", killPythonEngine);

process.on("SIGTERM", () => {
  engineRestarting = true;
  killPythonEngine();
  process.exit(0);
});
