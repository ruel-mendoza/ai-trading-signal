import { spawn, ChildProcess } from "child_process";
import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { serveStatic } from "./static";
import { createServer } from "http";

const app = express();
const httpServer = createServer(app);

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

function startPythonEngine(): Promise<void> {
  return new Promise((resolve, reject) => {
    const pyProc = spawn("python", ["-m", "trading_engine.main"], {
      env: { ...process.env, PYTHON_ENGINE_PORT: "5001" },
      stdio: ["pipe", "pipe", "pipe"],
    });

    pythonProcess = pyProc;

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
      reject(err);
    });

    pyProc.on("close", (code) => {
      log(`Python engine exited with code ${code}`, "python-engine");
      pythonProcess = null;
    });

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

app.use("/api/engine", async (req: Request, res: Response) => {
  try {
    const targetPath = req.path || "/";
    const queryString = new URLSearchParams(req.query as Record<string, string>).toString();
    const url = `${PYTHON_ENGINE_URL}${targetPath}${queryString ? "?" + queryString : ""}`;

    const fetchOptions: RequestInit = {
      method: req.method,
      headers: { "Content-Type": "application/json" },
    };

    if (req.method !== "GET" && req.method !== "HEAD") {
      fetchOptions.body = JSON.stringify(req.body);
    }

    const response = await fetch(url, fetchOptions);
    const data = await response.json();
    res.status(response.status).json(data);
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

process.on("exit", () => {
  if (pythonProcess) {
    pythonProcess.kill();
  }
});

process.on("SIGTERM", () => {
  if (pythonProcess) {
    pythonProcess.kill();
  }
  process.exit(0);
});
