import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import OpenAI from "openai";
import { insertSignalSchema } from "@shared/schema";
import { fetchFCSCandles, analyzeCandles } from "./fcs";

const openai = new OpenAI({
  apiKey: process.env.AI_INTEGRATIONS_OPENAI_API_KEY,
  baseURL: process.env.AI_INTEGRATIONS_OPENAI_BASE_URL,
});

const TRADING_PAIRS = [
  { pair: "EUR/USD", category: "forex" },
  { pair: "GBP/USD", category: "forex" },
  { pair: "USD/JPY", category: "forex" },
  { pair: "USD/CAD", category: "forex" },
  { pair: "AUD/USD", category: "forex" },
  { pair: "NZD/USD", category: "forex" },
  { pair: "USD/CHF", category: "forex" },
  { pair: "EUR/GBP", category: "forex" },
  { pair: "BTC/USD", category: "crypto" },
  { pair: "ETH/USD", category: "crypto" },
  { pair: "SOL/USD", category: "crypto" },
  { pair: "XAU/USD", category: "commodities" },
  { pair: "XAG/USD", category: "commodities" },
  { pair: "WTI/USD", category: "commodities" },
];

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  app.get("/api/signals", async (req, res) => {
    try {
      const { category } = req.query;
      const results =
        category && category !== "all"
          ? await storage.getSignalsByCategory(category as string)
          : await storage.getAllSignals();
      res.json(results);
    } catch (error) {
      console.error("Error fetching signals:", error);
      res.status(500).json({ error: "Failed to fetch signals" });
    }
  });

  app.get("/api/signals/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      if (isNaN(id)) return res.status(400).json({ error: "Invalid signal ID" });
      const signal = await storage.getSignalById(id);
      if (!signal) return res.status(404).json({ error: "Signal not found" });
      res.json(signal);
    } catch (error) {
      console.error("Error fetching signal:", error);
      res.status(500).json({ error: "Failed to fetch signal" });
    }
  });

  app.post("/api/signals/generate", async (req, res) => {
    try {
      const { pair } = req.body;
      const pairInfo = TRADING_PAIRS.find((p) => p.pair === pair);
      if (!pairInfo) return res.status(400).json({ error: "Invalid trading pair" });

      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("Connection", "keep-alive");

      const sendEvent = (data: object) => {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
      };

      sendEvent({ type: "status", message: "Analyzing market conditions..." });

      const response = await openai.chat.completions.create({
        model: "gpt-5-mini",
        messages: [
          {
            role: "system",
            content: `You are a professional trading analyst. Generate a realistic trading signal for the given pair. Respond with valid JSON only, no markdown or code blocks.

JSON structure:
{
  "direction": "Buy" or "Sell",
  "entryPrice": number,
  "stopLoss": number,
  "takeProfit": number,
  "confidence": number (60-95),
  "analysis": "2-3 paragraph technical analysis with support/resistance levels, indicators, and market conditions.",
  "shortSummary": "One sentence summary of the signal."
}`,
          },
          {
            role: "user",
            content: `Generate a trading signal for ${pair}. Use realistic current market prices. Today is ${new Date().toISOString().split("T")[0]}.`,
          },
        ],
        max_completion_tokens: 1024,
      });

      sendEvent({ type: "status", message: "Building signal..." });

      const content = response.choices[0]?.message?.content || "";
      let signalData;
      try {
        signalData = JSON.parse(content);
      } catch {
        const jsonMatch = content.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          signalData = JSON.parse(jsonMatch[0]);
        } else {
          throw new Error("Failed to parse AI response");
        }
      }

      const parsed = insertSignalSchema.parse({
        pair: pairInfo.pair,
        category: pairInfo.category,
        direction: signalData.direction,
        entryPrice: signalData.entryPrice,
        stopLoss: signalData.stopLoss,
        takeProfit: signalData.takeProfit,
        confidence: signalData.confidence || 75,
        analysis: signalData.analysis,
        shortSummary: signalData.shortSummary,
        status: "active",
      });

      const created = await storage.createSignal(parsed);
      sendEvent({ type: "complete", signal: created });
      res.end();
    } catch (error) {
      console.error("Error generating signal:", error);
      if (res.headersSent) {
        res.write(`data: ${JSON.stringify({ type: "error", message: "Failed to generate signal" })}\n\n`);
        res.end();
      } else {
        res.status(500).json({ error: "Failed to generate signal" });
      }
    }
  });

  app.patch("/api/signals/:id/status", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      if (isNaN(id)) return res.status(400).json({ error: "Invalid signal ID" });
      const { status } = req.body;
      if (!["active", "closed", "expired"].includes(status)) {
        return res.status(400).json({ error: "Invalid status" });
      }
      const updated = await storage.updateSignalStatus(id, status);
      if (!updated) return res.status(404).json({ error: "Signal not found" });
      res.json(updated);
    } catch (error) {
      console.error("Error updating signal:", error);
      res.status(500).json({ error: "Failed to update signal" });
    }
  });

  app.get("/api/pairs", (_req, res) => {
    res.json(TRADING_PAIRS);
  });

  app.get("/api/analysis", async (_req, res) => {
    try {
      const candles = await fetchFCSCandles(6);
      const analysis = analyzeCandles(candles);
      res.json({
        ...analysis,
        candles: analysis.candles.slice(-50),
        tokyoSessionCandles: analysis.tokyoSessionCandles,
        nySessionCandles: analysis.nySessionCandles,
      });
    } catch (error) {
      console.error("Error fetching analysis:", error);
      res.status(500).json({ error: "Failed to fetch analysis data" });
    }
  });

  return httpServer;
}
