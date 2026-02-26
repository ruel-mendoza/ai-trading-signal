import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import OpenAI from "openai";
import { insertSignalSchema } from "@shared/schema";

const openai = new OpenAI({
  apiKey: process.env.AI_INTEGRATIONS_OPENAI_API_KEY,
  baseURL: process.env.AI_INTEGRATIONS_OPENAI_BASE_URL,
});

const FOREX_PAIRS = [
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
      let results;
      if (category && category !== "all") {
        results = await storage.getSignalsByCategory(category as string);
      } else {
        results = await storage.getAllSignals();
      }
      res.json(results);
    } catch (error) {
      console.error("Error fetching signals:", error);
      res.status(500).json({ error: "Failed to fetch signals" });
    }
  });

  app.get("/api/signals/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const signal = await storage.getSignalById(id);
      if (!signal) {
        return res.status(404).json({ error: "Signal not found" });
      }
      res.json(signal);
    } catch (error) {
      console.error("Error fetching signal:", error);
      res.status(500).json({ error: "Failed to fetch signal" });
    }
  });

  app.post("/api/signals/generate", async (req, res) => {
    try {
      const { pair } = req.body;
      const pairInfo = FOREX_PAIRS.find(p => p.pair === pair);
      if (!pairInfo) {
        return res.status(400).json({ error: "Invalid trading pair" });
      }

      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("Connection", "keep-alive");

      res.write(`data: ${JSON.stringify({ type: "status", message: "Analyzing market conditions..." })}\n\n`);

      const response = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: `You are a professional trading analyst covering forex, crypto, and commodities. Generate a realistic trading signal for the given pair. You MUST choose either "Buy" or "Sell" based on your technical analysis — do NOT default to Buy. Bearish setups should produce Sell signals.

Respond with ONLY valid JSON (no markdown, no code blocks, no extra text):
{
  "direction": "Buy" or "Sell",
  "entryPrice": number (realistic current market price),
  "stopLoss": number (realistic stop loss level),
  "takeProfit": number (realistic take profit level),
  "confidence": number (60-95),
  "analysis": "A detailed 2-3 paragraph technical analysis explaining the signal reasoning, mentioning key support/resistance levels, indicators, and market conditions.",
  "shortSummary": "A concise one-sentence summary of the signal."
}`
          },
          {
            role: "user",
            content: `Generate a trading signal for ${pair}. Analyze whether the setup is bullish (Buy) or bearish (Sell) based on technical indicators and price action. Today's date is ${new Date().toISOString().split('T')[0]}.`
          }
        ],
        max_completion_tokens: 1024,
      });

      const content = response.choices[0]?.message?.content || "";
      let signalData;
      try {
        const cleaned = content.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
        signalData = JSON.parse(cleaned);
      } catch {
        const jsonMatch = content.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          signalData = JSON.parse(jsonMatch[0]);
        } else {
          console.error("AI response was not valid JSON:", content);
          throw new Error("Failed to parse AI response");
        }
      }

      if (!["Buy", "Sell"].includes(signalData.direction)) {
        signalData.direction = signalData.direction?.toLowerCase() === "sell" ? "Sell" : "Buy";
      }

      res.write(`data: ${JSON.stringify({ type: "status", message: "Generating signal..." })}\n\n`);

      const signalInsert = {
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
      };

      const parsed = insertSignalSchema.parse(signalInsert);
      const created = await storage.createSignal(parsed);

      res.write(`data: ${JSON.stringify({ type: "complete", signal: created })}\n\n`);
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
      const { status } = req.body;
      if (!["active", "closed", "expired"].includes(status)) {
        return res.status(400).json({ error: "Invalid status" });
      }
      const updated = await storage.updateSignalStatus(id, status);
      if (!updated) {
        return res.status(404).json({ error: "Signal not found" });
      }
      res.json(updated);
    } catch (error) {
      console.error("Error updating signal:", error);
      res.status(500).json({ error: "Failed to update signal" });
    }
  });

  app.get("/api/pairs", (_req, res) => {
    res.json(FOREX_PAIRS);
  });

  return httpServer;
}
