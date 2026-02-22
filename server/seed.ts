import { db } from "./db";
import { signals } from "@shared/schema";
import { sql } from "drizzle-orm";

export async function seedDatabase() {
  const existing = await db.select().from(signals);
  if (existing.length > 0) return;

  const seedSignals = [
    {
      pair: "EUR/USD",
      category: "forex",
      direction: "Buy",
      entryPrice: 1.0845,
      stopLoss: 1.0780,
      takeProfit: 1.0950,
      status: "active",
      confidence: 82,
      analysis: "The EUR/USD pair is showing strong bullish momentum after bouncing off the key support level at 1.0800. The 50-day moving average has crossed above the 200-day moving average, forming a golden cross pattern that typically signals further upside potential. RSI is currently at 58, indicating room for further gains without being overbought.\n\nThe pair has been consolidating in a tight range between 1.0820-1.0860 over the past few sessions, suggesting accumulation before a potential breakout. Eurozone PMI data has been improving, providing fundamental support for the euro. The target at 1.0950 aligns with a significant resistance zone from previous price action.",
      shortSummary: "Bullish setup as EUR/USD bounces off key support with golden cross confirmation.",
    },
    {
      pair: "GBP/USD",
      category: "forex",
      direction: "Sell",
      entryPrice: 1.2680,
      stopLoss: 1.2750,
      takeProfit: 1.2550,
      status: "active",
      confidence: 76,
      analysis: "GBP/USD has been struggling at the 1.2700 resistance level, with multiple failed attempts to break above. The pair is forming a descending triangle pattern on the 4-hour chart, which is typically a bearish continuation pattern. MACD is showing bearish divergence with price making higher highs while the indicator makes lower highs.\n\nThe UK economic outlook remains challenging with mixed inflation data and dovish Bank of England signals. A stronger US dollar environment supported by Fed hawkishness adds downside pressure. The stop loss at 1.2750 is placed above the recent swing high, while the take profit targets the lower boundary of the descending triangle.",
      shortSummary: "Bearish reversal pattern at resistance with MACD divergence signals further downside.",
    },
    {
      pair: "XAU/USD",
      category: "commodities",
      direction: "Buy",
      entryPrice: 2340.50,
      stopLoss: 2310.00,
      takeProfit: 2400.00,
      status: "active",
      confidence: 88,
      analysis: "Gold continues its strong uptrend, driven by geopolitical uncertainties and central bank buying. The XAU/USD pair has found solid support at the 2320 level, which coincides with the 21-day exponential moving average. The Bollinger Bands are expanding, indicating increasing volatility and momentum in favor of the bulls.\n\nSafe-haven demand remains elevated with ongoing global tensions and expectations of rate cuts later this year. Gold ETF holdings have been increasing for the third consecutive week. The technical picture shows an ascending channel formation with the target at 2400 representing the upper channel boundary and a psychological resistance level.",
      shortSummary: "Gold maintains bullish trend with strong safe-haven demand and ascending channel support.",
    },
    {
      pair: "BTC/USD",
      category: "crypto",
      direction: "Buy",
      entryPrice: 67500.00,
      stopLoss: 64800.00,
      takeProfit: 72000.00,
      status: "active",
      confidence: 71,
      analysis: "Bitcoin is consolidating above the critical 65,000 support level following a healthy correction from recent highs. The weekly chart shows a bullish engulfing pattern, suggesting strong buyer interest at current levels. On-chain metrics indicate that long-term holders are accumulating, with exchange outflows reaching a 3-month high.\n\nThe hash rate continues to climb, reflecting strong network fundamentals. Institutional interest remains robust with steady inflows into spot Bitcoin ETFs. The Fibonacci retracement from the recent swing low to high places the 0.618 level at approximately 64,500, adding confluence to our stop loss placement. The target at 72,000 represents the next major resistance zone.",
      shortSummary: "Bitcoin consolidates above key support with strong on-chain accumulation signals.",
    },
    {
      pair: "USD/JPY",
      category: "forex",
      direction: "Sell",
      entryPrice: 154.80,
      stopLoss: 155.80,
      takeProfit: 152.50,
      status: "active",
      confidence: 79,
      analysis: "USD/JPY is approaching overbought territory on the daily RSI at 72, with the pair testing the upper boundary of a rising wedge pattern near 155.00. This technical formation often precedes a bearish reversal, especially when confirmed by momentum divergence. The stochastic oscillator has already crossed bearishly in the overbought zone.\n\nJapanese officials have ramped up verbal intervention warnings as the yen weakens past the 154 level, increasing the risk of actual market intervention. Interest rate differential narrowing is also expected as the Bank of Japan signals potential policy normalization. The take profit at 152.50 targets the lower trendline of the wedge pattern.",
      shortSummary: "Rising wedge pattern with overbought conditions suggests reversal near JPY intervention levels.",
    },
    {
      pair: "WTI/USD",
      category: "commodities",
      direction: "Buy",
      entryPrice: 78.40,
      stopLoss: 76.50,
      takeProfit: 82.00,
      status: "closed",
      confidence: 73,
      analysis: "WTI crude oil has bounced off the 77.00 support level, which has been tested three times without a breakdown, forming a triple bottom pattern. This bullish reversal setup is supported by increasing volume on upward moves and decreasing volume on pullbacks, a classic sign of accumulation.\n\nOPEC+ production cuts continue to tighten supply conditions, while seasonal demand trends point to increased consumption heading into the driving season. Inventory draws reported by the EIA for three consecutive weeks add to the bullish case. The target at 82.00 aligns with the February highs and a key Fibonacci extension level.",
      shortSummary: "Triple bottom formation on WTI with OPEC+ supply tightening supports bullish outlook.",
    },
    {
      pair: "ETH/USD",
      category: "crypto",
      direction: "Sell",
      entryPrice: 3450.00,
      stopLoss: 3600.00,
      takeProfit: 3150.00,
      status: "expired",
      confidence: 65,
      analysis: "Ethereum is showing weakness relative to Bitcoin with the ETH/BTC ratio declining to multi-month lows. The ETH/USD pair has broken below its 50-day moving average at 3500 and is now testing this level as resistance. Volume profile analysis shows a high-volume node near 3200, suggesting that could act as a magnet for prices.\n\nNetwork activity has slowed with gas fees at multi-month lows, indicating reduced demand for block space. Layer 2 adoption continues to divert activity from the mainnet. The short-term technical picture favors sellers with the pair trading below key moving averages and the MACD histogram turning negative.",
      shortSummary: "ETH weakness against BTC with break below 50-day MA suggests further downside risk.",
    },
  ];

  await db.insert(signals).values(seedSignals);
  console.log("Database seeded with initial signals");
}
