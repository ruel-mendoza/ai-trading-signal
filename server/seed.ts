import { db } from "./db";
import { signals } from "@shared/schema";

export async function seedDatabase() {
  const existing = await db.select().from(signals);
  if (existing.length > 0) return;

  const seedSignals = [
    {
      pair: "EUR/USD",
      category: "forex",
      direction: "Buy",
      entryPrice: 1.0852,
      stopLoss: 1.0785,
      takeProfit: 1.0960,
      status: "active",
      confidence: 84,
      analysis:
        "EUR/USD is showing strong bullish momentum after bouncing off the 1.0800 support level. The 50-day moving average has crossed above the 200-day MA, forming a golden cross that typically signals sustained upward movement. RSI sits at 58, leaving ample room for further gains before reaching overbought territory.\n\nThe pair has been consolidating between 1.0825 and 1.0865 over recent sessions, indicating accumulation ahead of a potential breakout. Improving Eurozone PMI data provides fundamental tailwind for the euro. The take profit at 1.0960 aligns with a key resistance zone established by prior price action highs.",
      shortSummary: "Bullish golden cross with support bounce targeting 1.0960 resistance.",
    },
    {
      pair: "GBP/USD",
      category: "forex",
      direction: "Sell",
      entryPrice: 1.2685,
      stopLoss: 1.2755,
      takeProfit: 1.2545,
      status: "active",
      confidence: 77,
      analysis:
        "GBP/USD has stalled at the 1.2700 resistance level with three failed breakout attempts on the 4-hour chart. A descending triangle pattern is forming, which typically resolves to the downside. MACD is showing bearish divergence as price prints higher highs while the histogram makes lower highs.\n\nMixed UK inflation data and dovish Bank of England commentary are weighing on the pound. Relative USD strength from firmer Fed rate guidance adds downward pressure. The stop loss sits above the recent swing high at 1.2755, while the take profit targets the lower triangle boundary near 1.2545.",
      shortSummary: "Descending triangle at resistance with bearish MACD divergence.",
    },
    {
      pair: "BTC/USD",
      category: "crypto",
      direction: "Buy",
      entryPrice: 68200.0,
      stopLoss: 65400.0,
      takeProfit: 73500.0,
      status: "active",
      confidence: 72,
      analysis:
        "Bitcoin is consolidating above the critical 65,000 support after a healthy correction from recent highs. The weekly chart displays a bullish engulfing pattern, signaling strong buyer interest. On-chain data shows long-term holder accumulation increasing with exchange outflows at a 3-month high.\n\nHash rate continues climbing, reflecting robust network fundamentals. Institutional inflows into spot ETFs remain steady, providing a demand floor. The 0.618 Fibonacci retracement from the recent swing sits near 64,500, reinforcing stop loss placement. The 73,500 target marks the next major resistance cluster.",
      shortSummary: "Accumulation above 65K support with bullish weekly engulfing pattern.",
    },
    {
      pair: "XAU/USD",
      category: "commodities",
      direction: "Buy",
      entryPrice: 2345.0,
      stopLoss: 2312.0,
      takeProfit: 2405.0,
      status: "active",
      confidence: 89,
      analysis:
        "Gold continues its strong uptrend driven by geopolitical uncertainty and sustained central bank buying. XAU/USD has found solid support at 2320, which coincides with the 21-day exponential moving average. Bollinger Bands are expanding, indicating rising volatility that favors the prevailing bullish trend.\n\nSafe-haven demand remains elevated amid global tensions and growing expectations of rate cuts later this year. Gold ETF holdings have increased for three consecutive weeks. The ascending channel formation points to 2405 as the upper boundary target, which also aligns with a psychological resistance level.",
      shortSummary: "Strong uptrend with ascending channel targeting 2405 resistance.",
    },
    {
      pair: "USD/JPY",
      category: "forex",
      direction: "Sell",
      entryPrice: 154.85,
      stopLoss: 155.90,
      takeProfit: 152.40,
      status: "active",
      confidence: 80,
      analysis:
        "USD/JPY is approaching overbought territory with daily RSI at 72 as the pair tests the upper boundary of a rising wedge near 155.00. Rising wedge patterns often precede bearish reversals, especially when confirmed by momentum divergence. The stochastic oscillator has already crossed bearishly in the overbought zone.\n\nJapanese officials have escalated verbal intervention warnings as the yen weakens past 154, raising the risk of direct market intervention. Interest rate differentials are expected to narrow as the Bank of Japan signals potential policy normalization. The take profit at 152.40 targets the lower wedge trendline.",
      shortSummary: "Rising wedge with overbought RSI near intervention warning levels.",
    },
    {
      pair: "ETH/USD",
      category: "crypto",
      direction: "Sell",
      entryPrice: 3480.0,
      stopLoss: 3620.0,
      takeProfit: 3180.0,
      status: "closed",
      confidence: 66,
      analysis:
        "Ethereum is underperforming Bitcoin with the ETH/BTC ratio at multi-month lows. ETH/USD has broken below its 50-day moving average at 3500 and is retesting that level as resistance. Volume profile shows a high-volume node near 3200, which could act as a price magnet.\n\nNetwork activity has slowed with gas fees at multi-month lows, reflecting reduced demand for block space. Continued Layer 2 adoption diverts mainnet activity. The MACD histogram has turned negative while the pair trades below key moving averages, favoring sellers in the near term.",
      shortSummary: "Break below 50-day MA with weak ETH/BTC ratio signals downside.",
    },
    {
      pair: "WTI/USD",
      category: "commodities",
      direction: "Buy",
      entryPrice: 78.40,
      stopLoss: 76.50,
      takeProfit: 82.00,
      status: "expired",
      confidence: 73,
      analysis:
        "WTI crude bounced off the 77.00 support level, which has been tested three times without breaking, forming a triple bottom. Bullish volume divergence — increasing on rallies and declining on pullbacks — confirms accumulation at this level.\n\nOPEC+ production cuts continue tightening supply, while seasonal demand trends point higher heading into peak driving months. Three consecutive weekly EIA inventory draws add to the bullish case. The 82.00 target aligns with February highs and a key Fibonacci extension level.",
      shortSummary: "Triple bottom at 77.00 with OPEC+ supply tightening supporting rally.",
    },
  ];

  await db.insert(signals).values(seedSignals);
  console.log("Database seeded with 7 initial trading signals");
}
