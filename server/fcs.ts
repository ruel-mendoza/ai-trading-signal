interface FCSCandle {
  o: string;
  h: string;
  l: string;
  c: string;
  v: string;
  vw: string;
  t: string;
  tm: string;
}

export interface Candle {
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  timestamp: number;
  time: string;
}

const FCS_BASE_URL = "https://api-v4.fcsapi.com/forex/history";

export async function fetchFCSCandles(pages: number = 6): Promise<Candle[]> {
  const apiKey = process.env.FCS_API_KEY;
  const symbol = process.env.FCS_SYMBOL || "EURUSD";

  if (!apiKey) throw new Error("FCS_API_KEY not set");

  const allCandles: Candle[] = [];

  for (let page = 1; page <= pages; page++) {
    const url = `${FCS_BASE_URL}?symbol=${symbol}&period=1h&access_key=${apiKey}&page=${page}&per_page=20`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`FCS API error: ${res.status}`);

    const data = await res.json();
    if (data.code !== 200 || !data.response) {
      console.error("FCS API response error:", data.msg);
      break;
    }

    const response = data.response as Record<string, FCSCandle>;
    for (const [, candle] of Object.entries(response)) {
      allCandles.push({
        open: parseFloat(candle.o),
        high: parseFloat(candle.h),
        low: parseFloat(candle.l),
        close: parseFloat(candle.c),
        volume: parseFloat(candle.v),
        timestamp: parseInt(candle.t),
        time: candle.tm,
      });
    }
  }

  allCandles.sort((a, b) => a.timestamp - b.timestamp);
  return allCandles;
}

export function calculateATR(candles: Candle[], period: number = 100): number {
  if (candles.length < 2) return 0;

  const trueRanges: number[] = [];
  for (let i = 1; i < candles.length; i++) {
    const high = candles[i].high;
    const low = candles[i].low;
    const prevClose = candles[i - 1].close;
    const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
    trueRanges.push(tr);
  }

  const lookback = Math.min(period, trueRanges.length);
  const recentTRs = trueRanges.slice(-lookback);
  return recentTRs.reduce((sum, tr) => sum + tr, 0) / recentTRs.length;
}

function getETDate(timestamp: number): { year: number; month: number; day: number; hour: number; dayOfWeek: number } {
  const date = new Date(timestamp * 1000);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hour12: false,
    weekday: "short",
  }).formatToParts(date);

  const vals: Record<string, string> = {};
  for (const p of parts) vals[p.type] = p.value;

  const dayMap: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

  return {
    year: parseInt(vals.year),
    month: parseInt(vals.month),
    day: parseInt(vals.day),
    hour: parseInt(vals.hour),
    dayOfWeek: dayMap[vals.weekday] ?? 0,
  };
}

function getJSTDate(timestamp: number): { year: number; month: number; day: number; hour: number } {
  const date = new Date(timestamp * 1000);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const vals: Record<string, string> = {};
  for (const p of parts) vals[p.type] = p.value;

  return {
    year: parseInt(vals.year),
    month: parseInt(vals.month),
    day: parseInt(vals.day),
    hour: parseInt(vals.hour),
  };
}

function getTokyo8amTimestamp(referenceTimestamp: number): number {
  const jst = getJSTDate(referenceTimestamp);
  const et = getETDate(referenceTimestamp);

  let targetJSTDay = jst.day;
  if (jst.hour < 8) {
    targetJSTDay = jst.day - 1;
  }

  const tokyo8amLocal = new Date(
    Date.UTC(jst.year, jst.month - 1, targetJSTDay, 8 - 9, 0, 0)
  );
  return Math.floor(tokyo8amLocal.getTime() / 1000);
}

function getNY8amTimestamp(referenceTimestamp: number): number {
  const et = getETDate(referenceTimestamp);

  const ny8amStr = `${et.year}-${String(et.month).padStart(2, '0')}-${String(et.day).padStart(2, '0')}T08:00:00`;
  const utcGuess = new Date(ny8amStr + "Z");
  const etOffset = getETOffsetHours(utcGuess);
  const ny8amUTC = new Date(utcGuess.getTime() + etOffset * 3600000);
  return Math.floor(ny8amUTC.getTime() / 1000);
}

function getETOffsetHours(date: Date): number {
  const utcStr = date.toLocaleString("en-US", { timeZone: "UTC", hour12: false });
  const etStr = date.toLocaleString("en-US", { timeZone: "America/New_York", hour12: false });

  const utcDate = new Date(utcStr);
  const etDate = new Date(etStr);
  return (utcDate.getTime() - etDate.getTime()) / 3600000;
}

export interface AnalysisResult {
  candles: Candle[];
  highestClose: number | null;
  lowestClose: number | null;
  highestCloseTime: string | null;
  lowestCloseTime: string | null;
  atr100: number;
  trailingStop: number;
  tokyoSessionCandles: Candle[];
  nySessionCandles: Candle[];
  signal: TradeSignal | null;
  previousDayHigh: number | null;
  previousDayLow: number | null;
  currentPrice: number | null;
}

export interface TradeSignal {
  direction: "LONG" | "SHORT";
  entryPrice: number;
  trailingStop: number;
  atrAtEntry: number;
  reason: string;
  signalTime: string;
  rules: {
    rule1: boolean;
    rule2: boolean;
    rule3: boolean;
    rule1Detail: string;
    rule2Detail: string;
    rule3Detail: string;
  };
}

export function analyzeCandles(candles: Candle[]): AnalysisResult {
  if (candles.length === 0) {
    return {
      candles: [],
      highestClose: null,
      lowestClose: null,
      highestCloseTime: null,
      lowestCloseTime: null,
      atr100: 0,
      trailingStop: 0,
      tokyoSessionCandles: [],
      nySessionCandles: [],
      signal: null,
      previousDayHigh: null,
      previousDayLow: null,
      currentPrice: null,
    };
  }

  const atr100 = calculateATR(candles, 100);
  const trailingStopDistance = 0.25 * atr100;

  const latestCandle = candles[candles.length - 1];
  const latestET = getETDate(latestCandle.timestamp);

  const tokyo8amTs = getTokyo8amTimestamp(latestCandle.timestamp);
  const ny8amTs = getNY8amTimestamp(latestCandle.timestamp);

  const tokyoSessionCandles: Candle[] = [];
  const nySessionCandles: Candle[] = [];
  const candlesSinceTokyo8am: Candle[] = [];

  for (const c of candles) {
    if (c.timestamp >= tokyo8amTs) {
      tokyoSessionCandles.push(c);
      candlesSinceTokyo8am.push(c);
    }

    if (c.timestamp >= ny8amTs) {
      nySessionCandles.push(c);
    }
  }

  let highestClose: number | null = null;
  let lowestClose: number | null = null;
  let highestCloseTime: string | null = null;
  let lowestCloseTime: string | null = null;

  for (const c of tokyoSessionCandles) {
    if (highestClose === null || c.close > highestClose) {
      highestClose = c.close;
      highestCloseTime = c.time;
    }
    if (lowestClose === null || c.close < lowestClose) {
      lowestClose = c.close;
      lowestCloseTime = c.time;
    }
  }

  const previousDayCandles = getPreviousTradingDayCandles(candles, latestCandle.timestamp);
  let previousDayHigh: number | null = null;
  let previousDayLow: number | null = null;

  if (previousDayCandles.length > 0) {
    previousDayHigh = Math.max(...previousDayCandles.map((c) => c.high));
    previousDayLow = Math.min(...previousDayCandles.map((c) => c.low));
  }

  const signal = checkEntrySignal(
    nySessionCandles,
    candlesSinceTokyo8am,
    highestClose,
    lowestClose,
    previousDayHigh,
    previousDayLow,
    atr100
  );

  return {
    candles,
    highestClose,
    lowestClose,
    highestCloseTime,
    lowestCloseTime,
    atr100,
    trailingStop: trailingStopDistance,
    tokyoSessionCandles,
    nySessionCandles,
    signal,
    previousDayHigh,
    previousDayLow,
    currentPrice: latestCandle.close,
  };
}

function getPreviousTradingDayCandles(candles: Candle[], currentTimestamp: number): Candle[] {
  const currentET = getETDate(currentTimestamp);
  const currentKey = `${currentET.year}-${currentET.month}-${currentET.day}`;

  const prevDayCandles: Candle[] = [];
  let prevDayKey: string | null = null;

  for (let i = candles.length - 1; i >= 0; i--) {
    const et = getETDate(candles[i].timestamp);
    const key = `${et.year}-${et.month}-${et.day}`;

    if (key === currentKey) continue;

    if (et.dayOfWeek >= 1 && et.dayOfWeek <= 5) {
      if (prevDayKey === null) {
        prevDayKey = key;
      }
      if (key === prevDayKey) {
        prevDayCandles.push(candles[i]);
      } else if (prevDayKey !== null) {
        break;
      }
    }
  }

  return prevDayCandles;
}

function checkEntrySignal(
  nyCandles: Candle[],
  allCandlesSinceTokyo8am: Candle[],
  highestClose: number | null,
  lowestClose: number | null,
  prevDayHigh: number | null,
  prevDayLow: number | null,
  atr100: number
): TradeSignal | null {
  if (nyCandles.length < 2 || highestClose === null || lowestClose === null) {
    return null;
  }

  for (let i = 1; i < nyCandles.length; i++) {
    const currentCandle = nyCandles[i];
    const et = getETDate(currentCandle.timestamp);

    if (et.hour !== 9 && et.hour !== 10) continue;
    if (et.dayOfWeek < 1 || et.dayOfWeek > 5) continue;

    const candlesBeforeCurrent = allCandlesSinceTokyo8am.filter(
      (c) => c.timestamp < currentCandle.timestamp
    );

    const longSignal = checkLongEntry(currentCandle, candlesBeforeCurrent, lowestClose, prevDayLow, atr100);
    if (longSignal) return longSignal;

    const shortSignal = checkShortEntry(currentCandle, candlesBeforeCurrent, highestClose, prevDayHigh, atr100);
    if (shortSignal) return shortSignal;
  }

  return null;
}

function checkLongEntry(
  currentCandle: Candle,
  candlesSinceTokyo8am: Candle[],
  lowestClose: number,
  prevDayLow: number | null,
  atr100: number
): TradeSignal | null {
  let wentBelow = false;
  for (const c of candlesSinceTokyo8am) {
    if (c.low < lowestClose) {
      wentBelow = true;
      break;
    }
  }

  const closedAbove = currentCandle.close > lowestClose;
  const rule1 = wentBelow && closedAbove;

  const rule2 = currentCandle.close > currentCandle.open;

  const rule3 = prevDayLow === null || currentCandle.close >= prevDayLow;

  const rule1Detail = `Price ${wentBelow ? "went below" : "did NOT go below"} lowest close (${lowestClose.toFixed(5)}) since 8am Tokyo, then ${closedAbove ? "closed back above" : "did NOT close above"} it.`;
  const rule2Detail = `Closing candle close (${currentCandle.close.toFixed(5)}) is ${rule2 ? "higher" : "NOT higher"} than open (${currentCandle.open.toFixed(5)}).`;
  const rule3Detail = prevDayLow !== null
    ? `Entry price (${currentCandle.close.toFixed(5)}) is ${rule3 ? "not below" : "below"} previous day low (${prevDayLow.toFixed(5)}).`
    : "Previous day data not available.";

  if (rule1 && rule2 && rule3) {
    const trailingStop = 0.25 * atr100;
    return {
      direction: "LONG",
      entryPrice: currentCandle.close,
      trailingStop,
      atrAtEntry: atr100,
      reason: "All long entry conditions met",
      signalTime: currentCandle.time,
      rules: { rule1, rule2, rule3, rule1Detail, rule2Detail, rule3Detail },
    };
  }

  return null;
}

function checkShortEntry(
  currentCandle: Candle,
  candlesSinceTokyo8am: Candle[],
  highestClose: number,
  prevDayHigh: number | null,
  atr100: number
): TradeSignal | null {
  let wentAbove = false;
  for (const c of candlesSinceTokyo8am) {
    if (c.high > highestClose) {
      wentAbove = true;
      break;
    }
  }

  const closedBelow = currentCandle.close < highestClose;
  const rule1 = wentAbove && closedBelow;

  const rule2 = currentCandle.close < currentCandle.open;

  const rule3 = prevDayHigh === null || currentCandle.close <= prevDayHigh;

  const rule1Detail = `Price ${wentAbove ? "went above" : "did NOT go above"} highest close (${highestClose.toFixed(5)}) since 8am Tokyo, then ${closedBelow ? "closed back below" : "did NOT close below"} it.`;
  const rule2Detail = `Closing candle close (${currentCandle.close.toFixed(5)}) is ${rule2 ? "lower" : "NOT lower"} than open (${currentCandle.open.toFixed(5)}).`;
  const rule3Detail = prevDayHigh !== null
    ? `Entry price (${currentCandle.close.toFixed(5)}) is ${rule3 ? "not above" : "above"} previous day high (${prevDayHigh.toFixed(5)}).`
    : "Previous day data not available.";

  if (rule1 && rule2 && rule3) {
    const trailingStop = 0.25 * atr100;
    return {
      direction: "SHORT",
      entryPrice: currentCandle.close,
      trailingStop,
      atrAtEntry: atr100,
      reason: "All short entry conditions met",
      signalTime: currentCandle.time,
      rules: { rule1, rule2, rule3, rule1Detail, rule2Detail, rule3Detail },
    };
  }

  return null;
}
