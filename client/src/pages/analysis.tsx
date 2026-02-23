import { useQuery } from "@tanstack/react-query";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  TrendingUp,
  TrendingDown,
  Activity,
  ArrowUp,
  ArrowDown,
  RefreshCw,
  Clock,
  BarChart3,
  CheckCircle2,
  XCircle,
  AlertTriangle,
} from "lucide-react";
import { queryClient } from "@/lib/queryClient";

interface Candle {
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  timestamp: number;
  time: string;
}

interface TradeSignal {
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

interface AnalysisData {
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

function RuleCheck({ passed, label, detail }: { passed: boolean; label: string; detail: string }) {
  return (
    <div className="flex items-start gap-3 p-3 rounded-lg bg-card border border-border" data-testid={`rule-${label}`}>
      <div className="mt-0.5">
        {passed ? (
          <CheckCircle2 className="w-4 h-4 text-emerald-500" />
        ) : (
          <XCircle className="w-4 h-4 text-red-500" />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium">{label}</p>
        <p className="text-xs text-muted-foreground mt-0.5">{detail}</p>
      </div>
      <Badge
        variant={passed ? "default" : "destructive"}
        className="no-default-active-elevate text-[10px] shrink-0"
      >
        {passed ? "YES" : "NO"}
      </Badge>
    </div>
  );
}

function CandleTable({ candles, title }: { candles: Candle[]; title: string }) {
  if (candles.length === 0) return null;
  return (
    <div>
      <h3 className="text-sm font-semibold mb-2 text-muted-foreground uppercase tracking-wide">{title}</h3>
      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="w-full text-xs" data-testid={`table-${title.toLowerCase().replace(/\s/g, '-')}`}>
          <thead>
            <tr className="bg-muted/50">
              <th className="text-left p-2 font-medium">Time</th>
              <th className="text-right p-2 font-medium">Open</th>
              <th className="text-right p-2 font-medium">High</th>
              <th className="text-right p-2 font-medium">Low</th>
              <th className="text-right p-2 font-medium">Close</th>
              <th className="text-center p-2 font-medium">Dir</th>
            </tr>
          </thead>
          <tbody>
            {candles.map((c, i) => {
              const bullish = c.close > c.open;
              return (
                <tr key={i} className="border-t border-border hover:bg-muted/30">
                  <td className="p-2 text-muted-foreground whitespace-nowrap">{c.time}</td>
                  <td className="text-right p-2 font-mono">{c.open.toFixed(5)}</td>
                  <td className="text-right p-2 font-mono">{c.high.toFixed(5)}</td>
                  <td className="text-right p-2 font-mono">{c.low.toFixed(5)}</td>
                  <td className={`text-right p-2 font-mono font-semibold ${bullish ? "text-emerald-500" : "text-red-500"}`}>
                    {c.close.toFixed(5)}
                  </td>
                  <td className="text-center p-2">
                    {bullish ? (
                      <ArrowUp className="w-3 h-3 text-emerald-500 inline" />
                    ) : (
                      <ArrowDown className="w-3 h-3 text-red-500 inline" />
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function Analysis() {
  const { data, isLoading, isFetching } = useQuery<AnalysisData>({
    queryKey: ["/api/analysis"],
  });

  const handleRefresh = () => {
    queryClient.invalidateQueries({ queryKey: ["/api/analysis"] });
  };

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b border-border bg-card/80 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between gap-4 h-14">
            <div className="flex items-center gap-2.5">
              <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary text-primary-foreground">
                <Activity className="w-4 h-4" />
              </div>
              <span className="text-base font-semibold tracking-tight" data-testid="text-brand">
                EUR/USD Signal Analyzer
              </span>
            </div>
            <Button
              size="sm"
              variant="outline"
              className="gap-1.5"
              onClick={handleRefresh}
              disabled={isFetching}
              data-testid="button-refresh"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? "animate-spin" : ""}`} />
              Refresh
            </Button>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
        <div className="mb-6">
          <h1 className="text-xl sm:text-2xl font-bold tracking-tight mb-1" data-testid="text-page-title">
            EUR/USD Hourly Analysis
          </h1>
          <p className="text-sm text-muted-foreground max-w-xl">
            Real-time analysis using FCS hourly candle data with entry signal detection based on Tokyo/NY session rules.
          </p>
        </div>

        {isLoading ? (
          <div className="space-y-4">
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              {Array.from({ length: 4 }).map((_, i) => (
                <Skeleton key={i} className="h-20 rounded-lg" />
              ))}
            </div>
            <Skeleton className="h-64 rounded-lg" />
            <Skeleton className="h-48 rounded-lg" />
          </div>
        ) : data ? (
          <div className="space-y-6">
            {/* Key Metrics */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <Card className="p-3" data-testid="card-current-price">
                <div className="flex items-center gap-2 mb-1">
                  <BarChart3 className="w-3.5 h-3.5 text-primary" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase">Current Price</span>
                </div>
                <p className="text-lg font-bold font-mono">
                  {data.currentPrice?.toFixed(5) ?? "N/A"}
                </p>
              </Card>

              <Card className="p-3" data-testid="card-highest-close">
                <div className="flex items-center gap-2 mb-1">
                  <TrendingUp className="w-3.5 h-3.5 text-emerald-500" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase">Highest Close</span>
                </div>
                <p className="text-lg font-bold font-mono text-emerald-500">
                  {data.highestClose?.toFixed(5) ?? "N/A"}
                </p>
                {data.highestCloseTime && (
                  <p className="text-[10px] text-muted-foreground mt-0.5">{data.highestCloseTime}</p>
                )}
              </Card>

              <Card className="p-3" data-testid="card-lowest-close">
                <div className="flex items-center gap-2 mb-1">
                  <TrendingDown className="w-3.5 h-3.5 text-red-500" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase">Lowest Close</span>
                </div>
                <p className="text-lg font-bold font-mono text-red-500">
                  {data.lowestClose?.toFixed(5) ?? "N/A"}
                </p>
                {data.lowestCloseTime && (
                  <p className="text-[10px] text-muted-foreground mt-0.5">{data.lowestCloseTime}</p>
                )}
              </Card>

              <Card className="p-3" data-testid="card-atr">
                <div className="flex items-center gap-2 mb-1">
                  <Activity className="w-3.5 h-3.5 text-primary" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase">ATR(100)</span>
                </div>
                <p className="text-lg font-bold font-mono">
                  {data.atr100.toFixed(5)}
                </p>
                <p className="text-[10px] text-muted-foreground mt-0.5">
                  Trail: {data.trailingStop.toFixed(5)}
                </p>
              </Card>
            </div>

            {/* Previous Day Range */}
            <div className="grid grid-cols-2 gap-3">
              <Card className="p-3" data-testid="card-prev-day-high">
                <div className="flex items-center gap-2 mb-1">
                  <ArrowUp className="w-3.5 h-3.5 text-emerald-500" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase">Prev Day High</span>
                </div>
                <p className="text-base font-bold font-mono">
                  {data.previousDayHigh?.toFixed(5) ?? "N/A"}
                </p>
              </Card>
              <Card className="p-3" data-testid="card-prev-day-low">
                <div className="flex items-center gap-2 mb-1">
                  <ArrowDown className="w-3.5 h-3.5 text-red-500" />
                  <span className="text-[11px] text-muted-foreground font-medium uppercase">Prev Day Low</span>
                </div>
                <p className="text-base font-bold font-mono">
                  {data.previousDayLow?.toFixed(5) ?? "N/A"}
                </p>
              </Card>
            </div>

            {/* Signal Alert */}
            {data.signal ? (
              <Card className={`p-4 border-2 ${data.signal.direction === "LONG" ? "border-emerald-500/50 bg-emerald-500/5" : "border-red-500/50 bg-red-500/5"}`} data-testid="card-signal">
                <div className="flex items-center gap-3 mb-3">
                  <div className={`flex items-center justify-center w-10 h-10 rounded-full ${data.signal.direction === "LONG" ? "bg-emerald-500/20" : "bg-red-500/20"}`}>
                    {data.signal.direction === "LONG" ? (
                      <TrendingUp className="w-5 h-5 text-emerald-500" />
                    ) : (
                      <TrendingDown className="w-5 h-5 text-red-500" />
                    )}
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <h2 className="text-lg font-bold">
                        {data.signal.direction} Signal
                      </h2>
                      <Badge
                        variant="default"
                        className={`no-default-active-elevate ${data.signal.direction === "LONG" ? "bg-emerald-500" : "bg-red-500"}`}
                      >
                        ENTRY @ MARKET
                      </Badge>
                    </div>
                    <p className="text-sm text-muted-foreground">
                      Entry: {data.signal.entryPrice.toFixed(5)} | Trail Stop: {data.signal.trailingStop.toFixed(5)}
                    </p>
                  </div>
                </div>

                <div className="flex items-center gap-2 mb-3">
                  <Clock className="w-3.5 h-3.5 text-muted-foreground" />
                  <span className="text-xs text-muted-foreground">Signal time: {data.signal.signalTime}</span>
                </div>

                <div className="space-y-2">
                  <h3 className="text-sm font-semibold">Entry Rules Check</h3>
                  <RuleCheck
                    passed={data.signal.rules.rule1}
                    label="Rule 1 - Price Action"
                    detail={data.signal.rules.rule1Detail}
                  />
                  <RuleCheck
                    passed={data.signal.rules.rule2}
                    label="Rule 2 - Candle Direction"
                    detail={data.signal.rules.rule2Detail}
                  />
                  <RuleCheck
                    passed={data.signal.rules.rule3}
                    label="Rule 3 - Previous Day Range"
                    detail={data.signal.rules.rule3Detail}
                  />
                </div>

                <div className="mt-3 p-2 rounded-md bg-muted/50">
                  <p className="text-xs text-muted-foreground">
                    ATR(100) at entry: {data.signal.atrAtEntry.toFixed(5)} | Trailing Stop Distance: 0.25 x ATR = {data.signal.trailingStop.toFixed(5)}
                  </p>
                </div>
              </Card>
            ) : (
              <Card className="p-4 border-border" data-testid="card-no-signal">
                <div className="flex items-center gap-3">
                  <div className="flex items-center justify-center w-10 h-10 rounded-full bg-muted">
                    <AlertTriangle className="w-5 h-5 text-muted-foreground" />
                  </div>
                  <div>
                    <h2 className="text-base font-semibold">No Entry Signal</h2>
                    <p className="text-sm text-muted-foreground">
                      Entry conditions for Long or Short are not currently met. Signals are checked at 9am and 10am Eastern on weekdays.
                    </p>
                  </div>
                </div>
              </Card>
            )}

            {/* Session Candles */}
            <div className="grid gap-6 lg:grid-cols-2">
              <CandleTable candles={data.tokyoSessionCandles} title="Tokyo Session (from 8am JST)" />
              <CandleTable candles={data.nySessionCandles} title="NY Session (from 8am ET)" />
            </div>

            {/* Recent Candles */}
            <CandleTable candles={data.candles.slice(-20)} title="Recent Hourly Candles" />

            {/* Info */}
            <Card className="p-4" data-testid="card-rules-summary">
              <h3 className="text-sm font-semibold mb-3">Trading Rules Summary</h3>
              <div className="grid gap-4 sm:grid-cols-2">
                <div>
                  <h4 className="text-xs font-semibold text-emerald-500 uppercase mb-2">Long Entry</h4>
                  <ol className="text-xs text-muted-foreground space-y-1.5 list-decimal list-inside">
                    <li>Price goes below lowest hourly close since 8am Tokyo, then closes back above.</li>
                    <li>Signal candle closes higher than its open (bullish).</li>
                    <li>Entry price is not below the previous trading day low.</li>
                  </ol>
                </div>
                <div>
                  <h4 className="text-xs font-semibold text-red-500 uppercase mb-2">Short Entry</h4>
                  <ol className="text-xs text-muted-foreground space-y-1.5 list-decimal list-inside">
                    <li>Price goes above highest hourly close since 8am Tokyo, then closes back below.</li>
                    <li>Signal candle closes lower than its open (bearish).</li>
                    <li>Entry price is not above the previous trading day high.</li>
                  </ol>
                </div>
              </div>
              <div className="mt-3 pt-3 border-t border-border">
                <p className="text-xs text-muted-foreground">
                  <span className="font-semibold">Exit Rule:</span> Trailing stop of 0.25 x ATR(100). ATR is calculated at entry and fixed for the trade duration.
                  Signals only at 9am or 10am ET on US/Japan business days.
                </p>
              </div>
            </Card>
          </div>
        ) : (
          <Card className="p-8 text-center">
            <AlertTriangle className="w-8 h-8 text-muted-foreground mx-auto mb-3" />
            <h3 className="text-base font-semibold mb-1">Unable to load analysis</h3>
            <p className="text-sm text-muted-foreground mb-4">
              There was an issue fetching the data. Please try again.
            </p>
            <Button onClick={handleRefresh} data-testid="button-retry">
              <RefreshCw className="w-4 h-4 mr-1.5" />
              Retry
            </Button>
          </Card>
        )}
      </main>
    </div>
  );
}
