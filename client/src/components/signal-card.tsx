import type { Signal } from "@shared/schema";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, TrendingDown, Clock, Percent } from "lucide-react";

function formatPrice(price: number, pair: string): string {
  if (pair.includes("JPY")) return price.toFixed(3);
  if (pair.includes("BTC") || pair.includes("ETH") || pair.includes("SOL"))
    return price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (pair.includes("XAU") || pair.includes("XAG") || pair.includes("WTI"))
    return price.toFixed(2);
  return price.toFixed(5);
}

function timeAgo(date: string | Date): string {
  const now = new Date();
  const d = new Date(date);
  const diffMs = now.getTime() - d.getTime();
  const mins = Math.floor(diffMs / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export function SignalCard({ signal }: { signal: Signal }) {
  const isBuy = signal.direction === "Buy";
  const isActive = signal.status === "active";
  const rr = Math.abs(signal.takeProfit - signal.entryPrice) / Math.abs(signal.entryPrice - signal.stopLoss);

  return (
    <div
      className={`group relative p-4 sm:p-5 rounded-lg border transition-colors cursor-pointer hover-elevate ${
        isActive ? "bg-card border-border" : "bg-card/60 border-border/60 opacity-80"
      }`}
      data-testid={`card-signal-${signal.id}`}
    >
      <div className="flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-4">
        <div className="flex items-center gap-3 flex-1 min-w-0">
          <div
            className={`flex-shrink-0 flex items-center justify-center w-11 h-11 rounded-lg ${
              isBuy ? "bg-emerald-500/10" : "bg-red-500/10"
            }`}
          >
            {isBuy ? (
              <TrendingUp className="w-5 h-5 text-emerald-500" />
            ) : (
              <TrendingDown className="w-5 h-5 text-red-500" />
            )}
          </div>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap mb-0.5">
              <span className="font-semibold text-base tracking-tight" data-testid={`text-pair-${signal.id}`}>
                {signal.pair}
              </span>
              <Badge
                variant={isBuy ? "default" : "destructive"}
                className="no-default-active-elevate text-xs px-2 py-0"
                data-testid={`badge-direction-${signal.id}`}
              >
                {signal.direction}
              </Badge>
              {!isActive && (
                <Badge variant="secondary" className="no-default-active-elevate text-xs px-2 py-0 capitalize">
                  {signal.status}
                </Badge>
              )}
            </div>
            <p className="text-sm text-muted-foreground truncate" data-testid={`text-summary-${signal.id}`}>
              {signal.shortSummary}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-5 sm:gap-6 flex-shrink-0 pl-14 sm:pl-0">
          <div className="text-right">
            <p className="text-[11px] text-muted-foreground uppercase tracking-wide mb-0.5">Entry</p>
            <p className="font-mono font-semibold text-sm" data-testid={`text-entry-${signal.id}`}>
              {formatPrice(signal.entryPrice, signal.pair)}
            </p>
          </div>
          <div className="text-right">
            <p className="text-[11px] text-red-400/80 uppercase tracking-wide mb-0.5">SL</p>
            <p className="font-mono text-sm text-red-400" data-testid={`text-sl-${signal.id}`}>
              {formatPrice(signal.stopLoss, signal.pair)}
            </p>
          </div>
          <div className="text-right">
            <p className="text-[11px] text-emerald-400/80 uppercase tracking-wide mb-0.5">TP</p>
            <p className="font-mono text-sm text-emerald-400" data-testid={`text-tp-${signal.id}`}>
              {formatPrice(signal.takeProfit, signal.pair)}
            </p>
          </div>
          <div className="hidden sm:flex flex-col items-end gap-1">
            <div className="flex items-center gap-1 text-xs text-muted-foreground font-mono">
              <Percent className="w-3 h-3" />
              {signal.confidence}
            </div>
            <div className="flex items-center gap-1 text-xs text-muted-foreground">
              <Clock className="w-3 h-3" />
              {timeAgo(signal.createdAt)}
            </div>
            <span className="text-[10px] text-muted-foreground/70 font-mono">
              R:R 1:{rr.toFixed(1)}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
