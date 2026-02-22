import type { Signal } from "@shared/schema";
import { Badge } from "@/components/ui/badge";
import {
  TrendingUp,
  TrendingDown,
  Clock,
} from "lucide-react";

function formatPrice(price: number, pair: string): string {
  if (pair.includes("JPY")) return price.toFixed(3);
  if (pair.includes("BTC") || pair.includes("ETH")) return price.toFixed(2);
  if (pair.includes("XAU") || pair.includes("XAG") || pair.includes("WTI")) return price.toFixed(2);
  return price.toFixed(5);
}

function formatDate(date: string | Date): string {
  return new Date(date).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function getCategoryIcon(pair: string): string {
  if (pair.includes("BTC") || pair.includes("ETH")) return "crypto";
  if (pair.includes("XAU") || pair.includes("XAG") || pair.includes("WTI")) return "commodity";
  return "forex";
}

function getPairFlags(pair: string): { base: string; quote: string } {
  const parts = pair.split("/");
  return { base: parts[0], quote: parts[1] };
}

export function SignalCard({ signal }: { signal: Signal }) {
  const isBuy = signal.direction === "Buy";
  const { base, quote } = getPairFlags(signal.pair);
  const isActive = signal.status === "active";

  return (
    <div
      className={`group relative p-4 rounded-md border transition-all cursor-pointer hover-elevate ${
        isActive
          ? "bg-card border-card-border"
          : "bg-card/50 border-card-border/50"
      }`}
      data-testid={`card-signal-${signal.id}`}
    >
      <div className="flex flex-col sm:flex-row sm:items-center gap-4">
        <div className="flex items-center gap-4 flex-1 min-w-0">
          <div
            className={`flex-shrink-0 flex items-center justify-center w-12 h-12 rounded-md ${
              isBuy ? "bg-green-500/10" : "bg-red-500/10"
            }`}
          >
            {isBuy ? (
              <TrendingUp className="w-6 h-6 text-green-500" />
            ) : (
              <TrendingDown className="w-6 h-6 text-red-500" />
            )}
          </div>

          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap mb-1">
              <span className="font-semibold text-base" data-testid={`text-pair-${signal.id}`}>
                {signal.pair}
              </span>
              <Badge
                variant={isBuy ? "default" : "destructive"}
                className="no-default-active-elevate text-xs"
                data-testid={`badge-direction-${signal.id}`}
              >
                {signal.direction}
              </Badge>
              {!isActive && (
                <Badge variant="secondary" className="no-default-active-elevate text-xs">
                  {signal.status}
                </Badge>
              )}
            </div>
            <p className="text-sm text-muted-foreground truncate" data-testid={`text-summary-${signal.id}`}>
              {signal.shortSummary}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-6 sm:gap-8 flex-shrink-0">
          <div className="text-right">
            <p className="text-xs text-muted-foreground mb-0.5">Entry</p>
            <p className="font-mono font-semibold text-sm">
              {formatPrice(signal.entryPrice, signal.pair)}
            </p>
          </div>
          <div className="text-right">
            <p className="text-xs text-red-500/80 mb-0.5">SL</p>
            <p className="font-mono text-sm text-red-500">
              {formatPrice(signal.stopLoss, signal.pair)}
            </p>
          </div>
          <div className="text-right">
            <p className="text-xs text-green-500/80 mb-0.5">TP</p>
            <p className="font-mono text-sm text-green-500">
              {formatPrice(signal.takeProfit, signal.pair)}
            </p>
          </div>
          <div className="hidden sm:flex flex-col items-end gap-1">
            <Badge variant="secondary" className="no-default-active-elevate text-xs font-mono">
              {signal.confidence}%
            </Badge>
            <div className="flex items-center gap-1 text-xs text-muted-foreground">
              <Clock className="w-3 h-3" />
              {formatDate(signal.createdAt)}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
