import { useQuery, useMutation } from "@tanstack/react-query";
import { useParams, useLocation } from "wouter";
import type { Signal } from "@shared/schema";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/use-toast";
import {
  ArrowLeft,
  TrendingUp,
  TrendingDown,
  Target,
  ShieldAlert,
  Clock,
  BarChart3,
  Activity,
  CheckCircle,
  XCircle,
} from "lucide-react";

function formatPrice(price: number, pair: string): string {
  if (pair.includes("JPY")) return price.toFixed(3);
  if (pair.includes("BTC") || pair.includes("ETH") || pair.includes("SOL"))
    return price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (pair.includes("XAU") || pair.includes("XAG") || pair.includes("WTI"))
    return price.toFixed(2);
  return price.toFixed(5);
}

function formatDate(date: string | Date): string {
  return new Date(date).toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function getRiskReward(signal: Signal): string {
  const risk = Math.abs(signal.entryPrice - signal.stopLoss);
  const reward = Math.abs(signal.takeProfit - signal.entryPrice);
  return (reward / risk).toFixed(2);
}

function getPipDistance(price1: number, price2: number, pair: string): string {
  const diff = Math.abs(price1 - price2);
  if (pair.includes("JPY")) return (diff * 100).toFixed(1);
  if (pair.includes("BTC") || pair.includes("ETH") || pair.includes("SOL") ||
      pair.includes("XAU") || pair.includes("WTI"))
    return diff.toFixed(2);
  return (diff * 10000).toFixed(1);
}

export default function SignalDetail() {
  const { id } = useParams<{ id: string }>();
  const [, navigate] = useLocation();
  const { toast } = useToast();

  const { data: signal, isLoading } = useQuery<Signal>({
    queryKey: ["/api/signals", "detail", id],
    queryFn: async () => {
      const res = await fetch(`/api/signals/${id}`, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch signal");
      return res.json();
    },
  });

  const updateStatusMutation = useMutation({
    mutationFn: async (status: string) => {
      const res = await apiRequest("PATCH", `/api/signals/${id}/status`, { status });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/signals"] });
      queryClient.invalidateQueries({ queryKey: ["/api/signals", "detail", id] });
      toast({ title: "Signal status updated" });
    },
    onError: () => {
      toast({ title: "Failed to update status", variant: "destructive" });
    },
  });

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background">
        <div className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Skeleton className="h-8 w-32 mb-6" />
          <Skeleton className="h-48 w-full rounded-lg mb-4" />
          <Skeleton className="h-40 w-full rounded-lg" />
        </div>
      </div>
    );
  }

  if (!signal) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-lg font-semibold mb-3">Signal not found</h2>
          <Button variant="secondary" onClick={() => navigate("/")} className="gap-1.5" data-testid="button-back-not-found">
            <ArrowLeft className="w-4 h-4" />
            Back to signals
          </Button>
        </div>
      </div>
    );
  }

  const isBuy = signal.direction === "Buy";
  const riskReward = getRiskReward(signal);

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b border-border bg-card/80 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center gap-3 h-14">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => navigate("/")}
              data-testid="button-back"
              className="h-8 w-8"
            >
              <ArrowLeft className="w-4 h-4" />
            </Button>
            <div className="flex items-center gap-2">
              <Activity className="w-4 h-4 text-primary" />
              <span className="text-sm font-medium text-muted-foreground">Signal Details</span>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
          <div className="flex items-center gap-3">
            <div
              className={`flex items-center justify-center w-12 h-12 rounded-lg ${
                isBuy ? "bg-emerald-500/10" : "bg-red-500/10"
              }`}
            >
              {isBuy ? (
                <TrendingUp className="w-6 h-6 text-emerald-500" />
              ) : (
                <TrendingDown className="w-6 h-6 text-red-500" />
              )}
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight" data-testid="text-signal-pair">
                {signal.pair}
              </h1>
              <div className="flex items-center gap-1.5 flex-wrap mt-0.5">
                <Badge
                  variant={isBuy ? "default" : "destructive"}
                  className="no-default-active-elevate text-xs"
                  data-testid="badge-direction"
                >
                  {signal.direction}
                </Badge>
                <Badge
                  variant={signal.status === "active" ? "default" : "secondary"}
                  className="no-default-active-elevate text-xs capitalize"
                  data-testid="badge-status"
                >
                  {signal.status}
                </Badge>
                <Badge variant="secondary" className="no-default-active-elevate text-xs capitalize">
                  {signal.category}
                </Badge>
              </div>
            </div>
          </div>
          {signal.status === "active" && (
            <div className="flex items-center gap-2">
              <Button
                variant="secondary"
                size="sm"
                onClick={() => updateStatusMutation.mutate("closed")}
                disabled={updateStatusMutation.isPending}
                className="gap-1"
                data-testid="button-close-signal"
              >
                <CheckCircle className="w-3.5 h-3.5" />
                Close
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => updateStatusMutation.mutate("expired")}
                disabled={updateStatusMutation.isPending}
                className="gap-1"
                data-testid="button-expire-signal"
              >
                <XCircle className="w-3.5 h-3.5" />
                Expire
              </Button>
            </div>
          )}
        </div>

        <div className="grid grid-cols-3 gap-3 mb-5">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-1.5 mb-1.5">
                <Target className="w-3.5 h-3.5 text-muted-foreground" />
                <span className="text-xs text-muted-foreground uppercase tracking-wide">Entry</span>
              </div>
              <p className="text-lg font-bold font-mono" data-testid="text-entry-price">
                {formatPrice(signal.entryPrice, signal.pair)}
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-1.5 mb-1.5">
                <ShieldAlert className="w-3.5 h-3.5 text-red-400" />
                <span className="text-xs text-red-400 uppercase tracking-wide">Stop Loss</span>
              </div>
              <p className="text-lg font-bold font-mono text-red-400" data-testid="text-stop-loss">
                {formatPrice(signal.stopLoss, signal.pair)}
              </p>
              <p className="text-[11px] text-muted-foreground mt-1 font-mono">
                {getPipDistance(signal.entryPrice, signal.stopLoss, signal.pair)} pips
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-1.5 mb-1.5">
                <TrendingUp className="w-3.5 h-3.5 text-emerald-400" />
                <span className="text-xs text-emerald-400 uppercase tracking-wide">Take Profit</span>
              </div>
              <p className="text-lg font-bold font-mono text-emerald-400" data-testid="text-take-profit">
                {formatPrice(signal.takeProfit, signal.pair)}
              </p>
              <p className="text-[11px] text-muted-foreground mt-1 font-mono">
                {getPipDistance(signal.entryPrice, signal.takeProfit, signal.pair)} pips
              </p>
            </CardContent>
          </Card>
        </div>

        <div className="grid grid-cols-3 gap-3 mb-5">
          <div className="flex items-center gap-2.5 p-3 rounded-lg bg-card border border-border">
            <BarChart3 className="w-4 h-4 text-primary flex-shrink-0" />
            <div>
              <p className="text-xs text-muted-foreground">Confidence</p>
              <p className="text-base font-bold" data-testid="text-confidence">{signal.confidence}%</p>
            </div>
          </div>
          <div className="flex items-center gap-2.5 p-3 rounded-lg bg-card border border-border">
            <Target className="w-4 h-4 text-primary flex-shrink-0" />
            <div>
              <p className="text-xs text-muted-foreground">Risk / Reward</p>
              <p className="text-base font-bold font-mono" data-testid="text-rr">1:{riskReward}</p>
            </div>
          </div>
          <div className="flex items-center gap-2.5 p-3 rounded-lg bg-card border border-border">
            <Clock className="w-4 h-4 text-primary flex-shrink-0" />
            <div>
              <p className="text-xs text-muted-foreground">Generated</p>
              <p className="text-xs font-medium" data-testid="text-date">{formatDate(signal.createdAt)}</p>
            </div>
          </div>
        </div>

        <Card>
          <CardHeader className="pb-2">
            <h3 className="text-base font-semibold">Technical Analysis</h3>
            <p className="text-sm text-muted-foreground leading-relaxed">{signal.shortSummary}</p>
          </CardHeader>
          <CardContent>
            <div data-testid="text-analysis">
              {signal.analysis.split("\n").map((paragraph, i) => (
                <p key={i} className="mb-3 last:mb-0 text-sm leading-relaxed text-foreground/85">
                  {paragraph}
                </p>
              ))}
            </div>
          </CardContent>
        </Card>
      </main>
    </div>
  );
}
