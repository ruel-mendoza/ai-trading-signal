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
  CheckCircle,
  XCircle,
} from "lucide-react";

function formatPrice(price: number, pair: string): string {
  if (pair.includes("JPY")) return price.toFixed(3);
  if (pair.includes("BTC") || pair.includes("ETH")) return price.toFixed(2);
  if (pair.includes("XAU") || pair.includes("XAG") || pair.includes("WTI")) return price.toFixed(2);
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
  const ratio = reward / risk;
  return ratio.toFixed(2);
}

function getPipDistance(price1: number, price2: number, pair: string): string {
  const diff = Math.abs(price1 - price2);
  if (pair.includes("JPY")) return (diff * 100).toFixed(1);
  if (pair.includes("BTC") || pair.includes("ETH") || pair.includes("XAU") || pair.includes("WTI"))
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
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <Skeleton className="h-8 w-32 mb-6" />
        <Skeleton className="h-64 w-full rounded-md mb-4" />
        <Skeleton className="h-48 w-full rounded-md" />
      </div>
    );
  }

  if (!signal) {
    return (
      <div className="flex items-center justify-center py-24">
        <div className="text-center">
          <h2 className="text-xl font-semibold mb-2">Signal not found</h2>
          <Button variant="secondary" onClick={() => navigate("/")}>
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to signals
          </Button>
        </div>
      </div>
    );
  }

  const isBuy = signal.direction === "Buy";
  const riskReward = getRiskReward(signal);

  return (
    <div>
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => navigate("/")}
          className="mb-4"
          data-testid="button-back"
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Signals
        </Button>
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
          <div className="flex items-center gap-3">
            <div className={`flex items-center justify-center w-12 h-12 rounded-md ${isBuy ? "bg-green-500/10" : "bg-red-500/10"}`}>
              {isBuy ? (
                <TrendingUp className="w-6 h-6 text-green-500" />
              ) : (
                <TrendingDown className="w-6 h-6 text-red-500" />
              )}
            </div>
            <div>
              <h1 className="text-2xl font-bold" data-testid="text-signal-pair">{signal.pair}</h1>
              <div className="flex items-center gap-2 flex-wrap">
                <Badge
                  variant={isBuy ? "default" : "destructive"}
                  className="no-default-active-elevate"
                  data-testid="badge-direction"
                >
                  {signal.direction}
                </Badge>
                <Badge
                  variant={signal.status === "active" ? "default" : "secondary"}
                  className="no-default-active-elevate"
                  data-testid="badge-status"
                >
                  {signal.status === "active" ? "Active" : signal.status === "closed" ? "Closed" : "Expired"}
                </Badge>
                <Badge variant="secondary" className="no-default-active-elevate">
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
                data-testid="button-close-signal"
              >
                <CheckCircle className="w-4 h-4 mr-1" />
                Close
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => updateStatusMutation.mutate("expired")}
                disabled={updateStatusMutation.isPending}
                data-testid="button-expire-signal"
              >
                <XCircle className="w-4 h-4 mr-1" />
                Expire
              </Button>
            </div>
          )}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-2 mb-2">
                <Target className="w-4 h-4 text-muted-foreground" />
                <span className="text-sm text-muted-foreground">Entry Price</span>
              </div>
              <p className="text-xl font-bold font-mono" data-testid="text-entry-price">
                {formatPrice(signal.entryPrice, signal.pair)}
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-2 mb-2">
                <ShieldAlert className="w-4 h-4 text-red-500" />
                <span className="text-sm text-muted-foreground">Stop Loss</span>
              </div>
              <p className="text-xl font-bold font-mono text-red-500" data-testid="text-stop-loss">
                {formatPrice(signal.stopLoss, signal.pair)}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {getPipDistance(signal.entryPrice, signal.stopLoss, signal.pair)} pips risk
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-2 mb-2">
                <TrendingUp className="w-4 h-4 text-green-500" />
                <span className="text-sm text-muted-foreground">Take Profit</span>
              </div>
              <p className="text-xl font-bold font-mono text-green-500" data-testid="text-take-profit">
                {formatPrice(signal.takeProfit, signal.pair)}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {getPipDistance(signal.entryPrice, signal.takeProfit, signal.pair)} pips target
              </p>
            </CardContent>
          </Card>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
          <div className="flex items-center gap-3 p-4 rounded-md bg-card border border-card-border">
            <BarChart3 className="w-5 h-5 text-primary" />
            <div>
              <p className="text-sm text-muted-foreground">Confidence</p>
              <p className="text-lg font-semibold">{signal.confidence}%</p>
            </div>
          </div>
          <div className="flex items-center gap-3 p-4 rounded-md bg-card border border-card-border">
            <Target className="w-5 h-5 text-primary" />
            <div>
              <p className="text-sm text-muted-foreground">Risk/Reward</p>
              <p className="text-lg font-semibold">1:{riskReward}</p>
            </div>
          </div>
          <div className="flex items-center gap-3 p-4 rounded-md bg-card border border-card-border">
            <Clock className="w-5 h-5 text-primary" />
            <div>
              <p className="text-sm text-muted-foreground">Generated</p>
              <p className="text-sm font-medium">{formatDate(signal.createdAt)}</p>
            </div>
          </div>
        </div>

        <Card>
          <CardHeader className="pb-3">
            <h3 className="text-lg font-semibold">Technical Analysis</h3>
            <p className="text-sm text-muted-foreground">{signal.shortSummary}</p>
          </CardHeader>
          <CardContent>
            <div className="prose prose-sm dark:prose-invert max-w-none" data-testid="text-analysis">
              {signal.analysis.split("\n").map((paragraph, i) => (
                <p key={i} className="mb-3 text-sm leading-relaxed text-foreground/90">
                  {paragraph}
                </p>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
