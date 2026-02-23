import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "wouter";
import type { Signal } from "@shared/schema";
import { SignalCard } from "@/components/signal-card";
import { CategoryFilter } from "@/components/category-filter";
import { GenerateSignalDialog } from "@/components/generate-signal-dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Zap, TrendingUp, BarChart3, Activity } from "lucide-react";

export default function Home() {
  const [activeCategory, setActiveCategory] = useState("all");
  const [generateOpen, setGenerateOpen] = useState(false);

  const { data: signals, isLoading } = useQuery<Signal[]>({
    queryKey: ["/api/signals", activeCategory],
    queryFn: async () => {
      const url =
        activeCategory === "all"
          ? "/api/signals"
          : `/api/signals?category=${activeCategory}`;
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch signals");
      return res.json();
    },
  });

  const activeSignals = signals?.filter((s) => s.status === "active") || [];
  const closedSignals = signals?.filter((s) => s.status !== "active") || [];

  const avgConfidence =
    activeSignals.length > 0
      ? Math.round(
          activeSignals.reduce((sum, s) => sum + s.confidence, 0) /
            activeSignals.length
        )
      : 0;

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
                AI Signals
              </span>
            </div>
            <Button
              size="sm"
              className="gap-1.5"
              onClick={() => setGenerateOpen(true)}
              data-testid="button-generate-signal"
            >
              <Zap className="w-3.5 h-3.5" />
              Generate Signal
            </Button>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
        <div className="mb-6">
          <h1 className="text-xl sm:text-2xl font-bold tracking-tight mb-1" data-testid="text-page-title">
            Trading Signals
          </h1>
          <p className="text-sm text-muted-foreground max-w-xl">
            AI-powered signals with entry, stop loss, and take profit levels for forex, crypto, and commodities.
          </p>
        </div>

        <div className="grid grid-cols-3 gap-3 mb-6">
          <div className="flex items-center gap-2.5 p-3 rounded-lg bg-card border border-border">
            <TrendingUp className="w-4 h-4 text-emerald-500 flex-shrink-0" />
            <div>
              <p className="text-lg font-bold leading-none">{activeSignals.length}</p>
              <p className="text-[11px] text-muted-foreground mt-0.5">Active</p>
            </div>
          </div>
          <div className="flex items-center gap-2.5 p-3 rounded-lg bg-card border border-border">
            <BarChart3 className="w-4 h-4 text-primary flex-shrink-0" />
            <div>
              <p className="text-lg font-bold leading-none">{signals?.length || 0}</p>
              <p className="text-[11px] text-muted-foreground mt-0.5">Total</p>
            </div>
          </div>
          <div className="flex items-center gap-2.5 p-3 rounded-lg bg-card border border-border">
            <Activity className="w-4 h-4 text-primary flex-shrink-0" />
            <div>
              <p className="text-lg font-bold leading-none">{avgConfidence}%</p>
              <p className="text-[11px] text-muted-foreground mt-0.5">Avg Conf.</p>
            </div>
          </div>
        </div>

        <div className="mb-5">
          <CategoryFilter active={activeCategory} onChange={setActiveCategory} />
        </div>

        {isLoading ? (
          <div className="space-y-3">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-24 w-full rounded-lg" />
            ))}
          </div>
        ) : (
          <>
            {activeSignals.length > 0 && (
              <section className="mb-8">
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                  <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground" data-testid="text-active-signals">
                    Active Signals
                  </h2>
                  <Badge variant="secondary" className="no-default-active-elevate text-xs">
                    {activeSignals.length}
                  </Badge>
                </div>
                <div className="grid gap-2.5">
                  {activeSignals.map((signal) => (
                    <Link key={signal.id} href={`/signal/${signal.id}`} data-testid={`link-signal-${signal.id}`}>
                      <SignalCard signal={signal} />
                    </Link>
                  ))}
                </div>
              </section>
            )}

            {closedSignals.length > 0 && (
              <section>
                <div className="flex items-center gap-2 mb-3">
                  <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground" data-testid="text-closed-signals">
                    Previous Signals
                  </h2>
                  <Badge variant="secondary" className="no-default-active-elevate text-xs">
                    {closedSignals.length}
                  </Badge>
                </div>
                <div className="grid gap-2.5">
                  {closedSignals.map((signal) => (
                    <Link key={signal.id} href={`/signal/${signal.id}`} data-testid={`link-signal-${signal.id}`}>
                      <SignalCard signal={signal} />
                    </Link>
                  ))}
                </div>
              </section>
            )}

            {(!signals || signals.length === 0) && (
              <div className="text-center py-16">
                <div className="flex items-center justify-center w-14 h-14 rounded-full bg-muted mx-auto mb-4">
                  <Zap className="w-7 h-7 text-muted-foreground" />
                </div>
                <h3 className="text-base font-semibold mb-1.5">No signals yet</h3>
                <p className="text-sm text-muted-foreground mb-5 max-w-sm mx-auto">
                  Generate your first AI-powered trading signal by selecting a trading pair.
                </p>
                <Button onClick={() => setGenerateOpen(true)} className="gap-1.5" data-testid="button-generate-first">
                  <Zap className="w-4 h-4" />
                  Generate Your First Signal
                </Button>
              </div>
            )}
          </>
        )}
      </main>

      <GenerateSignalDialog open={generateOpen} onOpenChange={setGenerateOpen} />
    </div>
  );
}
