import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { Signal } from "@shared/schema";
import { SignalCard } from "@/components/signal-card";
import { CategoryFilter } from "@/components/category-filter";
import { GenerateSignalDialog } from "@/components/generate-signal-dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Input } from "@/components/ui/input";
import { Zap, TrendingUp, Shield, BarChart3, Search } from "lucide-react";

function matchesSearch(signal: Signal, query: string): boolean {
  if (!query.trim()) return true;
  const q = query.toLowerCase();
  return (
    (signal.pair || "").toLowerCase().includes(q) ||
    ((signal.fullName || "").toLowerCase().includes(q))
  );
}

export default function Home() {
  const [activeCategory, setActiveCategory] = useState("all");
  const [generateOpen, setGenerateOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  const { data: signals, isLoading } = useQuery<Signal[]>({
    queryKey: ["/api/signals", activeCategory],
    queryFn: async () => {
      const url = activeCategory === "all" ? "/api/signals" : `/api/signals?category=${activeCategory}`;
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch signals");
      return res.json();
    },
  });

  const allActive = signals?.filter(s => s.status === "active") || [];
  const allClosed = signals?.filter(s => s.status !== "active") || [];
  const activeSignals = allActive.filter(s => matchesSearch(s, searchQuery));
  const closedSignals = allClosed.filter(s => matchesSearch(s, searchQuery));

  return (
    <div>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
            <div>
              <h2 className="text-2xl font-bold" data-testid="text-page-title">Free AI Trading Signals</h2>
              <p className="text-muted-foreground max-w-2xl">
                Our AI analyzes market conditions, technical indicators, and price action to generate
                actionable trading signals for forex, crypto, and commodities.
              </p>
            </div>
            <Button data-testid="button-generate-signal" onClick={() => setGenerateOpen(true)} className="shrink-0">
              <Zap className="w-4 h-4 mr-2" />
              Generate Signal
            </Button>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-8">
            <div className="flex items-center gap-3 p-4 rounded-md bg-card border border-card-border">
              <div className="flex items-center justify-center w-10 h-10 rounded-md bg-primary/10">
                <TrendingUp className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="text-sm font-medium">{activeSignals.length} Active</p>
                <p className="text-xs text-muted-foreground">Live signals</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-4 rounded-md bg-card border border-card-border">
              <div className="flex items-center justify-center w-10 h-10 rounded-md bg-primary/10">
                <Shield className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="text-sm font-medium">AI-Powered</p>
                <p className="text-xs text-muted-foreground">Technical analysis</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-4 rounded-md bg-card border border-card-border">
              <div className="flex items-center justify-center w-10 h-10 rounded-md bg-primary/10">
                <BarChart3 className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="text-sm font-medium">{signals?.length || 0} Total</p>
                <p className="text-xs text-muted-foreground">Signals generated</p>
              </div>
            </div>
          </div>

          <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center mb-4">
            <CategoryFilter active={activeCategory} onChange={setActiveCategory} />
            <div className="relative w-full sm:w-64">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
              <Input
                data-testid="input-search-signals"
                className="pl-9 h-9 text-sm"
                placeholder="Search ticker or company…"
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
              />
            </div>
          </div>
        </div>

        {isLoading ? (
          <div className="space-y-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-32 w-full rounded-md" />
            ))}
          </div>
        ) : (
          <>
            {activeSignals.length > 0 && (
              <div className="mb-8">
                <div className="flex items-center gap-2 mb-4">
                  <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                  <h3 className="text-lg font-semibold" data-testid="text-active-signals">Active Signals</h3>
                  <Badge variant="secondary" className="no-default-active-elevate">{activeSignals.length}</Badge>
                </div>
                <div className="grid gap-4">
                  {activeSignals.map((signal) => (
                    <SignalCard key={signal.id} signal={signal} />
                  ))}
                </div>
              </div>
            )}

            {closedSignals.length > 0 && (
              <div>
                <div className="flex items-center gap-2 mb-4">
                  <h3 className="text-lg font-semibold" data-testid="text-closed-signals">Previous Signals</h3>
                  <Badge variant="secondary" className="no-default-active-elevate">{closedSignals.length}</Badge>
                </div>
                <div className="grid gap-4">
                  {closedSignals.map((signal) => (
                    <SignalCard key={signal.id} signal={signal} />
                  ))}
                </div>
              </div>
            )}

            {(!signals || signals.length === 0) && (
              <div className="text-center py-16">
                <div className="flex items-center justify-center w-16 h-16 rounded-full bg-muted mx-auto mb-4">
                  <Zap className="w-8 h-8 text-muted-foreground" />
                </div>
                <h3 className="text-lg font-semibold mb-2">No signals yet</h3>
                <p className="text-muted-foreground mb-6 max-w-md mx-auto">
                  Generate your first AI-powered trading signal by selecting a currency pair.
                </p>
                <Button data-testid="button-generate-first" onClick={() => setGenerateOpen(true)}>
                  <Zap className="w-4 h-4 mr-2" />
                  Generate Your First Signal
                </Button>
              </div>
            )}
          </>
        )}
      </div>

      <GenerateSignalDialog open={generateOpen} onOpenChange={setGenerateOpen} />
    </div>
  );
}
