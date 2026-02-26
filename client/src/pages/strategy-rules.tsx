import type { ReactNode } from "react";
import {
  TrendingUp,
  TrendingDown,
  ArrowRight,
  Layers,
  Target,
  ShieldCheck,
  BarChart3,
  Clock,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  ArrowUpRight,
  ArrowDownRight,
  Activity,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";

function SectionHeader({ icon: Icon, title, subtitle }: { icon: any; title: string; subtitle?: string }) {
  return (
    <div className="flex items-start gap-3 mb-4">
      <div className="flex items-center justify-center w-9 h-9 rounded-md bg-primary/10 shrink-0 mt-0.5">
        <Icon className="w-5 h-5 text-primary" />
      </div>
      <div>
        <h3 className="text-lg font-semibold" data-testid={`text-section-${title.toLowerCase().replace(/\s+/g, '-')}`}>{title}</h3>
        {subtitle && <p className="text-sm text-muted-foreground">{subtitle}</p>}
      </div>
    </div>
  );
}

function RuleCard({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <div className={`p-4 rounded-md bg-card border border-card-border ${className}`}>
      {children}
    </div>
  );
}

function ConditionStep({ step, label, detail, passed }: { step: number; label: string; detail: string; passed?: boolean }) {
  return (
    <div className="flex items-start gap-3 py-2">
      <div className="flex items-center justify-center w-7 h-7 rounded-full bg-primary/10 text-primary text-sm font-semibold shrink-0">
        {step}
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium">{label}</p>
        <p className="text-xs text-muted-foreground mt-0.5">{detail}</p>
      </div>
      {passed !== undefined && (
        passed ? <CheckCircle2 className="w-4 h-4 text-green-500 shrink-0 mt-1" /> : <XCircle className="w-4 h-4 text-muted-foreground/40 shrink-0 mt-1" />
      )}
    </div>
  );
}

function AssetGroup({ label, assets }: { label: string; assets: string[] }) {
  return (
    <div className="space-y-1.5">
      <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{label}</p>
      <div className="flex flex-wrap gap-1.5">
        {assets.map((a) => (
          <Badge key={a} variant="secondary" className="no-default-active-elevate text-xs" data-testid={`badge-asset-${a.replace(/\//g, '-')}`}>
            {a}
          </Badge>
        ))}
      </div>
    </div>
  );
}

export default function StrategyRules() {
  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-8">
        <h2 className="text-2xl font-bold" data-testid="text-strategy-title">
          MTF EMA Trend-Pullback Strategy
        </h2>
        <p className="text-muted-foreground mt-1 max-w-2xl">
          Multi-Timeframe EMA strategy using D1 + H4 + H1 timeframe synchronization
          with trend-pullback entry logic and dual exit management.
        </p>
      </div>

      <div className="space-y-8">
        <section data-testid="section-assets">
          <SectionHeader
            icon={Layers}
            title="Covered Assets"
            subtitle="12 instruments across 4 asset classes"
          />
          <RuleCard>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <AssetGroup label="Indices" assets={["SPX", "NDX", "RUT"]} />
              <AssetGroup label="Commodities" assets={["XAU/USD", "XAG/USD", "WTI/USD"]} />
              <AssetGroup label="Crypto" assets={["BTC/USD", "ETH/USD"]} />
              <AssetGroup label="Forex" assets={["EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD"]} />
            </div>
          </RuleCard>
        </section>

        <section data-testid="section-timeframes">
          <SectionHeader
            icon={Clock}
            title="Timeframe Hierarchy"
            subtitle="Three timeframes synchronized for confirmation"
          />
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <RuleCard>
              <p className="text-sm font-semibold mb-1">D1 (Daily)</p>
              <p className="text-xs text-muted-foreground mb-2">Trend Direction</p>
              <div className="space-y-1">
                <p className="text-xs">EMA 200 — Primary trend</p>
                <p className="text-xs">EMA 50 — Secondary trend</p>
              </div>
            </RuleCard>
            <RuleCard>
              <p className="text-sm font-semibold mb-1">H4 (4-Hour)</p>
              <p className="text-xs text-muted-foreground mb-2">Momentum & Pullback</p>
              <div className="space-y-1">
                <p className="text-xs">EMA 50 — Pullback zone</p>
                <p className="text-xs">EMA 200 — Slope acceleration</p>
                <p className="text-xs">ATR 100 — Volatility measure</p>
              </div>
            </RuleCard>
            <RuleCard>
              <p className="text-sm font-semibold mb-1">H1 (1-Hour)</p>
              <p className="text-xs text-muted-foreground mb-2">Entry Trigger</p>
              <div className="space-y-1">
                <p className="text-xs">EMA 20 — Crossover signal</p>
                <p className="text-xs">Candle body — Confirmation</p>
              </div>
            </RuleCard>
          </div>
        </section>

        <section data-testid="section-indicators">
          <SectionHeader
            icon={BarChart3}
            title="Indicators"
            subtitle="EMA 20/50/200 and ATR 100 across all timeframes"
          />
          <RuleCard>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-2 pr-4 text-xs font-medium text-muted-foreground">Indicator</th>
                    <th className="text-center py-2 px-4 text-xs font-medium text-muted-foreground">D1</th>
                    <th className="text-center py-2 px-4 text-xs font-medium text-muted-foreground">H4</th>
                    <th className="text-center py-2 px-4 text-xs font-medium text-muted-foreground">H1</th>
                    <th className="text-left py-2 pl-4 text-xs font-medium text-muted-foreground">Purpose</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  <tr>
                    <td className="py-2 pr-4 font-medium">EMA 20</td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 pl-4 text-muted-foreground">H1 crossover trigger</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium">EMA 50</td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 pl-4 text-muted-foreground">Pullback zone & exit level</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium">EMA 200</td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 pl-4 text-muted-foreground">Trend direction & slope</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium">ATR 100</td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 px-4 text-center"><CheckCircle2 className="w-4 h-4 text-green-500 inline" /></td>
                    <td className="py-2 pl-4 text-muted-foreground">Stop loss & trailing stop</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </RuleCard>
        </section>

        <section data-testid="section-entry-long">
          <SectionHeader
            icon={ArrowUpRight}
            title="Long Entry Conditions"
            subtitle="All 4 conditions must be met simultaneously"
          />
          <RuleCard>
            <div className="divide-y divide-border/50">
              <ConditionStep
                step={1}
                label="D1 Trend Validation"
                detail="Price must be above both D1 EMA 200 and D1 EMA 50, confirming a bullish macro trend."
              />
              <ConditionStep
                step={2}
                label="Slope Acceleration"
                detail="D1 EMA 200 must be rising (current > previous). H4 EMA 200 must be accelerating upward: (current - prev) > (prev - earlier)."
              />
              <ConditionStep
                step={3}
                label="Pullback Validation"
                detail="Price must be below H4 EMA 50 (dipped into pullback zone) AND within 1x H4 ATR 100 of the H4 EMA 50."
              />
              <ConditionStep
                step={4}
                label="H1 Confirmation"
                detail="Previous H1 candle's close was below H1 EMA 20, current H1 candle's close is above H1 EMA 20 (crossover). Current H1 candle must have a bullish body (Close > Open)."
              />
            </div>
          </RuleCard>
        </section>

        <section data-testid="section-entry-short">
          <SectionHeader
            icon={ArrowDownRight}
            title="Short Entry Conditions"
            subtitle="Mirrored logic — all 4 conditions must be met"
          />
          <RuleCard>
            <div className="divide-y divide-border/50">
              <ConditionStep
                step={1}
                label="D1 Trend Validation"
                detail="Price must be below both D1 EMA 200 and D1 EMA 50, confirming a bearish macro trend."
              />
              <ConditionStep
                step={2}
                label="Slope Acceleration"
                detail="D1 EMA 200 must be falling (current < previous). H4 EMA 200 must be accelerating downward: (prev - current) > (earlier - prev)."
              />
              <ConditionStep
                step={3}
                label="Pullback Validation"
                detail="Price must be above H4 EMA 50 (rallied into pullback zone) AND within 1x H4 ATR 100 of the H4 EMA 50."
              />
              <ConditionStep
                step={4}
                label="H1 Confirmation"
                detail="Previous H1 candle's close was above H1 EMA 20, current H1 candle's close is below H1 EMA 20 (crossover). Current H1 candle must have a bearish body (Close < Open)."
              />
            </div>
          </RuleCard>
        </section>

        <section data-testid="section-stop-loss">
          <SectionHeader
            icon={ShieldCheck}
            title="Stop Loss Selection"
            subtitle="Whichever-is-greater method comparing structural and ATR stops"
          />
          <div className="space-y-3">
            <RuleCard>
              <div className="flex items-start gap-2 mb-3">
                <Badge variant="secondary" className="no-default-active-elevate text-xs shrink-0">Method A</Badge>
                <div>
                  <p className="text-sm font-medium">ATR-Based Stop</p>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    0.5x H4 ATR(100) from entry price. For longs: entry - (0.5 x ATR). For shorts: entry + (0.5 x ATR).
                  </p>
                </div>
              </div>
              <div className="flex items-start gap-2 mb-3">
                <Badge variant="secondary" className="no-default-active-elevate text-xs shrink-0">Method B</Badge>
                <div>
                  <p className="text-sm font-medium">Structural Stop</p>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    For longs: lowest H1 low below H4 EMA 50 in last 24 H1 candles, minus 2-pip buffer (0.0002).
                    For shorts: highest H1 high above H4 EMA 50 in last 24 H1 candles, plus 2-pip buffer.
                  </p>
                </div>
              </div>
              <div className="p-3 rounded-md bg-muted/50 border border-border/50">
                <div className="flex items-center gap-2 mb-1">
                  <AlertTriangle className="w-4 h-4 text-primary" />
                  <p className="text-sm font-medium">Selection Rule</p>
                </div>
                <p className="text-xs text-muted-foreground">
                  The stop loss with the <span className="font-medium text-foreground">greater distance</span> from entry is selected.
                  If no structural candles qualify, ATR stop is used as fallback.
                  If the structural stop lands on the wrong side of entry, ATR stop is used.
                </p>
              </div>
            </RuleCard>

            <RuleCard>
              <p className="text-sm font-medium mb-1">Take Profit</p>
              <p className="text-xs text-muted-foreground">
                3.0x H4 ATR(100) from entry price. For longs: entry + (3.0 x ATR). For shorts: entry - (3.0 x ATR).
              </p>
            </RuleCard>
          </div>
        </section>

        <section data-testid="section-exit-rules">
          <SectionHeader
            icon={Target}
            title="Exit Rules"
            subtitle="Two exit mechanisms checked in priority order"
          />
          <div className="space-y-3">
            <RuleCard>
              <div className="flex items-center gap-2 mb-2">
                <Badge className="no-default-active-elevate text-xs">Priority 1</Badge>
                <p className="text-sm font-semibold">H4 EMA 50 Breach Exit</p>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div className="p-3 rounded-md bg-muted/50 border border-border/50">
                  <div className="flex items-center gap-1.5 mb-1">
                    <TrendingUp className="w-4 h-4 text-green-500" />
                    <p className="text-sm font-medium">Long Exit</p>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Exit immediately when an H4 candle <span className="font-medium text-foreground">closes below</span> the H4 EMA 50.
                    The pullback zone has been lost.
                  </p>
                </div>
                <div className="p-3 rounded-md bg-muted/50 border border-border/50">
                  <div className="flex items-center gap-1.5 mb-1">
                    <TrendingDown className="w-4 h-4 text-red-500" />
                    <p className="text-sm font-medium">Short Exit</p>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Exit immediately when an H4 candle <span className="font-medium text-foreground">closes above</span> the H4 EMA 50.
                    The pullback zone has been lost.
                  </p>
                </div>
              </div>
            </RuleCard>

            <RuleCard>
              <div className="flex items-center gap-2 mb-2">
                <Badge variant="secondary" className="no-default-active-elevate text-xs">Priority 2</Badge>
                <p className="text-sm font-semibold">Trailing Stop Exit</p>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div className="p-3 rounded-md bg-muted/50 border border-border/50">
                  <div className="flex items-center gap-1.5 mb-1">
                    <TrendingUp className="w-4 h-4 text-green-500" />
                    <p className="text-sm font-medium">Long Trailing Stop</p>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Tracks the highest price since entry. Stop = peak - (2.0x ATR at entry).
                    Triggers when price drops below the trailing stop.
                  </p>
                </div>
                <div className="p-3 rounded-md bg-muted/50 border border-border/50">
                  <div className="flex items-center gap-1.5 mb-1">
                    <TrendingDown className="w-4 h-4 text-red-500" />
                    <p className="text-sm font-medium">Short Trailing Stop</p>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Tracks the lowest price since entry. Stop = trough + (2.0x ATR at entry).
                    Triggers when price rises above the trailing stop.
                  </p>
                </div>
              </div>
            </RuleCard>
          </div>
        </section>

        <section data-testid="section-diagnostics">
          <SectionHeader
            icon={Activity}
            title="Exit Diagnostics Logging"
            subtitle="Every exit evaluation logs detailed slope and pullback data"
          />
          <RuleCard>
            <div className="space-y-2 text-xs">
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                <div className="p-2.5 rounded-md bg-muted/50 border border-border/50">
                  <p className="font-medium text-sm mb-1">D1 EMA 200 Slope</p>
                  <p className="text-muted-foreground">current - previous value. Positive = rising daily trend, negative = falling.</p>
                </div>
                <div className="p-2.5 rounded-md bg-muted/50 border border-border/50">
                  <p className="font-medium text-sm mb-1">H4 EMA 200 Slope</p>
                  <p className="text-muted-foreground">Current and previous period slopes logged separately to show momentum direction.</p>
                </div>
                <div className="p-2.5 rounded-md bg-muted/50 border border-border/50">
                  <p className="font-medium text-sm mb-1">H4 EMA 200 Acceleration</p>
                  <p className="text-muted-foreground">Difference between current slope and previous slope. Shows if momentum is increasing or decaying.</p>
                </div>
                <div className="p-2.5 rounded-md bg-muted/50 border border-border/50">
                  <p className="font-medium text-sm mb-1">Pullback Depth</p>
                  <p className="text-muted-foreground">H4 close minus H4 EMA 50. Shows how far price has moved from the key pullback level.</p>
                </div>
              </div>
            </div>
          </RuleCard>
        </section>

        <section data-testid="section-data-requirements">
          <SectionHeader
            icon={AlertTriangle}
            title="Data Requirements"
            subtitle="Minimum candle counts before strategy can evaluate"
          />
          <RuleCard>
            <div className="grid grid-cols-3 gap-4 text-center">
              <div>
                <p className="text-2xl font-bold" data-testid="text-min-d1">200</p>
                <p className="text-xs text-muted-foreground">D1 candles</p>
              </div>
              <div>
                <p className="text-2xl font-bold" data-testid="text-min-h4">200</p>
                <p className="text-xs text-muted-foreground">H4 candles</p>
              </div>
              <div>
                <p className="text-2xl font-bold" data-testid="text-min-h1">20</p>
                <p className="text-xs text-muted-foreground">H1 candles</p>
              </div>
            </div>
          </RuleCard>
        </section>

        <section data-testid="section-constants">
          <SectionHeader
            icon={ArrowRight}
            title="Strategy Constants"
            subtitle="Key parameters used in calculations"
          />
          <RuleCard>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-2 pr-4 text-xs font-medium text-muted-foreground">Constant</th>
                    <th className="text-left py-2 px-4 text-xs font-medium text-muted-foreground">Value</th>
                    <th className="text-left py-2 pl-4 text-xs font-medium text-muted-foreground">Usage</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/50">
                  <tr>
                    <td className="py-2 pr-4 font-medium font-mono text-xs">SL_ATR_MULT</td>
                    <td className="py-2 px-4">0.5</td>
                    <td className="py-2 pl-4 text-muted-foreground">ATR stop loss multiplier</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium font-mono text-xs">TP_ATR_MULT</td>
                    <td className="py-2 px-4">3.0</td>
                    <td className="py-2 pl-4 text-muted-foreground">Take profit multiplier</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium font-mono text-xs">TRAILING_STOP_ATR_MULT</td>
                    <td className="py-2 px-4">2.0</td>
                    <td className="py-2 pl-4 text-muted-foreground">Trailing stop distance</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium font-mono text-xs">STRUCTURAL_LOOKBACK_H1</td>
                    <td className="py-2 px-4">24</td>
                    <td className="py-2 pl-4 text-muted-foreground">H1 candles to scan for structural stop</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium font-mono text-xs">STRUCTURAL_PIP_BUFFER</td>
                    <td className="py-2 px-4">0.0002</td>
                    <td className="py-2 pl-4 text-muted-foreground">2-pip buffer on structural stop</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium font-mono text-xs">EMA periods</td>
                    <td className="py-2 px-4">20 / 50 / 200</td>
                    <td className="py-2 pl-4 text-muted-foreground">Fast / Medium / Slow EMA</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4 font-medium font-mono text-xs">ATR period</td>
                    <td className="py-2 px-4">100</td>
                    <td className="py-2 pl-4 text-muted-foreground">Volatility lookback</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </RuleCard>
        </section>
      </div>
    </div>
  );
}
