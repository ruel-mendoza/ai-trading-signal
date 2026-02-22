import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { queryClient } from "@/lib/queryClient";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { Zap, Loader2, CheckCircle, AlertCircle } from "lucide-react";

interface PairInfo {
  pair: string;
  category: string;
}

type GenerateState = "idle" | "generating" | "success" | "error";

export function GenerateSignalDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [selectedPair, setSelectedPair] = useState("");
  const [state, setState] = useState<GenerateState>("idle");
  const [statusMessage, setStatusMessage] = useState("");
  const { toast } = useToast();

  const { data: pairs } = useQuery<PairInfo[]>({
    queryKey: ["/api/pairs"],
  });

  const handleGenerate = async () => {
    if (!selectedPair) return;

    setState("generating");
    setStatusMessage("Connecting to AI...");

    try {
      const response = await fetch("/api/signals/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pair: selectedPair }),
      });

      if (!response.ok) throw new Error("Failed to generate signal");

      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const event = JSON.parse(line.slice(6));
            if (event.type === "status") {
              setStatusMessage(event.message);
            } else if (event.type === "complete") {
              setState("success");
              setStatusMessage("Signal generated successfully!");
              queryClient.invalidateQueries({ queryKey: ["/api/signals"] });
              toast({ title: `Signal generated for ${selectedPair}` });
              setTimeout(() => {
                onOpenChange(false);
                setState("idle");
                setSelectedPair("");
              }, 1500);
            } else if (event.type === "error") {
              throw new Error(event.message);
            }
          } catch (e) {
            if (!(e instanceof SyntaxError)) throw e;
          }
        }
      }
    } catch (error) {
      setState("error");
      setStatusMessage("Failed to generate signal. Please try again.");
      toast({ title: "Failed to generate signal", variant: "destructive" });
      setTimeout(() => setState("idle"), 3000);
    }
  };

  const handleClose = (open: boolean) => {
    if (state === "generating") return;
    onOpenChange(open);
    if (!open) {
      setState("idle");
      setSelectedPair("");
    }
  };

  const forexPairs = pairs?.filter(p => p.category === "forex") || [];
  const cryptoPairs = pairs?.filter(p => p.category === "crypto") || [];
  const commodityPairs = pairs?.filter(p => p.category === "commodities") || [];

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Zap className="w-5 h-5 text-primary" />
            Generate AI Signal
          </DialogTitle>
          <DialogDescription>
            Select a trading pair and our AI will analyze market conditions to generate
            a trading signal with entry, stop loss, and take profit levels.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 pt-2">
          <div>
            <label className="text-sm font-medium mb-2 block">Trading Pair</label>
            <Select
              value={selectedPair}
              onValueChange={setSelectedPair}
              disabled={state === "generating"}
            >
              <SelectTrigger data-testid="select-pair">
                <SelectValue placeholder="Select a pair..." />
              </SelectTrigger>
              <SelectContent>
                {forexPairs.length > 0 && (
                  <>
                    <div className="px-2 py-1.5 text-xs font-semibold text-muted-foreground">Forex</div>
                    {forexPairs.map(p => (
                      <SelectItem key={p.pair} value={p.pair} data-testid={`option-${p.pair}`}>
                        {p.pair}
                      </SelectItem>
                    ))}
                  </>
                )}
                {cryptoPairs.length > 0 && (
                  <>
                    <div className="px-2 py-1.5 text-xs font-semibold text-muted-foreground">Crypto</div>
                    {cryptoPairs.map(p => (
                      <SelectItem key={p.pair} value={p.pair} data-testid={`option-${p.pair}`}>
                        {p.pair}
                      </SelectItem>
                    ))}
                  </>
                )}
                {commodityPairs.length > 0 && (
                  <>
                    <div className="px-2 py-1.5 text-xs font-semibold text-muted-foreground">Commodities</div>
                    {commodityPairs.map(p => (
                      <SelectItem key={p.pair} value={p.pair} data-testid={`option-${p.pair}`}>
                        {p.pair}
                      </SelectItem>
                    ))}
                  </>
                )}
              </SelectContent>
            </Select>
          </div>

          {state !== "idle" && (
            <div className={`flex items-center gap-3 p-3 rounded-md ${
              state === "generating" ? "bg-primary/5 border border-primary/20" :
              state === "success" ? "bg-green-500/5 border border-green-500/20" :
              "bg-red-500/5 border border-red-500/20"
            }`}>
              {state === "generating" && <Loader2 className="w-4 h-4 text-primary animate-spin" />}
              {state === "success" && <CheckCircle className="w-4 h-4 text-green-500" />}
              {state === "error" && <AlertCircle className="w-4 h-4 text-red-500" />}
              <span className="text-sm">{statusMessage}</span>
            </div>
          )}

          <Button
            className="w-full"
            onClick={handleGenerate}
            disabled={!selectedPair || state === "generating" || state === "success"}
            data-testid="button-generate-confirm"
          >
            {state === "generating" ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Analyzing...
              </>
            ) : (
              <>
                <Zap className="w-4 h-4 mr-2" />
                Generate Signal
              </>
            )}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
