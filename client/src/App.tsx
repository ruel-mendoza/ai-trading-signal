import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Navbar } from "@/components/navbar";
import { useSignalStream } from "@/hooks/use-signal-stream";
import NotFound from "@/pages/not-found";
import Home from "@/pages/home";
import SignalDetail from "@/pages/signal-detail";

function Router() {
  return (
    <Switch>
      <Route path="/" component={Home} />
      <Route path="/signal/:id" component={SignalDetail} />
      <Route component={NotFound} />
    </Switch>
  );
}

function AppShell() {
  useSignalStream();
  return (
    <div className="min-h-screen bg-background">
      <Navbar />
      <Router />
    </div>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <AppShell />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
