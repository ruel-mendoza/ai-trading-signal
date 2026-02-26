import { useState, useEffect } from "react";
import { Link, useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { TrendingUp, Menu, Shield, LogIn } from "lucide-react";

export function Navbar() {
  const [location] = useLocation();
  const [isAdmin, setIsAdmin] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);

  useEffect(() => {
    fetch("/api/engine/admin/api/auth-status", { credentials: "include" })
      .then((res) => res.json())
      .then((data) => setIsAdmin(data.authenticated === true))
      .catch(() => setIsAdmin(false));
  }, [location]);

  const adminHref = isAdmin
    ? "/api/engine/admin/"
    : "/api/engine/admin/login";

  return (
    <header className="border-b bg-card/50 backdrop-blur-sm sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          <Link href="/" data-testid="link-home">
            <div className="flex items-center gap-3 cursor-pointer">
              <div className="flex items-center justify-center w-9 h-9 rounded-md bg-primary text-primary-foreground">
                <TrendingUp className="w-5 h-5" />
              </div>
              <div>
                <h1 className="text-lg font-semibold leading-tight">
                  DailyForex Premium
                </h1>
                <p className="text-xs text-muted-foreground leading-tight">
                  AI-Powered Trading Signals
                </p>
              </div>
            </div>
          </Link>

          <a
            href={adminHref}
            className="hidden sm:inline-flex"
            data-testid="link-admin-desktop"
          >
            <Button variant="default" size="sm">
              {isAdmin ? (
                <>
                  <Shield className="w-4 h-4 mr-2" />
                  Admin Dashboard
                </>
              ) : (
                <>
                  <LogIn className="w-4 h-4 mr-2" />
                  Admin Login
                </>
              )}
            </Button>
          </a>

          <div className="sm:hidden">
            <Sheet open={mobileOpen} onOpenChange={setMobileOpen}>
              <SheetTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  data-testid="button-mobile-menu"
                >
                  <Menu className="w-5 h-5" />
                </Button>
              </SheetTrigger>
              <SheetContent side="right" className="w-64">
                <nav className="flex flex-col gap-4 mt-8">
                  <Link
                    href="/"
                    onClick={() => setMobileOpen(false)}
                    data-testid="link-home-mobile"
                  >
                    <div className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-accent cursor-pointer">
                      <TrendingUp className="w-5 h-5" />
                      <span className="font-medium">Signals</span>
                    </div>
                  </Link>
                  <a
                    href={adminHref}
                    onClick={() => setMobileOpen(false)}
                    data-testid="link-admin-mobile"
                  >
                    <div className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-accent cursor-pointer">
                      {isAdmin ? (
                        <>
                          <Shield className="w-5 h-5" />
                          <span className="font-medium">Admin Dashboard</span>
                        </>
                      ) : (
                        <>
                          <LogIn className="w-5 h-5" />
                          <span className="font-medium">Admin Login</span>
                        </>
                      )}
                    </div>
                  </a>
                </nav>
              </SheetContent>
            </Sheet>
          </div>
        </div>
      </div>
    </header>
  );
}
