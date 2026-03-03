import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  Globe,
  Plus,
  Trash2,
  Pencil,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Loader2,
  Eye,
  EyeOff,
  ArrowLeft,
} from "lucide-react";
import { Link } from "wouter";

interface WpConfig {
  id: number;
  user_id: number;
  owner: string;
  site_url: string;
  wp_username: string;
  is_active: number;
  created_at: string;
  updated_at: string;
}

const API_BASE = "/api/engine/admin/api/user-cms-configs";

export default function WordPressIntegrations() {
  const { toast } = useToast();
  const [modalOpen, setModalOpen] = useState(false);
  const [editingConfig, setEditingConfig] = useState<WpConfig | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<WpConfig | null>(null);
  const [showPassword, setShowPassword] = useState(false);
  const [testingId, setTestingId] = useState<number | null>(null);
  const [testResults, setTestResults] = useState<Record<number, { ok: boolean; message: string }>>({});

  const [formSiteUrl, setFormSiteUrl] = useState("");
  const [formUsername, setFormUsername] = useState("");
  const [formPassword, setFormPassword] = useState("");
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] = useState<{ ok: boolean; message: string } | null>(null);

  const { data: configs, isLoading } = useQuery<WpConfig[]>({
    queryKey: [API_BASE],
    queryFn: async () => {
      const res = await fetch(API_BASE, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch WordPress configurations");
      return res.json();
    },
  });

  const createMutation = useMutation({
    mutationFn: async (data: { site_url: string; wp_username: string; app_password: string }) => {
      const res = await apiRequest("POST", API_BASE, data);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [API_BASE] });
      toast({ title: "Site added", description: "WordPress integration has been saved." });
      closeModal();
    },
    onError: (err: Error) => {
      toast({ title: "Error", description: err.message, variant: "destructive" });
    },
  });

  const updateMutation = useMutation({
    mutationFn: async ({ id, data }: { id: number; data: Record<string, string> }) => {
      const res = await apiRequest("PUT", `${API_BASE}/${id}`, data);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [API_BASE] });
      toast({ title: "Updated", description: "WordPress integration has been updated." });
      closeModal();
    },
    onError: (err: Error) => {
      toast({ title: "Error", description: err.message, variant: "destructive" });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: number) => {
      const res = await apiRequest("DELETE", `${API_BASE}/${id}`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [API_BASE] });
      toast({ title: "Deleted", description: "WordPress integration removed. Automated publishing to this site has stopped." });
      setDeleteTarget(null);
    },
    onError: (err: Error) => {
      toast({ title: "Error", description: err.message, variant: "destructive" });
      setDeleteTarget(null);
    },
  });

  function openAddModal() {
    setEditingConfig(null);
    setFormSiteUrl("");
    setFormUsername("");
    setFormPassword("");
    setShowPassword(false);
    setValidating(false);
    setValidationResult(null);
    setModalOpen(true);
  }

  function openEditModal(config: WpConfig) {
    setEditingConfig(config);
    setFormSiteUrl(config.site_url);
    setFormUsername(config.wp_username);
    setFormPassword("");
    setShowPassword(false);
    setValidating(false);
    setValidationResult(null);
    setModalOpen(true);
  }

  function closeModal() {
    setModalOpen(false);
    setEditingConfig(null);
    setFormSiteUrl("");
    setFormUsername("");
    setFormPassword("");
    setShowPassword(false);
    setValidating(false);
    setValidationResult(null);
  }

  async function validateAndSave() {
    const siteUrl = formSiteUrl.trim();
    const username = formUsername.trim();
    const password = formPassword.trim();

    if (!siteUrl || !username) {
      toast({ title: "Validation Error", description: "Site URL and Username are required.", variant: "destructive" });
      return;
    }

    if (!editingConfig && !password) {
      toast({ title: "Validation Error", description: "Application Password is required for new sites.", variant: "destructive" });
      return;
    }

    setValidating(true);
    setValidationResult(null);

    try {
      if (editingConfig && !password) {
        const updateData: Record<string, string> = {};
        if (siteUrl !== editingConfig.site_url) updateData.site_url = siteUrl;
        if (username !== editingConfig.wp_username) updateData.wp_username = username;
        if (Object.keys(updateData).length === 0) {
          toast({ title: "No changes", description: "No fields were modified." });
          setValidating(false);
          return;
        }
        updateMutation.mutate({ id: editingConfig.id, data: updateData });
        setValidating(false);
        return;
      }

      let handshakeOk = false;
      let handshakeMsg = "";

      try {
        const probeRes = await fetch("/api/engine/admin/api/wordpress/validate-credentials", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({ site_url: siteUrl, wp_username: username, app_password: password }),
        });
        const probeData = await probeRes.json();
        handshakeOk = probeData.status === "ok";
        handshakeMsg = probeData.message || (handshakeOk ? "Connection verified" : "Validation failed");
      } catch {
        handshakeOk = false;
        handshakeMsg = "Could not reach validation service. Please try again.";
      }

      setValidationResult({ ok: handshakeOk, message: handshakeMsg });

      if (handshakeOk) {
        if (editingConfig) {
          const updateData: Record<string, string> = { app_password: password };
          if (siteUrl !== editingConfig.site_url) updateData.site_url = siteUrl;
          if (username !== editingConfig.wp_username) updateData.wp_username = username;
          updateMutation.mutate({ id: editingConfig.id, data: updateData });
        } else {
          createMutation.mutate({ site_url: siteUrl, wp_username: username, app_password: password });
        }
      }
    } catch (err: any) {
      setValidationResult({ ok: false, message: err.message || "Validation failed" });
    } finally {
      setValidating(false);
    }
  }

  async function testConnection(configId: number) {
    setTestingId(configId);
    setTestResults((prev) => {
      const next = { ...prev };
      delete next[configId];
      return next;
    });
    try {
      const res = await fetch(`${API_BASE}/${configId}/test`, {
        method: "POST",
        credentials: "include",
      });
      const data = await res.json();
      setTestResults((prev) => ({
        ...prev,
        [configId]: {
          ok: data.status === "ok",
          message: data.status === "ok" ? `Connected — ${data.site_name || data.message}` : data.message || "Connection failed",
        },
      }));
    } catch {
      setTestResults((prev) => ({ ...prev, [configId]: { ok: false, message: "Network error" } }));
    } finally {
      setTestingId(null);
    }
  }

  return (
    <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-6">
        <Link href="/" data-testid="link-back-home">
          <span className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground cursor-pointer mb-4">
            <ArrowLeft className="w-4 h-4" />
            Back to Signals
          </span>
        </Link>
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold" data-testid="text-page-title">WordPress Integrations</h1>
            <p className="text-muted-foreground">
              Manage your WordPress sites for automated signal publishing.
            </p>
          </div>
          <Button onClick={openAddModal} data-testid="button-add-site" className="shrink-0">
            <Plus className="w-4 h-4 mr-2" />
            Add Site
          </Button>
        </div>
      </div>

      {isLoading ? (
        <div className="space-y-4" data-testid="skeleton-loading">
          {[1, 2, 3].map((i) => (
            <Skeleton key={i} className="h-24 w-full rounded-lg" />
          ))}
        </div>
      ) : !configs || configs.length === 0 ? (
        <div
          className="flex flex-col items-center justify-center py-20 text-center"
          data-testid="text-empty-state"
        >
          <Globe className="w-16 h-16 text-muted-foreground/40 mb-4" />
          <h3 className="text-lg font-semibold mb-2">No WordPress sites connected</h3>
          <p className="text-muted-foreground max-w-md mb-6">
            Connect your WordPress site to automatically publish trading signals as posts.
          </p>
          <Button onClick={openAddModal} data-testid="button-add-site-empty">
            <Plus className="w-4 h-4 mr-2" />
            Add Your First Site
          </Button>
        </div>
      ) : (
        <div className="space-y-4">
          {configs.map((config) => (
            <div
              key={config.id}
              className="flex flex-col sm:flex-row sm:items-center gap-4 p-5 rounded-lg border bg-card"
              data-testid={`card-wp-site-${config.id}`}
            >
              <div className="flex items-start gap-4 flex-1 min-w-0">
                <div className="flex items-center justify-center w-10 h-10 rounded-md bg-primary/10 shrink-0 mt-0.5">
                  <Globe className="w-5 h-5 text-primary" />
                </div>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 flex-wrap">
                    <h3 className="font-semibold truncate" data-testid={`text-site-url-${config.id}`}>
                      {config.site_url.replace(/^https?:\/\//, "").replace(/\/+$/, "")}
                    </h3>
                    <Badge
                      variant={config.is_active ? "default" : "secondary"}
                      data-testid={`badge-status-${config.id}`}
                    >
                      {config.is_active ? "Active" : "Inactive"}
                    </Badge>
                  </div>
                  <p className="text-sm text-muted-foreground mt-0.5" data-testid={`text-wp-user-${config.id}`}>
                    User: {config.wp_username}
                  </p>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    Added {config.created_at ? new Date(config.created_at).toLocaleDateString() : "—"}
                  </p>
                  {testResults[config.id] && (
                    <div
                      className={`flex items-center gap-1.5 mt-2 text-sm ${testResults[config.id].ok ? "text-green-500" : "text-destructive"}`}
                      data-testid={`text-test-result-${config.id}`}
                    >
                      {testResults[config.id].ok ? (
                        <CheckCircle2 className="w-4 h-4" />
                      ) : (
                        <XCircle className="w-4 h-4" />
                      )}
                      {testResults[config.id].message}
                    </div>
                  )}
                </div>
              </div>

              <div className="flex items-center gap-2 shrink-0 sm:ml-auto">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => testConnection(config.id)}
                  disabled={testingId === config.id}
                  data-testid={`button-test-${config.id}`}
                >
                  {testingId === config.id ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <RefreshCw className="w-4 h-4" />
                  )}
                  <span className="ml-1.5">Test</span>
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => openEditModal(config)}
                  data-testid={`button-edit-${config.id}`}
                >
                  <Pencil className="w-4 h-4" />
                  <span className="ml-1.5">Edit</span>
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setDeleteTarget(config)}
                  className="text-destructive hover:text-destructive"
                  data-testid={`button-delete-${config.id}`}
                >
                  <Trash2 className="w-4 h-4" />
                  <span className="ml-1.5">Remove</span>
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}

      <Dialog open={modalOpen} onOpenChange={(open) => { if (!open) closeModal(); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle data-testid="text-modal-title">
              {editingConfig ? "Edit WordPress Site" : "Add WordPress Site"}
            </DialogTitle>
            <DialogDescription>
              {editingConfig
                ? "Update your WordPress connection details. Leave the password blank to keep the existing one."
                : "Enter your WordPress site credentials. We'll verify the connection before saving."}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <div className="space-y-2">
              <Label htmlFor="site-url">Site URL</Label>
              <Input
                id="site-url"
                placeholder="https://yourdomain.com"
                value={formSiteUrl}
                onChange={(e) => setFormSiteUrl(e.target.value)}
                data-testid="input-site-url"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="wp-username">WordPress Username</Label>
              <Input
                id="wp-username"
                placeholder="admin"
                value={formUsername}
                onChange={(e) => setFormUsername(e.target.value)}
                data-testid="input-wp-username"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="wp-password">
                Application Password
                {editingConfig && (
                  <span className="text-xs text-muted-foreground ml-2">(leave blank to keep current)</span>
                )}
              </Label>
              <div className="relative">
                <Input
                  id="wp-password"
                  type={showPassword ? "text" : "password"}
                  placeholder={editingConfig ? "••••••••••••" : "xxxx xxxx xxxx xxxx"}
                  value={formPassword}
                  onChange={(e) => setFormPassword(e.target.value)}
                  className="pr-10"
                  data-testid="input-wp-password"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  data-testid="button-toggle-password"
                >
                  {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            {validationResult && (
              <div
                className={`flex items-center gap-2 text-sm p-3 rounded-md ${
                  validationResult.ok
                    ? "bg-green-500/10 text-green-500 border border-green-500/20"
                    : "bg-destructive/10 text-destructive border border-destructive/20"
                }`}
                data-testid="text-validation-result"
              >
                {validationResult.ok ? (
                  <CheckCircle2 className="w-4 h-4 shrink-0" />
                ) : (
                  <XCircle className="w-4 h-4 shrink-0" />
                )}
                {validationResult.message}
              </div>
            )}
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={closeModal} data-testid="button-modal-cancel">
              Cancel
            </Button>
            <Button
              onClick={validateAndSave}
              disabled={validating || createMutation.isPending || updateMutation.isPending}
              data-testid="button-modal-save"
            >
              {validating || createMutation.isPending || updateMutation.isPending ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  {validating ? "Validating..." : "Saving..."}
                </>
              ) : editingConfig ? (
                "Update Site"
              ) : (
                "Verify & Save"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle data-testid="text-delete-title">Remove WordPress Site</AlertDialogTitle>
            <AlertDialogDescription data-testid="text-delete-warning">
              Are you sure you want to remove{" "}
              <strong>{deleteTarget?.site_url.replace(/^https?:\/\//, "").replace(/\/+$/, "")}</strong>?
              <br /><br />
              <span className="text-destructive font-medium">
                Warning: This will permanently stop all automated signal publishing to this site.
                Any pending publications will not be delivered.
              </span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel data-testid="button-delete-cancel">Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => deleteTarget && deleteMutation.mutate(deleteTarget.id)}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              data-testid="button-delete-confirm"
            >
              {deleteMutation.isPending ? (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Trash2 className="w-4 h-4 mr-2" />
              )}
              Remove Site
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
