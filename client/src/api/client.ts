import type {
  QueryRequest,
  QueryResponse,
  TransactionResponse,
  HealthResponse,
  ListDatabasesResponse,
  DatabaseSummary,
  DatabaseInfoResponse,
  DatabaseStatsResponse,
  DatabaseSchemaResponse,
  CreateDatabaseRequest,
  SystemResources,
  WalStatusInfo,
  ValidationInfo,
  BackupEntry,
  TokenResponse,
  CreateTokenRequest,
} from "../types/api";

export class GrafeoApiError extends Error {
  constructor(
    public status: number,
    public detail: string,
  ) {
    super(detail);
    this.name = "GrafeoApiError";
  }
}

let authToken: string | null = null;

/** Set a Bearer token for all subsequent API requests. Pass null to clear. */
export function setAuthToken(token: string | null): void {
  authToken = token;
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...options.headers as Record<string, string>,
  };
  if (authToken) {
    headers["Authorization"] = `Bearer ${authToken}`;
  }
  const res = await fetch(path, { ...options, headers });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new GrafeoApiError(res.status, body.detail ?? res.statusText);
  }
  return res.json();
}

export const api = {
  health: () => request<HealthResponse>("/health"),

  query: (body: QueryRequest) =>
    request<QueryResponse>("/query", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  cypher: (body: QueryRequest) =>
    request<QueryResponse>("/cypher", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  graphql: (body: QueryRequest) =>
    request<QueryResponse>("/graphql", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  gremlin: (body: QueryRequest) =>
    request<QueryResponse>("/gremlin", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  sparql: (body: QueryRequest) =>
    request<QueryResponse>("/sparql", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  tx: {
    begin: (database?: string) =>
      request<TransactionResponse>("/tx/begin", {
        method: "POST",
        body: database ? JSON.stringify({ database }) : undefined,
      }),

    query: (sessionId: string, body: QueryRequest) =>
      request<QueryResponse>("/tx/query", {
        method: "POST",
        headers: { "X-Session-Id": sessionId },
        body: JSON.stringify(body),
      }),

    commit: (sessionId: string) =>
      request<TransactionResponse>("/tx/commit", {
        method: "POST",
        headers: { "X-Session-Id": sessionId },
      }),

    rollback: (sessionId: string) =>
      request<TransactionResponse>("/tx/rollback", {
        method: "POST",
        headers: { "X-Session-Id": sessionId },
      }),
  },

  db: {
    list: () => request<ListDatabasesResponse>("/db"),

    create: (req: CreateDatabaseRequest) =>
      request<DatabaseSummary>("/db", {
        method: "POST",
        body: JSON.stringify(req),
      }),

    delete: (name: string) =>
      request<{ deleted: string }>(`/db/${encodeURIComponent(name)}`, {
        method: "DELETE",
      }),

    info: (name: string) =>
      request<DatabaseInfoResponse>(`/db/${encodeURIComponent(name)}`),

    stats: (name: string) =>
      request<DatabaseStatsResponse>(`/db/${encodeURIComponent(name)}/stats`),

    schema: (name: string) =>
      request<DatabaseSchemaResponse>(`/db/${encodeURIComponent(name)}/schema`),
  },

  system: {
    resources: () => request<SystemResources>("/system/resources"),
  },

  admin: {
    stats: (db: string) =>
      request<DatabaseStatsResponse>(`/admin/${encodeURIComponent(db)}/stats`),

    walStatus: (db: string) =>
      request<WalStatusInfo>(`/admin/${encodeURIComponent(db)}/wal`),

    memory: (db: string) =>
      request<Record<string, unknown>>(`/admin/${encodeURIComponent(db)}/memory`),

    validate: (db: string) =>
      request<ValidationInfo>(`/admin/${encodeURIComponent(db)}/validate`),

    checkpoint: (db: string) =>
      request<{ success: boolean }>(`/admin/${encodeURIComponent(db)}/wal/checkpoint`, {
        method: "POST",
      }),
  },

  backup: {
    create: (db: string) =>
      request<BackupEntry>(`/admin/${encodeURIComponent(db)}/backup`, {
        method: "POST",
      }),

    list: (db: string) =>
      request<BackupEntry[]>(`/admin/${encodeURIComponent(db)}/backups`),

    listAll: () => request<BackupEntry[]>("/admin/backups"),

    restore: (db: string, backup: string) =>
      request<{ restored: boolean }>(`/admin/${encodeURIComponent(db)}/restore`, {
        method: "POST",
        body: JSON.stringify({ backup }),
      }),

    remove: (filename: string) =>
      request<{ deleted: boolean }>(`/admin/backups/${encodeURIComponent(filename)}`, {
        method: "DELETE",
      }),

    downloadUrl: (filename: string) =>
      `/admin/backups/download/${encodeURIComponent(filename)}`,
  },

  tokens: {
    list: () => request<TokenResponse[]>("/admin/tokens"),

    create: (req: CreateTokenRequest) =>
      request<TokenResponse>("/admin/tokens", {
        method: "POST",
        body: JSON.stringify(req),
      }),

    get: (id: string) =>
      request<TokenResponse>(`/admin/tokens/${encodeURIComponent(id)}`),

    delete: (id: string) =>
      request<{ deleted: boolean }>(`/admin/tokens/${encodeURIComponent(id)}`, {
        method: "DELETE",
      }),
  },
};
