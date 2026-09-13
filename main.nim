import std/[asyncdispatch, asynchttpserver, asyncnet, json, strutils, strformat,
  os, times, tables, sets, sequtils, math, algorithm, random, options,
  locks, hashes, base64, uri, deques, monotimes, sha1, mimetypes,
  httpclient, streams, parseutils, osproc, asyncstreams, net, nativesockets,
  atomics]

const
  SqliteLib = when defined(windows): "sqlite3_64.dll" elif defined(macosx): "libsqlite3.dylib" else: "libsqlite3.so(|.0)"
  DefaultRequestyBaseUrl = "https://router.requesty.ai/v1"
  DefaultCerebrasBaseUrl = "https://api.cerebras.ai/v1"
  DefaultGeminiBaseUrl = "https://generativelanguage.googleapis.com/v1"
  DefaultInstaVmBaseUrl = "https://api.instavm.io"
  Gpt6AstraModel = "openai/gpt-6-astra:flex"
  Glm52Model = "zai/glm-5.2"
  Gemini38Model = "gemini-3.8-flash"
  MiniMaxM3Model = "minimaxi/minimax-m3"
  Grok43Model = "grok-4.3"
  System1HzInterval = 50
  System2HzInterval = 1000
  DbBusyTimeoutMs = 15000
  EmbeddingDim = 128
  RrfK = 60.0
  VmLifetimeSeconds = 2_592_000
  VmDefaultMemoryMb = 4096
  VmDefaultVcpuCount = 4
  DefaultMaxRequestBodyBytes = 67_108_864

var
  DbFile {.threadvar.}: string
  WorkspaceRoot {.threadvar.}: string
  knowledgeRoot {.threadvar.}: string
  RequestyBaseUrl {.threadvar.}: string
  CerebrasBaseUrl {.threadvar.}: string
  GeminiBaseUrl {.threadvar.}: string
  InstaVmBaseUrl {.threadvar.}: string
  CerebrasGemma4Model {.threadvar.}: string
  PromptConfigFile {.threadvar.}: string
  ReferenceSkillRoot {.threadvar.}: string
  PublicRoot {.threadvar.}: string
  defaultTenantId {.threadvar.}: string
  serverPort: int

proc envTrim(name, fallback: string): string =
  let v = getEnv(name, "").strip()
  if v.len > 0: v else: fallback

proc stripTrailingSlash(s: string): string =
  result = s.strip()
  while result.len > 0 and result[^1] == '/':
    result.setLen(result.len - 1)

proc nowF(): float = epochTime()

proc canonical(node: JsonNode): string =
  if node.isNil:
    return "null"
  case node.kind
  of JObject:
    var keys: seq[string] = @[]
    for k, _ in node.fields:
      keys.add(k)
    keys.sort()
    var parts: seq[string] = @[]
    for k in keys:
      parts.add(escapeJson(k) & ":" & canonical(node.fields[k]))
    result = "{" & parts.join(",") & "}"
  of JArray:
    var parts: seq[string] = @[]
    for it in node.elems:
      parts.add(canonical(it))
    result = "[" & parts.join(",") & "]"
  of JString:
    result = escapeJson(node.getStr())
  of JInt:
    result = $node.getBiggestInt()
  of JFloat:
    result = formatFloat(node.getFloat(), ffDefault, 16)
  of JBool:
    result = if node.getBool(): "true" else: "false"
  of JNull:
    result = "null"

proc sha1Hex(s: string): string =
  result = ($secureHash(s)).toLowerAscii()

var
  rngLock: Lock
  globalRng: Rand

proc newId(prefix: string): string =
  acquire(rngLock)
  defer: release(rngLock)
  const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
  var buf = newStringOfCap(24)
  for _ in 0 ..< 20:
    buf.add(alphabet[globalRng.rand(alphabet.high)])
  result = prefix & "_" & buf

type
  SqliteDb = ptr object
  SqliteStmt = ptr object

{.push importc, cdecl, dynlib: SqliteLib.}
proc sqlite3_open_v2(filename: cstring, ppDb: ptr SqliteDb, flags: cint, zVfs: cstring): cint
proc sqlite3_close_v2(db: SqliteDb): cint
proc sqlite3_exec(db: SqliteDb, sql: cstring, callback: pointer, arg: pointer, errmsg: ptr cstring): cint
proc sqlite3_prepare_v2(db: SqliteDb, zSql: cstring, nByte: cint, ppStmt: ptr SqliteStmt, pzTail: ptr cstring): cint
proc sqlite3_step(pStmt: SqliteStmt): cint
proc sqlite3_finalize(pStmt: SqliteStmt): cint
proc sqlite3_reset(pStmt: SqliteStmt): cint
proc sqlite3_bind_text(pStmt: SqliteStmt, idx: cint, value: cstring, n: cint, destructor: pointer): cint
proc sqlite3_bind_int64(pStmt: SqliteStmt, idx: cint, value: int64): cint
proc sqlite3_bind_double(pStmt: SqliteStmt, idx: cint, value: float64): cint
proc sqlite3_bind_null(pStmt: SqliteStmt, idx: cint): cint
proc sqlite3_column_count(pStmt: SqliteStmt): cint
proc sqlite3_column_text(pStmt: SqliteStmt, iCol: cint): cstring
proc sqlite3_column_int64(pStmt: SqliteStmt, iCol: cint): int64
proc sqlite3_column_double(pStmt: SqliteStmt, iCol: cint): float64
proc sqlite3_column_type(pStmt: SqliteStmt, iCol: cint): cint
proc sqlite3_column_name(pStmt: SqliteStmt, iCol: cint): cstring
proc sqlite3_errmsg(db: SqliteDb): cstring
proc sqlite3_free(p: pointer)
proc sqlite3_busy_timeout(db: SqliteDb, ms: cint): cint
proc sqlite3_last_insert_rowid(db: SqliteDb): int64
proc sqlite3_changes(db: SqliteDb): cint
{.pop.}

const
  SQLITE_OK = 0.cint
  SQLITE_ROW = 100.cint
  SQLITE_DONE = 101.cint
  SQLITE_NULL = 5.cint
  SQLITE_OPEN_READWRITE = 0x00000002.cint
  SQLITE_OPEN_CREATE = 0x00000004.cint
  SQLITE_OPEN_FULLMUTEX = 0x00010000.cint

let SQLITE_TRANSIENT = cast[pointer](-1)

type
  DbError = object of CatchableError
  Row = Table[string, JsonNode]
  Store = ref object
    handle: SqliteDb
    lock: Lock
    path: string
  SqlOperation = object
    sql: string
    params: seq[JsonNode]

var
  store {.threadvar.}: Store
  fts5Available {.threadvar.}: bool

proc raiseDb(s: Store, ctx: string) =
  raise newException(DbError, ctx & ": " & $sqlite3_errmsg(s.handle))

proc openStore(path: string): Store =
  var db: SqliteDb
  let flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE or SQLITE_OPEN_FULLMUTEX
  let rc = sqlite3_open_v2(path.cstring, addr db, flags, nil)
  if rc != SQLITE_OK:
    var message = "cannot open database: " & path
    if not db.isNil:
      let err = sqlite3_errmsg(db)
      if err != nil:
        message.add(": " & $err)
      discard sqlite3_close_v2(db)
      db = nil
    raise newException(DbError, message)
  result = Store(handle: db, path: path)
  initLock(result.lock)
  if sqlite3_busy_timeout(db, DbBusyTimeoutMs.cint) != SQLITE_OK:
    let message = $sqlite3_errmsg(db)
    discard sqlite3_close_v2(db)
    result.handle = nil
    deinitLock(result.lock)
    raise newException(DbError, "cannot set SQLite busy timeout: " & message)

proc execRawUnlocked(s: Store, sql: string) =
  var err: cstring
  if sqlite3_exec(s.handle, sql.cstring, nil, nil, addr err) != SQLITE_OK:
    var msg = "sqlite error"
    if err != nil:
      msg = $err
      sqlite3_free(err)
    raise newException(DbError, msg & " :: " & sql)

proc execRaw(s: Store, sql: string) =
  acquire(s.lock)
  defer: release(s.lock)
  s.execRawUnlocked(sql)

proc bindParams(s: Store, st: SqliteStmt, params: seq[JsonNode]) =
  for i, p in params:
    let idx = (i + 1).cint
    var rc: cint
    case p.kind
    of JNull:
      rc = sqlite3_bind_null(st, idx)
    of JInt:
      rc = sqlite3_bind_int64(st, idx, p.getBiggestInt())
    of JFloat:
      rc = sqlite3_bind_double(st, idx, p.getFloat())
    of JBool:
      rc = sqlite3_bind_int64(st, idx, if p.getBool(): 1 else: 0)
    of JString:
      let v = p.getStr()
      rc = sqlite3_bind_text(st, idx, v.cstring, v.len.cint, SQLITE_TRANSIENT)
    else:
      let v = $p
      rc = sqlite3_bind_text(st, idx, v.cstring, v.len.cint, SQLITE_TRANSIENT)
    if rc != SQLITE_OK:
      s.raiseDb("bind failed at parameter " & $idx)

proc execUnlocked(s: Store, sql: string, params: seq[JsonNode] = @[]): int64 =
  var st: SqliteStmt
  if sqlite3_prepare_v2(s.handle, sql.cstring, -1.cint, addr st, nil) != SQLITE_OK:
    s.raiseDb("prepare failed")
  defer: discard sqlite3_finalize(st)
  s.bindParams(st, params)
  let rc = sqlite3_step(st)
  if rc != SQLITE_DONE and rc != SQLITE_ROW:
    s.raiseDb("execute failed")
  result = sqlite3_last_insert_rowid(s.handle)

proc exec(s: Store, sql: string, params: seq[JsonNode] = @[]): int64 =
  acquire(s.lock)
  defer: release(s.lock)
  result = s.execUnlocked(sql, params)

proc execTransaction(s: Store, ops: openArray[SqlOperation]) =
  acquire(s.lock)
  defer: release(s.lock)
  s.execRawUnlocked("BEGIN IMMEDIATE;")
  try:
    for op in ops:
      discard s.execUnlocked(op.sql, op.params)
    s.execRawUnlocked("COMMIT;")
  except CatchableError:
    try:
      s.execRawUnlocked("ROLLBACK;")
    except CatchableError:
      discard
    raise

proc query(s: Store, sql: string, params: seq[JsonNode] = @[]): seq[Row] =
  acquire(s.lock)
  defer: release(s.lock)
  var st: SqliteStmt
  if sqlite3_prepare_v2(s.handle, sql.cstring, -1.cint, addr st, nil) != SQLITE_OK:
    s.raiseDb("prepare failed")
  defer: discard sqlite3_finalize(st)
  s.bindParams(st, params)
  result = @[]
  while true:
    let rc = sqlite3_step(st)
    if rc == SQLITE_DONE:
      break
    if rc != SQLITE_ROW:
      s.raiseDb("query step failed")
    var row = initTable[string, JsonNode]()
    let count = sqlite3_column_count(st)
    for i in 0 ..< count:
      let key = $sqlite3_column_name(st, i)
      if sqlite3_column_type(st, i) == SQLITE_NULL:
        row[key] = newJNull()
      else:
        let raw = sqlite3_column_text(st, i)
        row[key] = if raw == nil: newJNull() else: newJString($raw)
    result.add(row)

proc getStr(r: Row, key: string, fallback = ""): string =
  if r.hasKey(key) and r[key].kind == JString: r[key].getStr() else: fallback

proc getInt(r: Row, key: string, fallback: int64 = 0): int64 =
  if r.hasKey(key) and r[key].kind == JString:
    try:
      return parseBiggestInt(r[key].getStr())
    except CatchableError:
      return fallback
  fallback

proc getFloat(r: Row, key: string, fallback = 0.0): float =
  if r.hasKey(key) and r[key].kind == JString:
    try:
      return parseFloat(r[key].getStr())
    except CatchableError:
      return fallback
  fallback

proc getJson(r: Row, key: string, fallback: JsonNode = nil): JsonNode =
  let raw = r.getStr(key, "")
  if raw.len == 0:
    if fallback.isNil:
      return newJObject()
    return copy(fallback)
  try:
    return parseJson(raw)
  except CatchableError as e:
    raise newException(DbError, "malformed persisted JSON in column " & key & ": " & e.msg)

proc columnExists(s: Store, tableName, columnName: string): bool =
  if tableName.len == 0 or columnName.len == 0:
    return false
  for row in s.query("PRAGMA table_info(" & tableName & ")"):
    if row.getStr("name") == columnName:
      return true
  false

proc migrate(s: Store) =
  s.execRaw("PRAGMA journal_mode=WAL;")
  s.execRaw("PRAGMA synchronous=NORMAL;")
  s.execRaw("PRAGMA foreign_keys=ON;")
  s.execRaw("PRAGMA busy_timeout=15000;")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS tenants (
  tenant_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  api_key_hash TEXT NOT NULL DEFAULT '',
  token_budget INTEGER NOT NULL DEFAULT 9223372036854775807,
  tokens_used INTEGER NOT NULL DEFAULT 0,
  allowed_tools TEXT NOT NULL DEFAULT '[]',
  created_at REAL NOT NULL
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS browser_sessions (
  session_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  expires_at REAL NOT NULL,
  created_at REAL NOT NULL,
  FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
);""")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_browser_sessions_token ON browser_sessions(token_hash, expires_at);")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  title TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  initial_state_json TEXT NOT NULL,
  state_json TEXT NOT NULL,
  latest_obs_json TEXT NOT NULL,
  status TEXT NOT NULL,
  step_index INTEGER NOT NULL DEFAULT 0,
  max_steps INTEGER NOT NULL DEFAULT 0,
  tokens_used INTEGER NOT NULL DEFAULT 0,
  terminal_reason TEXT NOT NULL DEFAULT '',
  verified INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS checkpoints (
  ckpt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  state_json TEXT NOT NULL,
  obs_json TEXT NOT NULL,
  action_json TEXT NOT NULL,
  patch_json TEXT NOT NULL DEFAULT '{}',
  receipt_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL,
  UNIQUE(task_id, step_index),
  FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS task_events (
  task_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_json TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY(task_id, sequence),
  FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS chat_jobs (
  job_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  request_json TEXT NOT NULL,
  status TEXT NOT NULL,
  content TEXT NOT NULL DEFAULT '',
  reasoning TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT '',
  prompt_tokens INTEGER NOT NULL DEFAULT 0,
  completion_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  model TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  usage_json TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
);""")
  if not s.columnExists("chat_jobs", "model"):
    s.execRaw("ALTER TABLE chat_jobs ADD COLUMN model TEXT NOT NULL DEFAULT '';")
  if not s.columnExists("chat_jobs", "task_id"):
    s.execRaw("ALTER TABLE chat_jobs ADD COLUMN task_id TEXT NOT NULL DEFAULT '';")
  if not s.columnExists("chat_jobs", "usage_json"):
    s.execRaw("ALTER TABLE chat_jobs ADD COLUMN usage_json TEXT NOT NULL DEFAULT '{}';")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS chat_job_events (
  job_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_json TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY(job_id, sequence),
  FOREIGN KEY(job_id) REFERENCES chat_jobs(job_id) ON DELETE CASCADE
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  name TEXT NOT NULL,
  path TEXT NOT NULL,
  kind TEXT NOT NULL,
  mime_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS skills (
  skill_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'local',
  name TEXT NOT NULL,
  domain TEXT NOT NULL,
  trigger_spec TEXT NOT NULL,
  procedure_spec TEXT NOT NULL,
  skill_code TEXT NOT NULL DEFAULT '',
  preconditions_json TEXT NOT NULL DEFAULT '[]',
  postconditions_json TEXT NOT NULL DEFAULT '[]',
  failure_modes_json TEXT NOT NULL DEFAULT '[]',
  version INTEGER NOT NULL DEFAULT 1,
  active INTEGER NOT NULL DEFAULT 1,
  success_count INTEGER NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  reward REAL NOT NULL DEFAULT 0.0,
  embedding_json TEXT NOT NULL DEFAULT '[]',
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  UNIQUE(tenant_id, name)
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS knowledge_docs (
  doc_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'local',
  slug TEXT NOT NULL,
  category TEXT NOT NULL,
  path TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  git_commit_hash TEXT NOT NULL DEFAULT '',
  content TEXT NOT NULL DEFAULT '',
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL DEFAULT 0,
  UNIQUE(tenant_id, slug)
);""")
  if not s.columnExists("knowledge_docs", "content"):
    s.execRaw("ALTER TABLE knowledge_docs ADD COLUMN content TEXT NOT NULL DEFAULT '';")
  if not s.columnExists("knowledge_docs", "updated_at"):
    s.execRaw("ALTER TABLE knowledge_docs ADD COLUMN updated_at REAL NOT NULL DEFAULT 0;")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS raw_traces (
  trace_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  initial_state_json TEXT NOT NULL,
  skill_id TEXT NOT NULL DEFAULT '',
  action_json TEXT NOT NULL,
  obs_json TEXT NOT NULL,
  delta_json TEXT NOT NULL,
  post_state_json TEXT NOT NULL,
  success INTEGER NOT NULL,
  latency_ms INTEGER NOT NULL,
  receipt_json TEXT NOT NULL,
  immutable_hash TEXT NOT NULL UNIQUE,
  created_at REAL NOT NULL
);""")
  s.execRaw("""
CREATE TRIGGER IF NOT EXISTS raw_traces_no_update
BEFORE UPDATE ON raw_traces
BEGIN
  SELECT RAISE(ABORT, 'raw_traces is an immutable ledger');
END;""")
  s.execRaw("""
CREATE TRIGGER IF NOT EXISTS raw_traces_no_delete
BEFORE DELETE ON raw_traces
BEGIN
  SELECT RAISE(ABORT, 'raw_traces is an immutable ledger');
END;""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS cognition (
  cog_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  vector_json TEXT NOT NULL,
  gate REAL NOT NULL,
  subgoal TEXT NOT NULL,
  strategy TEXT NOT NULL,
  route_json TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL
);""")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_cognition_task ON cognition(task_id, created_at DESC);")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS reflections (
  reflection_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  patch_json TEXT NOT NULL,
  failure_point TEXT NOT NULL,
  pivot_action TEXT NOT NULL,
  attribution TEXT NOT NULL,
  verifier_report_json TEXT NOT NULL,
  created_at REAL NOT NULL
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS policy_weights (
  tenant_id TEXT NOT NULL,
  token TEXT NOT NULL,
  weight REAL NOT NULL,
  updates INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL,
  PRIMARY KEY(tenant_id, token)
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS diagnostics (
  diagnostic_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  expectation_json TEXT NOT NULL,
  created_at REAL NOT NULL
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS meta_agent_events (
  event_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  signature TEXT NOT NULL,
  occurrences INTEGER NOT NULL,
  candidate_json TEXT NOT NULL,
  validation_json TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  UNIQUE(tenant_id, signature)
);""")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_meta_events_tenant ON meta_agent_events(tenant_id, status, updated_at DESC);")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS subagents (
  agent_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  parent_agent_id TEXT NOT NULL DEFAULT '',
  model_role TEXT NOT NULL,
  name TEXT NOT NULL DEFAULT '',
  goal TEXT NOT NULL,
  instructions TEXT NOT NULL DEFAULT '',
  context_json TEXT NOT NULL DEFAULT '{}',
  messages_json TEXT NOT NULL DEFAULT '[]',
  state_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  result TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT '',
  stop_requested INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);""")
  s.execRaw("""
CREATE TABLE IF NOT EXISTS subagent_events (
  agent_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_json TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY(agent_id, sequence),
  FOREIGN KEY(agent_id) REFERENCES subagents(agent_id) ON DELETE CASCADE
);""")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_subagents_task ON subagents(task_id, parent_agent_id, created_at);")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_subagents_status ON subagents(status, updated_at);")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_subagent_events_agent ON subagent_events(agent_id, sequence);")
  try:
    s.execRaw("CREATE VIRTUAL TABLE IF NOT EXISTS skill_fts USING fts5(skill_id UNINDEXED, tenant_id UNINDEXED, name, domain, trigger_spec, procedure_spec, tokenize='porter ascii');")
    s.execRaw("DROP TRIGGER IF EXISTS skills_ai;")
    s.execRaw("DROP TRIGGER IF EXISTS skills_ad;")
    s.execRaw("DROP TRIGGER IF EXISTS skills_au;")
    s.execRaw("""
CREATE TRIGGER skills_ai AFTER INSERT ON skills BEGIN
  INSERT INTO skill_fts(skill_id, tenant_id, name, domain, trigger_spec, procedure_spec)
  VALUES (new.skill_id, new.tenant_id, new.name, new.domain, new.trigger_spec, new.procedure_spec || ' ' || new.skill_code);
END;""")
    s.execRaw("""
CREATE TRIGGER skills_ad AFTER DELETE ON skills BEGIN
  DELETE FROM skill_fts WHERE skill_id = old.skill_id;
END;""")
    s.execRaw("""
CREATE TRIGGER skills_au AFTER UPDATE ON skills BEGIN
  DELETE FROM skill_fts WHERE skill_id = old.skill_id;
  INSERT INTO skill_fts(skill_id, tenant_id, name, domain, trigger_spec, procedure_spec)
  VALUES (new.skill_id, new.tenant_id, new.name, new.domain, new.trigger_spec, new.procedure_spec || ' ' || new.skill_code);
END;""")
    s.execRaw("DELETE FROM skill_fts;")
    s.execRaw("INSERT INTO skill_fts(skill_id, tenant_id, name, domain, trigger_spec, procedure_spec) SELECT skill_id, tenant_id, name, domain, trigger_spec, procedure_spec || ' ' || skill_code FROM skills;")
    fts5Available = true
  except CatchableError:
    fts5Available = false
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_raw_traces_task ON raw_traces(task_id, step_index, created_at);")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_reflections_task ON reflections(task_id, created_at DESC);")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_tasks_updated ON tasks(updated_at DESC);")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id, sequence);")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_chat_jobs_updated ON chat_jobs(updated_at DESC);")
  s.execRaw("CREATE INDEX IF NOT EXISTS idx_chat_job_events_job ON chat_job_events(job_id, sequence);")

proc ensureLocalTenant(): string =
  let rows = store.query("SELECT tenant_id FROM tenants WHERE name='local' ORDER BY created_at ASC LIMIT 1")
  if rows.len > 0:
    return rows[0].getStr("tenant_id")
  let id = "local"
  discard store.exec("INSERT OR IGNORE INTO tenants (tenant_id, name, api_key_hash, token_budget, tokens_used, allowed_tools, created_at) VALUES (?,'local','',9223372036854775807,0,'[]',?)", @[%id, %nowF()])
  id

const StopWords = ["the", "and", "for", "with", "that", "this", "from", "into", "have", "has", "are", "was", "were", "not", "but", "you", "your", "then", "than", "will", "can", "any", "all", "its"]

proc tokenizeText(s: string): seq[string] =
  result = @[]
  var current = newStringOfCap(32)
  for ch in s:
    if ch.isAlphaNumeric() or ch == '_':
      current.add(ch.toLowerAscii())
    else:
      if current.len >= 2:
        result.add(current)
      current.setLen(0)
  if current.len >= 2:
    result.add(current)

proc contentTerms(s: string): seq[string] =
  result = @[]
  for term in tokenizeText(s):
    if term notin StopWords:
      result.add(term)

proc positiveEnvInt(name: string, fallback: int): int

proc textEmbedding(text: string, dims: int = EmbeddingDim): seq[float] =
  let input = text.strip()
  if input.len == 0:
    return @[]
  let key = getEnv("REQUESTY_API_KEY", "").strip()
  if key.len == 0:
    raise newException(IOError, "REQUESTY_API_KEY is required for semantic embeddings")
  let model = envTrim("EMBEDDING_MODEL", "openai/text-embedding-3-small")
  let baseUrl = stripTrailingSlash(if RequestyBaseUrl.len > 0: RequestyBaseUrl else: DefaultRequestyBaseUrl)
  var client = newHttpClient(maxRedirects = 0, timeout = positiveEnvInt("EMBEDDING_HTTP_TIMEOUT_MS", 60000))
  defer: client.close()
  client.headers = newHttpHeaders({
    "Authorization": "Bearer " & key,
    "Content-Type": "application/json",
    "Accept": "application/json"
  })
  var requestBody = %*{"model": model, "input": input}
  if dims > 0:
    requestBody["dimensions"] = %dims
  let response = client.request(baseUrl & "/embeddings", httpMethod = HttpPost, body = $requestBody)
  if response.code.int < 200 or response.code.int >= 300:
    raise newException(IOError, "embedding provider status " & $response.code.int & ": " & response.body)
  if response.body.len > positiveEnvInt("EMBEDDING_RESPONSE_MAX_BYTES", 8 * 1024 * 1024):
    raise newException(IOError, "embedding response exceeded configured size limit")
  let parsed = parseJson(response.body)
  if parsed{"data"}.kind != JArray or parsed["data"].elems.len == 0 or parsed["data"][0]{"embedding"}.kind != JArray:
    raise newException(IOError, "embedding provider returned no embedding vector")
  for item in parsed["data"][0]["embedding"].elems:
    if item.kind notin {JInt, JFloat}:
      raise newException(IOError, "embedding provider returned a non-numeric embedding value")
    result.add(item.getFloat())
  if result.len == 0:
    raise newException(IOError, "embedding provider returned an empty embedding vector")

proc cosineSimilarity(a, b: seq[float]): float =
  if a.len == 0 or a.len != b.len:
    return 0.0
  result = 0.0
  for i in 0 ..< a.len:
    result += a[i] * b[i]

proc embToJson(v: seq[float]): string =
  var arr = newJArray()
  for value in v:
    arr.add(%value)
  result = $arr

proc jsonToEmb(s: string): seq[float] =
  result = @[]
  if s.len == 0:
    return
  try:
    let node = parseJson(s)
    if node.kind == JArray:
      for item in node.elems:
        result.add(item.getFloat())
  except CatchableError:
    result = @[]

proc rrfFuse(dense, sparse: seq[(string, float)], limit: int): seq[(string, float)] =
  var fused = initTable[string, float]()
  for i, item in dense:
    fused[item[0]] = fused.getOrDefault(item[0], 0.0) + 1.0 / (RrfK + (i + 1).float)
  for i, item in sparse:
    fused[item[0]] = fused.getOrDefault(item[0], 0.0) + 1.0 / (RrfK + (i + 1).float)
  result = @[]
  for key, score in fused:
    result.add((key, score))
  result.sort(proc(a, b: (string, float)): int = cmp(b[1], a[1]))
  if limit >= 0 and result.len > limit:
    result.setLen(limit)

proc updatePolicyWeight(tenantId, token: string, delta: float) =
  let key = token.strip().toLowerAscii()
  if key.len == 0:
    return
  discard store.exec("INSERT INTO policy_weights (tenant_id,token,weight,updates,updated_at) VALUES (?,?,?,1,?) ON CONFLICT(tenant_id,token) DO UPDATE SET weight=policy_weights.weight*0.95+excluded.weight*0.05,updates=policy_weights.updates+1,updated_at=excluded.updated_at", @[%tenantId, %key, %delta, %nowF()])

proc searchSkills(tenantId, queryText: string, limit: int): seq[Row] =
  let rows = store.query("SELECT * FROM skills WHERE tenant_id=? AND active=1", @[%tenantId])
  if rows.len == 0:
    return @[]
  let actualLimit = max(1, limit)
  let qEmb = textEmbedding(queryText)
  var dense: seq[(string, float)] = @[]
  var sparse: seq[(string, float)] = @[]
  var byId = initTable[string, Row]()
  let qTerms = contentTerms(queryText).toHashSet()
  for row in rows:
    let id = row.getStr("skill_id")
    if id.len == 0:
      continue
    byId[id] = row
    let corpus = row.getStr("name") & " " & row.getStr("domain") & " " & row.getStr("trigger_spec") & " " & row.getStr("procedure_spec") & " " & row.getStr("skill_code")
    var embedding = jsonToEmb(row.getStr("embedding_json"))
    if embedding.len != qEmb.len:
      embedding = textEmbedding(corpus)
      discard store.exec("UPDATE skills SET embedding_json=?,updated_at=? WHERE skill_id=?", @[%embToJson(embedding), %nowF(), %id])
    dense.add((id, cosineSimilarity(qEmb, embedding)))
    let overlap = intersection(qTerms, contentTerms(corpus).toHashSet()).len.float
    if overlap > 0.0:
      sparse.add((id, overlap))
  dense.sort(proc(a, b: (string, float)): int = cmp(b[1], a[1]))
  sparse.sort(proc(a, b: (string, float)): int = cmp(b[1], a[1]))
  if fts5Available:
    let terms = contentTerms(queryText)
    if terms.len > 0:
      let ftsQuery = terms.mapIt(it.replace("\"", "")).join(" OR ")
      try:
        let ftsRows = store.query("SELECT skill_id, rank FROM skill_fts WHERE skill_fts MATCH ? AND tenant_id=? ORDER BY rank LIMIT ?", @[%ftsQuery, %tenantId, %(actualLimit * 8)])
        sparse = @[]
        for row in ftsRows:
          sparse.add((row.getStr("skill_id"), -row.getFloat("rank", 0.0)))
      except CatchableError:
        discard
  let fused = rrfFuse(dense, sparse, actualLimit * 4)
  var scored: seq[(Row, float)] = @[]
  for item in fused:
    if not byId.hasKey(item[0]):
      continue
    let row = byId[item[0]]
    let successes = row.getInt("success_count").float
    let failures = row.getInt("failure_count").float
    let prior = (successes + 1.0) / (successes + failures + 2.0)
    scored.add((row, item[1] * 100.0 + prior * 2.0 + row.getFloat("reward") * 0.5))
  scored.sort(proc(a, b: (Row, float)): int = cmp(b[1], a[1]))
  result = @[]
  for i in 0 ..< min(actualLimit, scored.len):
    result.add(scored[i][0])

proc learnedPolicySignals(tenantId, context: string, limit: int = 16): string =
  let rows = store.query("SELECT token,weight,updates FROM policy_weights WHERE tenant_id=? AND ABS(weight)>=0.02 ORDER BY ABS(weight) DESC,updates DESC", @[%tenantId])
  if rows.len == 0:
    return "No learned policy signals are available yet."
  let ctxTerms = contentTerms(context).toHashSet()
  var ranked: seq[(string, float, float, int64)] = @[]
  for row in rows:
    let key = row.getStr("token").strip()
    if key.len == 0:
      continue
    let parts = contentTerms(key)
    var overlap = 0
    for part in parts:
      if part in ctxTerms:
        inc overlap
    let relevance = if parts.len == 0: 0.0 else: overlap.float / parts.len.float
    let weight = row.getFloat("weight")
    let updates = row.getInt("updates")
    ranked.add((key, abs(weight) * (1.0 + relevance * 2.0), weight, updates))
  ranked.sort(proc(a, b: (string, float, float, int64)): int = cmp(b[1], a[1]))
  var lines: seq[string] = @[]
  for i in 0 ..< min(max(1, limit), ranked.len):
    let item = ranked[i]
    lines.add((if item[2] >= 0.0: "FAVOR " else: "AVOID ") & item[0] & " weight=" & formatFloat(item[2], ffDecimal, 4) & " evidence=" & $item[3])
  result = lines.join("\n")

proc searchKnowledge(tenantId, queryText: string, limit: int): seq[Row] =
  let rows = store.query("SELECT * FROM knowledge_docs WHERE tenant_id=?", @[%tenantId])
  if rows.len == 0:
    return @[]
  let actualLimit = max(1, limit)
  let qEmb = textEmbedding(queryText)
  let qTerms = contentTerms(queryText).toHashSet()
  var scored: seq[(Row, float)] = @[]
  for row in rows:
    let corpus = row.getStr("slug") & " " & row.getStr("category") & " " & row.getStr("content")
    let dense = cosineSimilarity(qEmb, textEmbedding(corpus))
    let sparse = intersection(qTerms, contentTerms(corpus).toHashSet()).len.float
    scored.add((row, dense + sparse * 0.2))
  scored.sort(proc(a, b: (Row, float)): int = cmp(b[1], a[1]))
  result = @[]
  for i in 0 ..< min(actualLimit, scored.len):
    result.add(scored[i][0])

proc positiveEnvInt(name: string, fallback: int): int =
  let raw = getEnv(name, "").strip()
  if raw.len == 0:
    return fallback
  try:
    let parsed = parseInt(raw)
    if parsed > 0:
      return parsed
  except ValueError:
    discard
  fallback

proc digestOf(node: JsonNode): string =
  sha1Hex(canonical(node))

proc stalenessEncoding(elapsed: float): JsonNode =
  result = newJArray()
  for k in 0 ..< 4:
    let freq = pow(10.0, float(k) * 2.0 / 8.0)
    result.add(%sin(elapsed / freq))
    result.add(%cos(elapsed / freq))

proc policyNgrams(text: string, maxN: int = 3): seq[string] =
  let terms = contentTerms(text)
  var seen = initHashSet[string]()
  for n in 1 .. max(1, maxN):
    if terms.len < n:
      break
    for i in 0 .. terms.len - n:
      let key = terms[i ..< i + n].join(" ")
      if key.len >= 2 and key notin seen:
        seen.incl(key)
        result.add(key)

proc sanitizeKnowledgeName(s: string): string

proc safeJoin(tenant, rel: string): string =
  let tenantId = tenant.strip()
  if tenantId.len == 0:
    raise newException(ValueError, "tenant id is required")
  if tenantId in [".", ".."] or tenantId.contains("/") or tenantId.contains("\\") or '\0' in tenantId:
    raise newException(ValueError, "invalid tenant id")
  let tenantDir = sanitizeKnowledgeName(tenantId) & "-" & sha1Hex(tenantId)[0 .. 15]
  let workspaceBase = absolutePath(WorkspaceRoot)
  createDir(workspaceBase)
  if symlinkExists(workspaceBase):
    raise newException(ValueError, "workspace root cannot be a symbolic link")
  let base = absolutePath(workspaceBase / tenantDir)
  let rootClean = if workspaceBase.endsWith($DirSep): workspaceBase else: workspaceBase & $DirSep
  if not base.startsWith(rootClean):
    raise newException(ValueError, "tenant workspace escapes workspace root")
  if symlinkExists(base):
    raise newException(ValueError, "tenant workspace cannot be a symbolic link")
  createDir(base)
  let baseClean = if base.endsWith($DirSep): base else: base & $DirSep
  var cleaned = rel.replace('\\', '/')
  while cleaned.startsWith("/"):
    if cleaned.len == 1:
      cleaned = ""
    else:
      cleaned = cleaned[1 .. ^1]
  var parts: seq[string] = @[]
  for seg in cleaned.split('/'):
    if seg.len == 0 or seg == ".":
      continue
    if seg == "..":
      if parts.len == 0:
        raise newException(ValueError, "path escapes workspace sandbox")
      parts.setLen(parts.len - 1)
      continue
    if '\0' in seg:
      raise newException(ValueError, "invalid path segment")
    parts.add(seg)
  var current = base
  for seg in parts:
    current = current / seg
    if symlinkExists(current):
      raise newException(ValueError, "symbolic links are not allowed in workspace paths")
  let cleanedPath = parts.join($DirSep)
  let full = if cleanedPath.len == 0: base else: absolutePath(baseClean / cleanedPath)
  if not (full == base or full.startsWith(baseClean)):
    raise newException(ValueError, "path escapes workspace sandbox")
  result = full

proc atomicWrite(full, content: string) =
  createDir(parentDir(full))
  let tmp = full & ".tmp." & $getTime().toUnix() & "." & newId("t")
  try:
    writeFile(tmp, content)
    moveFile(tmp, full)
  finally:
    if fileExists(tmp):
      try:
        removeFile(tmp)
      except CatchableError:
        discard

proc readLinesOf(full: string): seq[string] =
  if not fileExists(full):
    return @[]
  let raw = readFile(full)
  if raw.len == 0:
    return @[]
  result = raw.splitLines()
  if result.len > 0 and result[^1].len == 0 and raw.endsWith("\n"):
    result.setLen(result.len - 1)

proc evalMathExpression(expr: string): (bool, float, string) =
  var pos = 0
  var failed = false
  var errMsg = ""

  proc fail(msg: string): float =
    if not failed:
      failed = true
      errMsg = msg
    0.0

  proc skipWs() =
    while pos < expr.len and expr[pos] in {' ', '\t', '\r', '\n'}:
      inc pos

  proc parseExpr(): float
  proc parseUnary(): float

  proc parseNumber(): float =
    skipWs()
    let start = pos
    var sawDigit = false
    while pos < expr.len and expr[pos].isDigit():
      sawDigit = true
      inc pos
    if pos < expr.len and expr[pos] == '.':
      inc pos
      while pos < expr.len and expr[pos].isDigit():
        sawDigit = true
        inc pos
    if not sawDigit:
      return fail("invalid numeric literal at pos " & $start)
    if pos < expr.len and expr[pos] in {'e', 'E'}:
      let expStart = pos
      inc pos
      if pos < expr.len and expr[pos] in {'+', '-'}:
        inc pos
      let digitsStart = pos
      while pos < expr.len and expr[pos].isDigit():
        inc pos
      if pos == digitsStart:
        pos = expStart
        return fail("invalid numeric exponent at pos " & $expStart)
    try:
      result = parseFloat(expr[start ..< pos])
    except CatchableError:
      result = fail("invalid numeric literal at pos " & $start)

  proc parsePrimary(): float =
    skipWs()
    if pos >= expr.len:
      return fail("unexpected end of expression")
    if expr[pos] == '(':
      inc pos
      let value = parseExpr()
      skipWs()
      if pos >= expr.len or expr[pos] != ')':
        return fail("missing closing parenthesis")
      inc pos
      return value
    if expr[pos].isAlphaAscii():
      var name = ""
      while pos < expr.len and (expr[pos].isAlphaNumeric() or expr[pos] == '_'):
        name.add(expr[pos].toLowerAscii())
        inc pos
      skipWs()
      if name == "pi" and (pos >= expr.len or expr[pos] != '('):
        return PI
      if name == "e" and (pos >= expr.len or expr[pos] != '('):
        return E
      if pos >= expr.len or expr[pos] != '(':
        return fail("unknown constant or symbol: " & name)
      inc pos
      let a = parseExpr()
      skipWs()
      if pos >= expr.len or expr[pos] != ')':
        return fail("missing closing parenthesis")
      inc pos
      if failed:
        return 0.0
      case name
      of "sin": return sin(a)
      of "cos": return cos(a)
      of "tan": return tan(a)
      of "sqrt":
        if a < 0.0: return fail("domain error: sqrt of negative")
        return sqrt(a)
      of "abs": return abs(a)
      of "ln":
        if a <= 0.0: return fail("domain error: ln non-positive")
        return ln(a)
      of "log10":
        if a <= 0.0: return fail("domain error: log10 non-positive")
        return log10(a)
      of "exp": return exp(a)
      of "floor": return floor(a)
      of "ceil": return ceil(a)
      of "round": return round(a)
      else: return fail("unknown function: " & name)
    if expr[pos].isDigit() or expr[pos] == '.':
      return parseNumber()
    fail("invalid token at pos " & $pos)

  proc parsePower(): float =
    var value = parsePrimary()
    if failed:
      return 0.0
    skipWs()
    if pos + 1 < expr.len and expr[pos] == '*' and expr[pos + 1] == '*':
      pos += 2
      let rhs = parseUnary()
      if failed: return 0.0
      value = pow(value, rhs)
    elif pos < expr.len and expr[pos] == '^':
      inc pos
      let rhs = parseUnary()
      if failed: return 0.0
      value = pow(value, rhs)
    value

  proc parseUnary(): float =
    skipWs()
    if pos < expr.len and expr[pos] == '+':
      inc pos
      return parseUnary()
    if pos < expr.len and expr[pos] == '-':
      inc pos
      return -parseUnary()
    parsePower()

  proc parseTerm(): float =
    var value = parseUnary()
    while not failed:
      skipWs()
      if pos < expr.len and expr[pos] == '*' and not (pos + 1 < expr.len and expr[pos + 1] == '*'):
        inc pos
        value *= parseUnary()
      elif pos < expr.len and expr[pos] == '/':
        inc pos
        let d = parseUnary()
        if abs(d) < 1e-15: return fail("division by zero")
        value /= d
      elif pos < expr.len and expr[pos] == '%':
        inc pos
        let d = parseUnary()
        if abs(d) < 1e-15: return fail("modulo by zero")
        value = value - d * floor(value / d)
      else:
        break
    value

  proc parseExpr(): float =
    var value = parseTerm()
    while not failed:
      skipWs()
      if pos < expr.len and expr[pos] == '+':
        inc pos
        value += parseTerm()
      elif pos < expr.len and expr[pos] == '-':
        inc pos
        value -= parseTerm()
      else:
        break
    value

  let value = parseExpr()
  skipWs()
  if not failed and pos != expr.len:
    discard fail("trailing unparsed token at pos " & $pos)
  if failed:
    result = (false, 0.0, errMsg)
  else:
    result = (true, value, "")

proc sanitizeKnowledgeName(s: string): string =
  result = newStringOfCap(s.len)
  for ch in s:
    if ch.isAlphaNumeric() or ch in {'-', '_', '.'}:
      result.add(ch)
    elif ch in {' ', '/', '\\', ':'}:
      result.add('-')
  while result.contains("--"):
    result = result.replace("--", "-")
  result = result.strip(chars = {'-', '.'})
  if result.len == 0:
    result = "entry"
  if result.len > 96:
    result.setLen(96)

proc ensureKnowledgeRepo(tenant: string): string =
  let tenantId = tenant.strip()
  if tenantId.len == 0:
    raise newException(ValueError, "tenant id is required")
  let safeTenant = sanitizeKnowledgeName(tenantId) & "-" & sha1Hex(tenantId)[0 .. 15]
  let root = absolutePath(knowledgeRoot)
  createDir(root)
  let dir = absolutePath(root / safeTenant)
  let rootClean = if root.endsWith($DirSep): root else: root & $DirSep
  if not dir.startsWith(rootClean):
    raise newException(ValueError, "knowledge repository escapes configured root")
  createDir(dir)
  if not dirExists(dir / ".git"):
    let initRes = execCmdEx("git -C " & quoteShell(dir) & " init")
    if initRes.exitCode != 0:
      raise newException(IOError, "git init failed: " & initRes.output)
    let emailRes = execCmdEx("git -C " & quoteShell(dir) & " config user.email agent@runtime.local")
    if emailRes.exitCode != 0:
      raise newException(IOError, "git config failed: " & emailRes.output)
    let nameRes = execCmdEx("git -C " & quoteShell(dir) & " config user.name AutonomousRuntime")
    if nameRes.exitCode != 0:
      raise newException(IOError, "git config failed: " & nameRes.output)
  result = dir

proc commitKnowledgeDoc(tenant, slug, category, body: string): string =
  let repo = ensureKnowledgeRepo(tenant)
  let safeSlug = sanitizeKnowledgeName(slug) & "-" & sha1Hex(slug)[0 .. 15]
  let safeCategory = sanitizeKnowledgeName(category) & "-" & sha1Hex(category)[0 .. 15]
  let rel = safeCategory & "_" & safeSlug & ".md"
  let full = repo / rel
  let contentHash = sha1Hex(body)
  let existing = store.query("SELECT doc_id,content_hash,git_commit_hash FROM knowledge_docs WHERE tenant_id=? AND slug=?", @[%tenant, %slug])
  if existing.len > 0 and existing[0].getStr("content_hash") == contentHash and fileExists(full):
    return existing[0].getStr("doc_id")
  atomicWrite(full, "# " & slug & "\nCategory: " & category & "\n\n" & body & "\n")
  let addRes = execCmdEx("git -C " & quoteShell(repo) & " add -- " & quoteShell(rel))
  if addRes.exitCode != 0:
    raise newException(IOError, "git add failed: " & addRes.output)
  let commitRes = execCmdEx("git -C " & quoteShell(repo) & " commit -m " & quoteShell("knowledge update: " & safeSlug))
  if commitRes.exitCode != 0:
    let statusRes = execCmdEx("git -C " & quoteShell(repo) & " status --porcelain -- " & quoteShell(rel))
    if statusRes.exitCode != 0 or statusRes.output.strip().len > 0:
      raise newException(IOError, "git commit failed: " & commitRes.output)
  let revRes = execCmdEx("git -C " & quoteShell(repo) & " rev-parse HEAD")
  if revRes.exitCode != 0:
    raise newException(IOError, "git rev-parse failed: " & revRes.output)
  let commitHash = revRes.output.strip()
  let docId = if existing.len > 0: existing[0].getStr("doc_id") else: newId("doc")
  let ts = nowF()
  discard store.exec("INSERT INTO knowledge_docs (doc_id,tenant_id,slug,category,path,content_hash,git_commit_hash,content,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,slug) DO UPDATE SET category=excluded.category,path=excluded.path,content_hash=excluded.content_hash,git_commit_hash=excluded.git_commit_hash,content=excluded.content,updated_at=excluded.updated_at",
    @[%docId, %tenant, %slug, %category, %rel, %contentHash, %commitHash, %body, %ts, %ts])
  result = docId

proc parseLegacyNumber(part: string, ok: var bool): uint64 =
  if part.len == 0:
    ok = false
    return 0
  var base = 10'u64
  var i = 0
  if part.len > 2 and part[0] == '0' and part[1] in {'x', 'X'}:
    base = 16
    i = 2
  elif part.len > 1 and part[0] == '0':
    base = 8
    i = 1
  if i >= part.len:
    ok = true
    return 0
  var value = 0'u64
  while i < part.len:
    let ch = part[i]
    var d = -1
    if ch in {'0'..'9'}: d = ch.ord - '0'.ord
    elif ch in {'a'..'f'}: d = 10 + ch.ord - 'a'.ord
    elif ch in {'A'..'F'}: d = 10 + ch.ord - 'A'.ord
    if d < 0 or uint64(d) >= base or value > (high(uint32).uint64 - uint64(d)) div base:
      ok = false
      return 0
    value = value * base + uint64(d)
    inc i
  ok = true
  value

proc parseLegacyIpv4(host: string): (bool, array[4, uint8]) =
  let parts = host.split('.')
  if parts.len < 1 or parts.len > 4:
    return (false, default(array[4, uint8]))
  var nums: seq[uint64] = @[]
  for part in parts:
    var ok = false
    let n = parseLegacyNumber(part, ok)
    if not ok:
      return (false, default(array[4, uint8]))
    nums.add(n)
  var value = 0'u64
  case nums.len
  of 1:
    if nums[0] > 0xffffffff'u64: return (false, default(array[4, uint8]))
    value = nums[0]
  of 2:
    if nums[0] > 0xff'u64 or nums[1] > 0xffffff'u64: return (false, default(array[4, uint8]))
    value = (nums[0] shl 24) or nums[1]
  of 3:
    if nums[0] > 0xff'u64 or nums[1] > 0xff'u64 or nums[2] > 0xffff'u64: return (false, default(array[4, uint8]))
    value = (nums[0] shl 24) or (nums[1] shl 16) or nums[2]
  of 4:
    for n in nums:
      if n > 0xff'u64: return (false, default(array[4, uint8]))
    value = (nums[0] shl 24) or (nums[1] shl 16) or (nums[2] shl 8) or nums[3]
  else:
    return (false, default(array[4, uint8]))
  var outv: array[4, uint8]
  outv[0] = uint8((value shr 24) and 0xff)
  outv[1] = uint8((value shr 16) and 0xff)
  outv[2] = uint8((value shr 8) and 0xff)
  outv[3] = uint8(value and 0xff)
  (true, outv)

proc blockedIpv4(a: array[4, uint8]): bool =
  let x = a[0].int
  let y = a[1].int
  if x == 0 or x == 10 or x == 127: return true
  if x == 100 and y >= 64 and y <= 127: return true
  if x == 169 and y == 254: return true
  if x == 172 and y >= 16 and y <= 31: return true
  if x == 192 and y == 168: return true
  if x == 198 and y in [18, 19]: return true
  if x >= 224: return true
  false

proc blockedIp(ip: IpAddress): bool =
  case ip.family
  of IpAddressFamily.IPv4:
    blockedIpv4(ip.address_v4)
  of IpAddressFamily.IPv6:
    let a = ip.address_v6
    var allZero = true
    for b in a:
      if b != 0'u8: allZero = false
    if allZero: return true
    var loopback = true
    for i in 0 ..< 15:
      if a[i] != 0'u8: loopback = false
    if loopback and a[15] == 1'u8: return true
    if (a[0] and 0xfe'u8) == 0xfc'u8: return true
    if a[0] == 0xfe'u8 and (a[1] and 0xc0'u8) == 0x80'u8: return true
    if a[0] == 0xff'u8: return true
    var mapped = true
    for i in 0 ..< 10:
      if a[i] != 0'u8: mapped = false
    if mapped and a[10] == 0xff'u8 and a[11] == 0xff'u8:
      return blockedIpv4([a[12], a[13], a[14], a[15]])
    false

proc validateOutboundHost(hostInput: string) =
  var host = hostInput.toLowerAscii().strip()
  while host.len > 0 and host.endsWith("."):
    host.setLen(host.len - 1)
  if host.len == 0:
    raise newException(ValueError, "URL hostname required")
  if '%' in host or '\0' in host:
    raise newException(ValueError, "invalid URL hostname")
  if host == "localhost" or host.endsWith(".localhost") or host == "metadata.google.internal" or host.endsWith(".metadata.google.internal"):
    raise newException(ValueError, "target blocked")
  let legacy = parseLegacyIpv4(host)
  if legacy[0]:
    if blockedIpv4(legacy[1]):
      raise newException(ValueError, "target blocked")
    return
  if isIpAddress(host):
    if blockedIp(parseIpAddress(host)):
      raise newException(ValueError, "target blocked")
    return
  let resolved = getHostByName(host)
  if resolved.addrList.len == 0:
    raise newException(ValueError, "hostname did not resolve")
  for address in resolved.addrList:
    if not isIpAddress(address):
      raise newException(ValueError, "hostname resolved to an invalid address")
    if blockedIp(parseIpAddress(address)):
      raise newException(ValueError, "hostname resolves to a blocked address")

proc validateOutboundUrl(url: string): Uri =
  if '\r' in url or '\n' in url:
    raise newException(ValueError, "invalid URL")
  let parsed = parseUri(url)
  let scheme = parsed.scheme.toLowerAscii()
  if scheme notin ["http", "https"]:
    raise newException(ValueError, "only http/https allowed")
  if parsed.username.len > 0 or parsed.password.len > 0:
    raise newException(ValueError, "userinfo in URL is not allowed")
  validateOutboundHost(parsed.hostname)
  parsed

proc readBoundedBody(resp: AsyncResponse, maxBytes: int): Future[(string, bool)] {.async.} =
  if maxBytes <= 0:
    raise newException(ValueError, "response body limit must be positive")
  var body = newStringOfCap(min(maxBytes, 8192))
  while true:
    let item = await resp.bodyStream.read()
    if not item[0]:
      break
    let chunk = item[1]
    if chunk.len > maxBytes - body.len:
      return (body, true)
    body.add(chunk)
  return (body, false)

proc responseHeadersJson(headers: HttpHeaders): JsonNode =
  result = newJObject()
  if headers.isNil:
    return
  for key, value in headers:
    result[key] = %value

proc mapHttpMethod(methodName: string): HttpMethod =
  case methodName.toUpperAscii()
  of "GET": HttpGet
  of "POST": HttpPost
  of "PUT": HttpPut
  of "DELETE": HttpDelete
  of "HEAD": HttpHead
  of "PATCH": HttpPatch
  of "OPTIONS": HttpOptions
  else: raise newException(ValueError, "unsupported HTTP method: " & methodName)

type
  ProviderKind = enum
    pkRequesty,
    pkCerebras,
    pkGemini
  ModelRole = enum
    mrOrchestrator,
    mrGpt6Astra,
    mrGlm52,
    mrGemini38,
    mrMiniMaxM3,
    mrGrok43
  OrchestratorState = enum
    osPerceive,
    osDeliberate,
    osAct,
    osValidate,
    osReflect,
    osConsolidate,
    osTerminal
  ModelSpec = object
    role: ModelRole
    provider: ProviderKind
    model: string
    multimodal: bool
    structured: bool
  ReferenceSkill = object
    name: string
    description: string
    path: string
    content: string
  TopLogprobItem = object
    token: string
    logprob: float
  LogprobItem = object
    token: string
    logprob: float
    textOffset: int
    topLogprobs: seq[TopLogprobItem]
  LlmResponse = object
    content: string
    reasoningContent: string
    raw: JsonNode
    usage: JsonNode
    model: string
    provider: ProviderKind
    finishReason: string
    promptTokens: int
    completionTokens: int
    totalTokens: int
    logprobs: seq[LogprobItem]
  DirectChatResult = object
    content: string
    reasoningContent: string
    model: string
    usage: JsonNode
    taskId: string
  RouteDecision = object
    raw: JsonNode
    intent: string
    primaryModel: ModelRole
    secondaryModels: seq[ModelRole]
    requiresVm: bool
    requiresBrowser: bool
    requiresDesktop: bool
    requiresVisualAnalysis: bool
    requiresDocumentAnalysis: bool
    plan: JsonNode
    delegations: JsonNode
    completionCriteria: JsonNode
  ToolResult = object
    ok: bool
    payload: JsonNode
    receipt: string
    message: string
  OrchestratorGraph = object
    edges: Table[OrchestratorState, HashSet[OrchestratorState]]
  StateTransitionEngine = ref object
    graph: OrchestratorGraph
    maxRetries: int
  ValidationGate = ref object
    epsilon: float
  MetaAgent = ref object
    minOccurrences: int
    lookback: int
    maxCandidates: int
    gate: ValidationGate
  SseClient = ref object
    req: Request
    tenantId: string
    alive: bool
    queue: Deque[string]
    lock: Lock
  TaskHandle = ref object
    taskId: string
    tenantId: string
    title: string
    spec: JsonNode
    sigma: JsonNode
    obs: JsonNode
    stepIndex: int
    maxSteps: int
    status: string
    terminalReason: string
    verified: bool
    paused: bool
    stopRequested: bool
    loopActive: Atomic[bool]
    transitionBusy: bool
    lock: Lock
    orchestratorState: OrchestratorState
    subscribers: seq[tuple[id: string, cb: proc(ev: JsonNode) {.closure.}]]
    cognition: JsonNode
    cognitionAt: float
    allowedTools: HashSet[string]
    broadcastAttached: bool
    lastPlannedDigest: string
  SubAgentHandle = ref object
    agentId: string
    taskId: string
    parentAgentId: string
    name: string
    goal: string
    instructions: string
    modelRole: ModelRole
    context: JsonNode
    messages: JsonNode
    state: JsonNode
    status: string
    resultText: string
    errorText: string
    stopRequested: bool
    loopActive: Atomic[bool]
    lock: Lock
    rootTask: TaskHandle
  ToolHandler = proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.closure.}
  GcsafeToolHandler = proc(tenant: string, args: JsonNode): Future[ToolResult] {.closure, gcsafe.}
  GcsafeRequestHandler = proc(req: Request): Future[void] {.closure, gcsafe.}
  ToolSpec = object
    name: string
    description: string
    schema: JsonNode
    handler: ToolHandler

var
  toolRegistry {.threadvar.}: OrderedTable[string, ToolSpec]
  promptRegistry {.threadvar.}: OrderedTable[string, string]
  referenceSkills {.threadvar.}: OrderedTable[string, ReferenceSkill]
  activeTasks = initTable[string, TaskHandle]()
  tasksLock: Lock
  sseLock: Lock
  sseSubscribers = initTable[string, seq[tuple[id: string, cb: proc(ev: JsonNode) {.closure.}]]]()
  activeChatJobs = initHashSet[string]()
  chatJobsLock: Lock
  activeSubAgents = initTable[string, SubAgentHandle]()
  subAgentsLock: Lock

proc orchestratorStateName(state: OrchestratorState): string =
  case state
  of osPerceive: "perceive"
  of osDeliberate: "deliberate"
  of osAct: "act"
  of osValidate: "validate"
  of osReflect: "reflect"
  of osConsolidate: "consolidate"
  of osTerminal: "terminal"

proc parseOrchestratorState(value: string): OrchestratorState =
  case value.strip().toLowerAscii()
  of "perceive": osPerceive
  of "deliberate": osDeliberate
  of "act": osAct
  of "validate": osValidate
  of "reflect": osReflect
  of "consolidate": osConsolidate
  of "terminal": osTerminal
  else: osPerceive

proc buildOrchestratorGraph(): OrchestratorGraph =
  result.edges = initTable[OrchestratorState, HashSet[OrchestratorState]]()
  for state in OrchestratorState:
    result.edges[state] = initHashSet[OrchestratorState]()
  result.edges[osPerceive].incl(osDeliberate)
  result.edges[osDeliberate].incl(osAct)
  result.edges[osDeliberate].incl(osValidate)
  result.edges[osAct].incl(osValidate)
  result.edges[osAct].incl(osReflect)
  result.edges[osValidate].incl(osPerceive)
  result.edges[osValidate].incl(osAct)
  result.edges[osValidate].incl(osReflect)
  result.edges[osValidate].incl(osConsolidate)
  result.edges[osValidate].incl(osTerminal)
  result.edges[osReflect].incl(osConsolidate)
  result.edges[osConsolidate].incl(osPerceive)
  result.edges[osConsolidate].incl(osTerminal)
  result.edges[osTerminal].incl(osTerminal)
  result.edges[osTerminal].incl(osPerceive)

proc canOrchestratorTransition(graph: OrchestratorGraph, fromState, toState: OrchestratorState): bool =
  if fromState == toState:
    return true
  if not graph.edges.hasKey(fromState):
    return false
  toState in graph.edges[fromState]

proc newStateTransitionEngine(): StateTransitionEngine =
  StateTransitionEngine(graph: buildOrchestratorGraph(), maxRetries: 3)

var transitionEngine: StateTransitionEngine

proc modelRoleName(role: ModelRole): string =
  case role
  of mrOrchestrator: "orchestrator"
  of mrGpt6Astra: "gpt6_astra"
  of mrGlm52: "glm52"
  of mrGemini38: "gemini38"
  of mrMiniMaxM3: "minimax_m3"
  of mrGrok43: "grok43"

proc parseModelRole(s: string): ModelRole =
  case s.strip().toLowerAscii()
  of "orchestrator": mrOrchestrator
  of "gpt6_astra", "gpt-6-astra", "gpt6", "gpt_6_astra": mrGpt6Astra
  of "glm52", "glm_5_2", "glm-5.2": mrGlm52
  of "gemini38", "gemini_3_8", "gemini-3.8": mrGemini38
  of "minimax_m3", "minimax-m3", "minimax": mrMiniMaxM3
  of "grok43", "grok_4_3", "grok-4.3": mrGrok43
  else: raise newException(ValueError, "unknown model role: " & s)

proc providerName(p: ProviderKind): string =
  case p
  of pkRequesty: "requesty"
  of pkCerebras: "cerebras"
  of pkGemini: "gemini"

proc modelSpec(role: ModelRole): ModelSpec =
  case role
  of mrOrchestrator:
    ModelSpec(role: role, provider: pkCerebras, model: CerebrasGemma4Model, multimodal: false, structured: true)
  of mrGpt6Astra:
    ModelSpec(role: role, provider: pkRequesty, model: Gpt6AstraModel, multimodal: true, structured: true)
  of mrGlm52:
    ModelSpec(role: role, provider: pkRequesty, model: Glm52Model, multimodal: false, structured: false)
  of mrGemini38:
    ModelSpec(role: role, provider: pkGemini, model: Gemini38Model, multimodal: true, structured: true)
  of mrMiniMaxM3:
    ModelSpec(role: role, provider: pkRequesty, model: MiniMaxM3Model, multimodal: false, structured: false)
  of mrGrok43:
    ModelSpec(role: role, provider: pkRequesty, model: Grok43Model, multimodal: true, structured: false)

proc yamlScalar(value: string): string =
  let v = value.strip()
  if v.len >= 2 and ((v[0] == '"' and v[^1] == '"') or (v[0] == '\'' and v[^1] == '\'')):
    return v[1 .. ^2]
  v

proc loadPromptRegistry(path: string): OrderedTable[string, string] =
  if not fileExists(path):
    raise newException(IOError, "prompt configuration file does not exist: " & path)
  var prompts = initOrderedTable[string, string]()
  let raw = readFile(path)
  let header = ": |"
  var cursor = 0
  while true:
    let marker = raw.find(header, cursor)
    if marker < 0:
      break
    var keyStart = marker - 1
    while keyStart >= 0 and (raw[keyStart].isAlphaNumeric or raw[keyStart] == '_'):
      dec keyStart
    inc keyStart
    let key = raw[keyStart ..< marker].strip()
    if key.len == 0:
      raise newException(ValueError, "invalid prompt YAML header")
    let bodyStart = marker + header.len
    let nextMarker = raw.find(header, bodyStart)
    let bodyEnd = if nextMarker < 0: raw.len else: nextMarker
    var promptLines: seq[string] = @[]
    for line in raw[bodyStart ..< bodyEnd].splitLines():
      if line.len >= 2 and line[0] == ' ' and line[1] == ' ':
        promptLines.add(line[2 .. ^1])
      else:
        promptLines.add(line)
    let value = promptLines.join("\n").strip(chars = {' ', '\t', '\n', '\r'})
    if value.len == 0:
      raise newException(ValueError, "empty prompt: " & key)
    if prompts.hasKey(key):
      raise newException(ValueError, "duplicate prompt key: " & key)
    prompts[key] = value
    if nextMarker < 0:
      break
    cursor = nextMarker
  if prompts.len == 0:
    raise newException(ValueError, "prompt configuration is empty: " & path)
  result = prompts

proc promptText(key: string): string =
  if not promptRegistry.hasKey(key):
    raise newException(ValueError, "missing configured prompt: " & key)
  promptRegistry[key]

proc parseSkillFrontMatter(content, path: string): ReferenceSkill =
  let lines = content.splitLines()
  if lines.len < 3 or lines[0].strip() != "---":
    raise newException(ValueError, "SKILL.md is missing YAML front matter: " & path)
  var fields = initTable[string, string]()
  var i = 1
  var closing = -1
  while i < lines.len:
    if lines[i].strip() == "---":
      closing = i
      break
    let raw = lines[i]
    if raw.strip().len == 0:
      inc i
      continue
    if raw[0] in {' ', '\t'}:
      raise newException(ValueError, "unexpected indentation in SKILL.md front matter: " & path)
    let colon = raw.find(':')
    if colon <= 0:
      raise newException(ValueError, "invalid SKILL.md front matter line: " & raw)
    let field = raw[0 ..< colon].strip()
    let tail = raw[colon + 1 .. ^1].strip()
    if tail in [">", ">-", ">+", "|", "|-", "|+"]:
      let folded = tail[0] == '>'
      inc i
      var chunks: seq[string] = @[]
      while i < lines.len and lines[i].strip() != "---" and (lines[i].len == 0 or lines[i][0] in {' ', '\t'}):
        let line = lines[i]
        var value = line
        if value.len >= 2 and value[0] == ' ' and value[1] == ' ':
          value = value[2 .. ^1]
        else:
          value = value.strip()
        chunks.add(value)
        inc i
      fields[field] = if folded: chunks.join(" ").splitWhitespace().join(" ") else: chunks.join("\n").strip()
      continue
    fields[field] = yamlScalar(tail)
    inc i
  if closing < 0:
    raise newException(ValueError, "SKILL.md front matter is not closed: " & path)
  let name = fields.getOrDefault("name", "").strip()
  let description = fields.getOrDefault("description", "").strip()
  if name.len == 0 or description.len == 0:
    raise newException(ValueError, "SKILL.md requires name and description: " & path)
  result = ReferenceSkill(name: name, description: description, path: path, content: content)

proc loadReferenceSkills(root: string): OrderedTable[string, ReferenceSkill] =
  if not dirExists(root):
    raise newException(IOError, "reference skill directory does not exist: " & root)
  result = initOrderedTable[string, ReferenceSkill]()
  var paths: seq[string] = @[]
  for path in walkDirRec(root):
    let parts = splitFile(path)
    if parts.name == "SKILL" and parts.ext.toLowerAscii() == ".md":
      paths.add(path)
  paths.sort()
  for path in paths:
    let skill = parseSkillFrontMatter(readFile(path), path)
    if result.hasKey(skill.name):
      raise newException(ValueError, "duplicate reference skill name: " & skill.name)
    result[skill.name] = skill
  if result.len == 0:
    raise newException(ValueError, "no SKILL.md files found under: " & root)

proc referenceSkillCatalog(): string =
  var parts: seq[string] = @[]
  for name, skill in referenceSkills.pairs:
    parts.add("REFERENCE SKILL: " & name & "\nSOURCE: " & skill.path & "\n" & skill.content)
  parts.join("\n\n")

proc configuredModelRoleNames(): seq[string] =
  @[modelRoleName(mrGpt6Astra), modelRoleName(mrGlm52), modelRoleName(mrGemini38), modelRoleName(mrMiniMaxM3), modelRoleName(mrGrok43)]

proc configuredSubAgentModelRoleNames(): seq[string] =
  @[modelRoleName(mrOrchestrator), modelRoleName(mrGpt6Astra), modelRoleName(mrGlm52), modelRoleName(mrGemini38), modelRoleName(mrMiniMaxM3), modelRoleName(mrGrok43)]

proc normalizePlanStep(step: JsonNode, index: int): JsonNode =
  if step.isNil or step.kind != JObject:
    raise newException(ValueError, "plan step must be an object")
  result = copy(step)
  let rawModel = result{"model"}.getStr("").strip()
  if rawModel.len == 0:
    raise newException(ValueError, "plan step model is required because the orchestrator must choose it explicitly")
  let role = parseModelRole(rawModel)
  if role == mrOrchestrator:
    raise newException(ValueError, "orchestrator cannot be used as a specialist plan model")
  result["model"] = %modelRoleName(role)
  if result{"id"}.getStr("").strip().len == 0:
    result["id"] = %("step-" & $(index + 1))
  if result{"goal"}.getStr("").strip().len == 0:
    raise newException(ValueError, "plan step goal is required")
  if not result.hasKey("execution_mode") or result["execution_mode"].kind != JString or result["execution_mode"].getStr("").strip().len == 0:
    raise newException(ValueError, "plan step execution_mode is required because the orchestrator must choose it explicitly")
  if result["execution_mode"].getStr("") notin ["reason", "vm", "browser", "desktop", "visual", "document", "subagent"]:
    raise newException(ValueError, "invalid execution_mode in plan step: " & result["execution_mode"].getStr(""))
  if result.hasKey("depends_on"):
    if result["depends_on"].kind != JArray:
      raise newException(ValueError, "plan step depends_on must be an array")
    for dependency in result["depends_on"].elems:
      if dependency.kind != JString or dependency.getStr("").strip().len == 0:
        raise newException(ValueError, "plan step dependencies must be non-empty strings")
  else:
    result["depends_on"] = newJArray()
  if result.hasKey("status"):
    if result["status"].kind != JString or result["status"].getStr("").strip().toLowerAscii() != "pending":
      raise newException(ValueError, "new plan step status must be pending")
  result["status"] = %"pending"

proc parseJsonObjectLoose(s: string): JsonNode =
  let t = s.strip()
  if t.len == 0:
    return nil
  try:
    let j = parseJson(t)
    if j.kind == JObject:
      return j
  except CatchableError:
    discard
  var start = t.find('{')
  while start >= 0:
    var depth = 0
    var inString = false
    var escaped = false
    for i in start ..< t.len:
      let ch = t[i]
      if inString:
        if escaped:
          escaped = false
        elif ch == '\\':
          escaped = true
        elif ch == '"':
          inString = false
      else:
        if ch == '"':
          inString = true
        elif ch == '{':
          inc depth
        elif ch == '}':
          dec depth
          if depth == 0:
            try:
              let j = parseJson(t[start .. i])
              if j.kind == JObject:
                return j
            except CatchableError:
              break
    let next = t.find('{', start + 1)
    if next < 0:
      break
    start = next
  nil

proc copyOrEmpty(n: JsonNode): JsonNode =
  if n.isNil: newJObject() else: copy(n)

proc jsonArrayStrings(n: JsonNode): seq[string] =
  result = @[]
  if n.isNil or n.kind != JArray:
    return
  for it in n.elems:
    if it.kind == JString:
      result.add(it.getStr())

proc httpRequestAsync(url: string, meth: HttpMethod, body = "", headers: HttpHeaders = nil): Future[(int, string, HttpHeaders)] {.async.} =
  let maxRedirects = positiveEnvInt("OUTBOUND_HTTP_MAX_REDIRECTS", 5)
  let timeoutMs = positiveEnvInt("OUTBOUND_HTTP_TIMEOUT_MS", 60000)
  let maxBytes = positiveEnvInt("OUTBOUND_HTTP_MAX_BYTES", 32 * 1024 * 1024)
  var currentUrl = url
  var currentMethod = meth
  var currentBody = body
  for redirectIndex in 0 .. maxRedirects:
    discard validateOutboundUrl(currentUrl)
    var client = newAsyncHttpClient(maxRedirects = 0)
    client.timeout = timeoutMs
    if headers != nil:
      client.headers = headers
    try:
      let resp = await client.request(currentUrl, httpMethod = currentMethod, body = currentBody)
      let status = resp.code.int
      if status in [301, 302, 303, 307, 308] and resp.headers.hasKey("Location"):
        if redirectIndex >= maxRedirects:
          raise newException(IOError, "too many HTTP redirects")
        let location = resp.headers["Location"]
        let nextUri = combine(parseUri(currentUrl), parseUri(location))
        let nextUrl = $nextUri
        discard validateOutboundUrl(nextUrl)
        if status == 303 or ((status == 301 or status == 302) and currentMethod == HttpPost):
          currentMethod = HttpGet
          currentBody = ""
        currentUrl = nextUrl
        continue
      let bounded = await readBoundedBody(resp, maxBytes)
      if bounded[1]:
        raise newException(IOError, "upstream response exceeded configured size limit")
      return (status, bounded[0], resp.headers)
    finally:
      client.close()
  raise newException(IOError, "HTTP redirect processing failed")

proc requireEnv(name: string): string =
  result = getEnv(name, "").strip()
  if result.len == 0:
    raise newException(IOError, name & " is not configured")

proc openAiContent(msg: JsonNode): string =
  if msg.isNil or msg.kind != JObject:
    return ""
  let c = msg{"content"}
  if c.isNil:
    return ""
  case c.kind
  of JString:
    return c.getStr()
  of JArray:
    var parts: seq[string] = @[]
    for it in c.elems:
      if it.kind == JObject:
        let t = it{"text"}.getStr(it{"content"}.getStr(""))
        if t.len > 0:
          parts.add(t)
    return parts.join("")
  else:
    return ""

proc streamText(node: JsonNode): string =
  if node.isNil:
    return ""
  case node.kind
  of JString:
    return node.getStr("")
  of JArray:
    var parts: seq[string] = @[]
    for item in node.elems:
      if item.kind == JString:
        parts.add(item.getStr(""))
      elif item.kind == JObject:
        let text = item{"text"}.getStr(item{"content"}.getStr(""))
        if text.len > 0:
          parts.add(text)
    return parts.join("")
  of JObject:
    return node{"text"}.getStr(node{"content"}.getStr(""))
  else:
    return ""

proc requestyBody(role: ModelRole, messages: JsonNode, structured = false, stream = false): JsonNode =
  let spec = modelSpec(role)
  result = %*{"model": spec.model, "messages": messages}
  case role
  of mrGpt6Astra:
    result["reasoning_effort"] = %"max"
    result["reasoning"] = %*{"effort": "max", "mode": "pro"}
    result["requesty"] = %*{"auto_cache": true}
  of mrGlm52:
    result["temperature"] = %0
    result["max_tokens"] = %131072
    result["reasoning_effort"] = %"max"
  of mrMiniMaxM3:
    result["temperature"] = %0
    result["max_tokens"] = %131072
    result["thinking"] = %*{"type": "adaptive"}
  of mrGrok43:
    result["reasoning_effort"] = %"high"
    result["requesty"] = %*{"auto_cache": true}
  else:
    discard
  if structured:
    result["response_format"] = %*{"type": "json_object"}
  if stream:
    result["stream"] = %true
    result["stream_options"] = %*{"include_usage": true}

proc parseOpenAiResponse(raw: string, provider: ProviderKind): LlmResponse =
  let j = parseJson(raw)
  result = LlmResponse(provider: provider, raw: j, logprobs: @[])
  result.model = j{"model"}.getStr("")
  result.usage = if j.hasKey("usage") and j["usage"].kind == JObject: copy(j["usage"]) else: newJObject()
  if result.usage.kind == JObject and result.usage.len > 0:
    result.promptTokens = result.usage{"prompt_tokens"}.getInt(result.usage{"input_tokens"}.getInt(0))
    result.completionTokens = result.usage{"completion_tokens"}.getInt(result.usage{"output_tokens"}.getInt(0))
    result.totalTokens = result.usage{"total_tokens"}.getInt(0)
    if result.totalTokens == 0 and (result.promptTokens > 0 or result.completionTokens > 0):
      result.totalTokens = result.promptTokens + result.completionTokens
  if j.hasKey("choices") and j["choices"].kind == JArray and j["choices"].elems.len > 0:
    let ch = j["choices"][0]
    result.finishReason = ch{"finish_reason"}.getStr("")
    let msg = ch{"message"}
    result.content = openAiContent(msg)
    result.reasoningContent = msg{"reasoning_content"}.getStr(msg{"reasoning"}.getStr(""))
    if ch.hasKey("logprobs") and ch["logprobs"].kind == JObject and ch["logprobs"].hasKey("content") and ch["logprobs"]["content"].kind == JArray:
      for item in ch["logprobs"]["content"].elems:
        if item.kind != JObject:
          continue
        var tops: seq[TopLogprobItem] = @[]
        if item.hasKey("top_logprobs") and item["top_logprobs"].kind == JArray:
          for candidate in item["top_logprobs"].elems:
            if candidate.kind == JObject:
              tops.add(TopLogprobItem(token: candidate{"token"}.getStr(""), logprob: candidate{"logprob"}.getFloat(-99.0)))
        result.logprobs.add(LogprobItem(token: item{"token"}.getStr(""), logprob: item{"logprob"}.getFloat(-99.0), textOffset: item{"text_offset"}.getInt(-1), topLogprobs: tops))

proc requestyCall(role: ModelRole, messages: JsonNode, structured = false): Future[LlmResponse] {.async.} =
  let key = requireEnv("REQUESTY_API_KEY")
  let body = requestyBody(role, messages, structured, false)
  let headers = newHttpHeaders({
    "Authorization": "Bearer " & key,
    "Content-Type": "application/json",
    "Accept": "application/json"
  })
  let (status, raw, _) = await httpRequestAsync(RequestyBaseUrl & "/chat/completions", HttpPost, $body, headers)
  if status < 200 or status >= 300:
    raise newException(IOError, "Requesty status " & $status & ": " & raw)
  return parseOpenAiResponse(raw, pkRequesty)

proc resolveCerebrasGemma4(): Future[string] {.async.} =
  if CerebrasGemma4Model.len > 0:
    return CerebrasGemma4Model
  let explicit = getEnv("CEREBRAS_GEMMA4_MODEL", "").strip()
  if explicit.len > 0:
    CerebrasGemma4Model = explicit
    return explicit
  let key = requireEnv("CEREBRAS_API_KEY")
  let headers = newHttpHeaders({
    "Authorization": "Bearer " & key,
    "Accept": "application/json"
  })
  let (status, raw, _) = await httpRequestAsync(CerebrasBaseUrl & "/models", HttpGet, "", headers)
  if status < 200 or status >= 300:
    raise newException(IOError, "Cerebras models status " & $status & ": " & raw)
  let j = parseJson(raw)
  if not j.hasKey("data") or j["data"].kind != JArray:
    raise newException(IOError, "Cerebras model catalog has no data array")
  for it in j["data"].elems:
    if it.kind != JObject:
      continue
    let id = it{"id"}.getStr("")
    let hay = (id & " " & it{"name"}.getStr("") & " " & it{"description"}.getStr("")).toLowerAscii()
    if "gemma" in hay and ("4" in hay or "four" in hay):
      CerebrasGemma4Model = id
      return id
  raise newException(IOError, "Gemma 4 model is not available in the Cerebras model catalog")

proc cerebrasCall(messages: JsonNode, structured = true): Future[LlmResponse] {.async.} =
  let key = requireEnv("CEREBRAS_API_KEY")
  let model = await resolveCerebrasGemma4()
  var body = %*{
    "model": model,
    "messages": messages,
    "reasoning_effort": "high"
  }
  if structured:
    body["response_format"] = %*{"type": "json_object"}
  let headers = newHttpHeaders({
    "Authorization": "Bearer " & key,
    "Content-Type": "application/json",
    "Accept": "application/json"
  })
  let (status, raw, _) = await httpRequestAsync(CerebrasBaseUrl & "/chat/completions", HttpPost, $body, headers)
  if status < 200 or status >= 300:
    raise newException(IOError, "Cerebras status " & $status & ": " & raw)
  return parseOpenAiResponse(raw, pkCerebras)

proc extractGeminiOutput(j: JsonNode): string =
  var parts: seq[string] = @[]
  if j.hasKey("output_text") and j["output_text"].kind == JString:
    parts.add(j["output_text"].getStr())
  if j.hasKey("steps") and j["steps"].kind == JArray:
    for step in j["steps"].elems:
      if step.kind != JObject or step{"type"}.getStr("") != "model_output":
        continue
      let content = step{"content"}
      if content.kind == JArray:
        for contentBlock in content.elems:
          if contentBlock.kind == JObject and contentBlock{"type"}.getStr("") == "text":
            let text = contentBlock{"text"}.getStr("")
            if text.len > 0:
              parts.add(text)
  result = parts.join("")

proc geminiCall(input: JsonNode, systemInstruction = ""): Future[LlmResponse] {.async.} =
  let key = requireEnv("GEMINI_API_KEY")
  var body = %*{
    "model": Gemini38Model,
    "input": input,
    "tools": [
      {"type": "code_execution"},
      {"type": "google_search"},
      {"type": "url_context"}
    ],
    "generation_config": {
      "max_output_tokens": 65536,
      "thinking_level": "high"
    }
  }
  if systemInstruction.len > 0:
    body["system_instruction"] = %systemInstruction
  let headers = newHttpHeaders({
    "x-goog-api-key": key,
    "Content-Type": "application/json",
    "Accept": "application/json"
  })
  let (status, raw, _) = await httpRequestAsync(GeminiBaseUrl & "/interactions", HttpPost, $body, headers)
  if status < 200 or status >= 300:
    raise newException(IOError, "Gemini status " & $status & ": " & raw)
  let j = parseJson(raw)
  var usage = newJObject()
  if j.hasKey("usage") and j["usage"].kind == JObject:
    usage = copy(j["usage"])
  elif j.hasKey("usageMetadata") and j["usageMetadata"].kind == JObject:
    usage = copy(j["usageMetadata"])
  let promptTokens = usage{"prompt_tokens"}.getInt(usage{"input_tokens"}.getInt(usage{"promptTokenCount"}.getInt(usage{"inputTokenCount"}.getInt(0))))
  let completionTokens = usage{"completion_tokens"}.getInt(usage{"output_tokens"}.getInt(usage{"candidatesTokenCount"}.getInt(usage{"outputTokenCount"}.getInt(0))))
  var totalTokens = usage{"total_tokens"}.getInt(usage{"totalTokenCount"}.getInt(0))
  if totalTokens == 0 and (promptTokens > 0 or completionTokens > 0):
    totalTokens = promptTokens + completionTokens
  if usage.kind == JObject and usage.len > 0:
    usage["prompt_tokens"] = %promptTokens
    usage["completion_tokens"] = %completionTokens
    usage["total_tokens"] = %totalTokens
  return LlmResponse(content: extractGeminiOutput(j), raw: j, usage: usage, model: Gemini38Model, provider: pkGemini, finishReason: j{"status"}.getStr(""), promptTokens: promptTokens, completionTokens: completionTokens, totalTokens: totalTokens, logprobs: @[])

proc addGeminiMediaPart(input: JsonNode, kind, value, mimeType: string) =
  if input.isNil or input.kind != JArray or value.len == 0:
    return
  if value.startsWith("data:"):
    let comma = value.find(',')
    if comma > 5:
      let header = value[5 ..< comma]
      let payload = if comma + 1 < value.len: value[comma + 1 .. ^1] else: ""
      let semi = header.find(';')
      let actualMime = if semi >= 0: header[0 ..< semi] else: header
      input.add(%*{"type": kind, "mime_type": (if actualMime.len > 0: actualMime else: mimeType), "data": payload})
      return
  if value.startsWith("http://") or value.startsWith("https://") or value.startsWith("gs://"):
    input.add(%*{"type": kind, "uri": value})
  else:
    input.add(%*{"type": kind, "mime_type": mimeType, "data": value})

proc geminiInputFromMessages(messages: JsonNode): JsonNode =
  result = newJArray()
  if messages.isNil or messages.kind != JArray:
    return
  for m in messages.elems:
    if m.kind != JObject:
      continue
    let roleName = m{"role"}.getStr("user")
    if roleName == "system":
      continue
    let content = m{"content"}
    var contentParts = newJArray()
    if content.kind == JString:
      contentParts.add(%*{"type": "text", "text": content.getStr()})
    elif content.kind == JArray:
      for part in content.elems:
        if part.kind != JObject:
          continue
        let typ = part{"type"}.getStr("")
        case typ
        of "text", "input_text":
          let text = part{"text"}.getStr(part{"content"}.getStr(""))
          if text.len > 0:
            contentParts.add(%*{"type": "text", "text": text})
        of "image_url", "input_image", "image":
          var value = part{"url"}.getStr(part{"uri"}.getStr(part{"data"}.getStr("")))
          if part.hasKey("image_url"):
            if part["image_url"].kind == JObject:
              value = part["image_url"]{"url"}.getStr(value)
            elif part["image_url"].kind == JString:
              value = part["image_url"].getStr()
          addGeminiMediaPart(contentParts, "image", value, part{"mime_type"}.getStr("image/jpeg"))
        of "video_url", "input_video", "video":
          var value = part{"url"}.getStr(part{"uri"}.getStr(part{"data"}.getStr("")))
          if part.hasKey("video_url"):
            if part["video_url"].kind == JObject:
              value = part["video_url"]{"url"}.getStr(value)
            elif part["video_url"].kind == JString:
              value = part["video_url"].getStr()
          addGeminiMediaPart(contentParts, "video", value, part{"mime_type"}.getStr("video/mp4"))
        of "document", "file":
          var value = part{"url"}.getStr(part{"uri"}.getStr(part{"data"}.getStr("")))
          addGeminiMediaPart(contentParts, "document", value, part{"mime_type"}.getStr("application/pdf"))
        else:
          discard
    if contentParts.len > 0:
      let stepType = if roleName == "assistant": "model_output" else: "user_input"
      result.add(%*{"type": stepType, "content": contentParts})

proc geminiSystemInstructionFromMessages(messages: JsonNode): string =
  let specialist = promptText("gemini38")
  var systemMessages: seq[string] = @[]
  var containsSubAgentProtocol = false
  if not messages.isNil and messages.kind == JArray:
    for message in messages.elems:
      if message.kind == JObject and message{"role"}.getStr("") == "system":
        let text = openAiContent(message).strip()
        if text.len > 0:
          systemMessages.add(text)
          if promptText("subagent_core") in text:
            containsSubAgentProtocol = true
  var parts: seq[string] = @[]
  if not containsSubAgentProtocol:
    parts.add(specialist)
  for text in systemMessages:
    if text != specialist or parts.len == 0:
      parts.add(text)
  parts.join("\n\n")

proc findMediaValue(n: JsonNode, preferredKeys: openArray[string]): string =
  if n.isNil:
    return ""
  case n.kind
  of JObject:
    for key in preferredKeys:
      if n.hasKey(key) and n[key].kind == JString and n[key].getStr("").len > 64:
        return n[key].getStr()
    for _, value in n.fields:
      let found = findMediaValue(value, preferredKeys)
      if found.len > 0:
        return found
  of JArray:
    for value in n.elems:
      let found = findMediaValue(value, preferredKeys)
      if found.len > 0:
        return found
  else:
    discard
  ""

proc chargeTokens(tenantId, taskId: string, used: int): bool
proc enforcePromptBound(messages: JsonNode)

proc invokeModel(role: ModelRole, messages: JsonNode, structured = false, tenantId = "", taskId = ""): Future[LlmResponse] {.async.} =
  enforcePromptBound(messages)
  var response: LlmResponse
  case role
  of mrOrchestrator:
    response = await cerebrasCall(messages, true)
  of mrGemini38:
    let input = geminiInputFromMessages(messages)
    response = await geminiCall(input, geminiSystemInstructionFromMessages(messages))
  else:
    response = await requestyCall(role, messages, structured)
  if tenantId.len > 0 and response.totalTokens > 0:
    if not chargeTokens(tenantId, taskId, response.totalTokens):
      raise newException(IOError, "token budget exhausted")
  return response

proc enforcePromptBound(messages: JsonNode) =
  if messages.isNil or messages.kind != JArray:
    raise newException(ValueError, "messages must be an array")
  let maxBytes = positiveEnvInt("AGENT_MAX_PROMPT_BYTES", 2 * 1024 * 1024)
  let encoded = canonical(messages)
  if encoded.len > maxBytes:
    raise newException(ValueError, "prompt exceeds AGENT_MAX_PROMPT_BYTES")

proc effectiveMaxTokens(requested: int, source: string): int =
  let sourceName = source.strip().toLowerAscii()
  let providerMaximum =
    if sourceName in ["gemini", "gemini38", "gemini-3.8-flash"]: 65536
    elif sourceName in ["orchestrator", "cerebras", "gemma4", "gemma-4"]: 32768
    else: 131072
  if requested <= 0:
    return providerMaximum
  min(requested, providerMaximum)

proc chargeTokens(tenantId, taskId: string, used: int): bool =
  if used <= 0:
    return true
  acquire(store.lock)
  defer: release(store.lock)
  discard store.execUnlocked("UPDATE tenants SET tokens_used=tokens_used+? WHERE tenant_id=? AND token_budget-tokens_used>=?", @[%used, %tenantId, %used])
  if sqlite3_changes(store.handle) <= 0:
    return false
  if taskId.len > 0:
    discard store.execUnlocked("UPDATE tasks SET tokens_used=tokens_used+?,updated_at=? WHERE task_id=? AND tenant_id=?", @[%used, %nowF(), %taskId, %tenantId])
  true

proc callChatCompletionsAsync(messages: JsonNode, maxTokens: int = 0,
                              temperature: float = 0.96,
                              jsonMode: bool = false,
                              logprobs: bool = false,
                              topLogprobs: int = 20,
                              echoPrompt: bool = false,
                              topP: float = 1.0): Future[LlmResponse] {.async.} =
  enforcePromptBound(messages)
  let key = requireEnv("REQUESTY_API_KEY")
  var body = requestyBody(mrGpt6Astra, messages, jsonMode, false)
  body["max_tokens"] = %effectiveMaxTokens(maxTokens, "gpt6_astra")
  body["temperature"] = %temperature
  body["top_p"] = %topP
  if logprobs:
    body["logprobs"] = %true
    body["top_logprobs"] = %max(1, min(20, topLogprobs))
  if echoPrompt:
    body["echo"] = %true
  let headers = newHttpHeaders({
    "Authorization": "Bearer " & key,
    "Content-Type": "application/json",
    "Accept": "application/json"
  })
  let (status, raw, _) = await httpRequestAsync(RequestyBaseUrl & "/chat/completions", HttpPost, $body, headers)
  if status < 200 or status >= 300:
    raise newException(IOError, "Requesty status " & $status & ": " & raw)
  return parseOpenAiResponse(raw, pkRequesty)

proc extractJsonObject(text: string): JsonNode =
  parseJsonObjectLoose(text)

proc boundUtf8Bytes(s: string, maxBytes: int): string =
  if maxBytes <= 0:
    return ""
  if s.len <= maxBytes:
    return s
  var n = maxBytes
  while n > 0 and n < s.len and (s[n].ord and 0xc0) == 0x80:
    dec n
  if n <= 0:
    return ""
  s[0 ..< n]

proc recursiveReasonCall(tenantId, taskId, goal, context: string, depth, maxDepth, branches: int): Future[JsonNode] {.async.} =
  let requestedDepth = max(1, if maxDepth > 0: maxDepth else: max(1, depth))
  let requestedBranches = max(1, branches)
  let currentDepth = max(0, depth)
  let systemText = promptText("recursive_reason")
  let userText = "Goal:\n" & goal & "\n\nContext:\n" & context & "\n\nReasoning depth index: " & $currentDepth
  let response = await callChatCompletionsAsync(%*[
    {"role": "system", "content": systemText},
    {"role": "user", "content": userText}
  ], maxTokens = 131072, temperature = 0.0, jsonMode = true)
  if response.totalTokens > 0 and not chargeTokens(tenantId, taskId, response.totalTokens):
    raise newException(IOError, "token budget exhausted")
  var parsed = parseJsonObjectLoose(response.content)
  if parsed.isNil:
    parsed = %*{"analysis": response.content, "candidate_actions": newJArray(), "uncertainties": newJArray(), "conclusion": response.content}
  parsed["model"] = %response.model
  parsed["depth"] = %currentDepth
  if currentDepth + 1 < requestedDepth:
    var branchResults = newJArray()
    let candidates = parsed{"candidate_actions"}
    if not candidates.isNil and candidates.kind == JArray and candidates.elems.len > 0:
      let takeCount = min(requestedBranches, candidates.elems.len)
      for i in 0 ..< takeCount:
        let branchContext = context & "\n\nCandidate branch:\n" & canonical(candidates[i])
        branchResults.add(await recursiveReasonCall(tenantId, taskId, goal, branchContext, currentDepth + 1, requestedDepth, requestedBranches))
    elif requestedBranches == 1:
      branchResults.add(await recursiveReasonCall(tenantId, taskId, goal, context, currentDepth + 1, requestedDepth, requestedBranches))
    parsed["branches"] = branchResults
  return parsed

proc topProbMap(item: LogprobItem): Table[string, float] =
  result = initTable[string, float]()
  result[item.token] = exp(item.logprob)
  for candidate in item.topLogprobs:
    let probability = exp(candidate.logprob)
    if probability > result.getOrDefault(candidate.token, 0.0):
      result[candidate.token] = probability

proc tokenReverseKl(teacher, student: LogprobItem): float =
  let tp = topProbMap(teacher)
  let sp = topProbMap(student)
  var keys = initHashSet[string]()
  for key in tp.keys: keys.incl(key)
  for key in sp.keys: keys.incl(key)
  var tNorm = 0.0
  var sNorm = 0.0
  for key in keys:
    tNorm += tp.getOrDefault(key, 0.0)
    sNorm += sp.getOrDefault(key, 0.0)
  if tNorm <= 0.0 or sNorm <= 0.0:
    return 0.0
  let epsilon = 1e-12
  for key in keys:
    let p = max(epsilon, sp.getOrDefault(key, 0.0) / sNorm)
    let q = max(epsilon, tp.getOrDefault(key, 0.0) / tNorm)
    result += p * ln(p / q)

proc findAlignedTokenWindow(teacher, student: seq[LogprobItem]): seq[(int, int)] =
  result = @[]
  if teacher.len == 0 or student.len == 0:
    return
  var ti = 0
  var si = 0
  while ti < teacher.len and si < student.len:
    if teacher[ti].token == student[si].token:
      result.add((ti, si))
      inc ti
      inc si
      continue
    var found = false
    for delta in 1 .. 8:
      if ti + delta < teacher.len and teacher[ti + delta].token == student[si].token:
        ti += delta
        found = true
        break
      if si + delta < student.len and teacher[ti].token == student[si + delta].token:
        si += delta
        found = true
        break
    if not found:
      inc ti
      inc si

proc recordModelContext(h: TaskHandle, role: ModelRole, phase, content: string, metadata: JsonNode = nil)
proc persistReflection(h: TaskHandle, failurePoint, pivotAction, attribution: string, patch, verifier: JsonNode)

proc persistTask(h: TaskHandle)
proc emit(h: TaskHandle, ev: JsonNode)
proc haltForBudget(h: TaskHandle)

proc runTokenLevelDistillation(h: TaskHandle, reflectionPatch: JsonNode) {.async.} =
  acquire(h.lock)
  let goal = h.sigma{"goal"}.getStr(h.title)
  let obs = copy(h.obs)
  release(h.lock)
  let teacherPrompt = %*[
    {"role": "system", "content": promptText("distillation_teacher")},
    {"role": "user", "content": "Goal:\n" & goal & "\nObservation:\n" & canonical(obs) & "\nReflection:\n" & canonical(reflectionPatch)}
  ]
  let teacher = await callChatCompletionsAsync(teacherPrompt, maxTokens = 131072, temperature = 0.0, logprobs = true, topLogprobs = 20)
  if teacher.totalTokens > 0 and not chargeTokens(h.tenantId, h.taskId, teacher.totalTokens):
    h.haltForBudget()
    return
  let studentPrompt = %*[
    {"role": "system", "content": promptText("distillation_student")},
    {"role": "user", "content": "Goal:\n" & goal & "\nObservation:\n" & canonical(obs)}
  ]
  let student = await callChatCompletionsAsync(studentPrompt, maxTokens = 131072, temperature = 0.0, logprobs = true, topLogprobs = 20)
  if student.totalTokens > 0 and not chargeTokens(h.tenantId, h.taskId, student.totalTokens):
    h.haltForBudget()
    return
  let aligned = findAlignedTokenWindow(teacher.logprobs, student.logprobs)
  if aligned.len == 0:
    h.recordModelContext(mrGpt6Astra, "distillation_teacher", teacher.content, %*{"aligned_positions": 0, "reverse_kl": newJNull()})
    h.recordModelContext(mrGpt6Astra, "distillation_student", student.content, %*{"aligned_positions": 0})
    return
  var reverseKl = 0.0
  for pair in aligned:
    reverseKl += tokenReverseKl(teacher.logprobs[pair[0]], student.logprobs[pair[1]])
  reverseKl /= aligned.len.float
  let signalText = teacher.content & " " & reflectionPatch{"pivot_action"}.getStr("")
  let magnitude = clamp(0.02 + reverseKl * 0.01, 0.01, 0.2)
  for token in policyNgrams(signalText, 3):
    updatePolicyWeight(h.tenantId, token, magnitude)
  h.recordModelContext(mrGpt6Astra, "distillation_teacher", teacher.content, %*{"aligned_positions": aligned.len, "reverse_kl": reverseKl})
  h.recordModelContext(mrGpt6Astra, "distillation_student", student.content, %*{"aligned_positions": aligned.len})

proc reflectAndDistill(h: TaskHandle, verifierReport: JsonNode) {.async.} =
  acquire(h.lock)
  let state = copy(h.sigma)
  let observation = copy(h.obs)
  release(h.lock)
  let messages = %*[
    {"role": "system", "content": promptText("failure_diagnosis")},
    {"role": "user", "content": "State:\n" & canonical(state) & "\nObservation:\n" & canonical(observation) & "\nVerifier report:\n" & canonical(verifierReport)}
  ]
  let response = await invokeModel(mrGpt6Astra, messages, true, h.tenantId, h.taskId)
  let reflection = parseJsonObjectLoose(response.content)
  if reflection.isNil:
    return
  let patch = if reflection.hasKey("patch") and reflection["patch"].kind == JObject: copy(reflection["patch"]) else: newJObject()
  h.persistReflection(reflection{"failure_point"}.getStr("validation failed"), reflection{"pivot_action"}.getStr("replan from exact evidence"), reflection{"attribution"}.getStr("unknown"), patch, verifierReport)
  await h.runTokenLevelDistillation(reflection)

proc haltForBudget(h: TaskHandle) =
  if h.isNil:
    return
  acquire(h.lock)
  h.status = "halted"
  h.stopRequested = true
  h.verified = false
  h.terminalReason = "token budget exhausted"
  h.sigma["phase"] = %"terminal"
  h.orchestratorState = osTerminal
  h.obs = %*{"status": "halted", "error": "token budget exhausted"}
  release(h.lock)
  h.persistTask()
  h.emit(%*{"type": "done", "status": "halted", "verified": false, "reason": "token budget exhausted"})

proc defaultSigma(goal: string): JsonNode =
  %*{
    "goal": goal,
    "progress": 0.0,
    "phase": "perceive",
    "route": newJObject(),
    "plan": {
      "steps": newJArray(),
      "current_step_id": "",
      "completed_step_ids": newJArray()
    },
    "subgoals": newJArray(),
    "constraints": newJArray(),
    "facts": newJObject(),
    "blockers": newJArray(),
    "artifacts": newJArray(),
    "model_context": newJObject(),
    "runtime": {
      "instavm_session_id": "",
      "instavm_vm_id": "",
      "browser_session_id": "",
      "pty_id": "",
      "pty_ws_url": "",
      "workspace": "/app"
    },
    "system1": {
      "queued_actions": newJArray()
    },
    "system2": {
      "gate": 0.0,
      "subgoal": "",
      "strategy": "",
      "cognition": newJArray(),
      "last_observation_digest": ""
    },
    "completion_checklist": newJArray(),
    "handoffs": newJArray(),
    "step_summary": "initialized"
  }

proc ensureStateShape(sigma: JsonNode, goal: string) =
  if not sigma.hasKey("goal"): sigma["goal"] = %goal
  if not sigma.hasKey("progress"): sigma["progress"] = %0.0
  if not sigma.hasKey("phase"): sigma["phase"] = %"perceive"
  if not sigma.hasKey("route") or sigma["route"].kind != JObject: sigma["route"] = newJObject()
  if not sigma.hasKey("plan") or sigma["plan"].kind != JObject: sigma["plan"] = %*{"steps": newJArray(), "current_step_id": "", "completed_step_ids": newJArray()}
  if not sigma["plan"].hasKey("steps") or sigma["plan"]["steps"].kind != JArray: sigma["plan"]["steps"] = newJArray()
  if not sigma["plan"].hasKey("completed_step_ids") or sigma["plan"]["completed_step_ids"].kind != JArray: sigma["plan"]["completed_step_ids"] = newJArray()
  if not sigma.hasKey("subgoals") or sigma["subgoals"].kind != JArray: sigma["subgoals"] = newJArray()
  if not sigma.hasKey("constraints") or sigma["constraints"].kind != JArray: sigma["constraints"] = newJArray()
  if not sigma.hasKey("facts") or sigma["facts"].kind != JObject: sigma["facts"] = newJObject()
  if not sigma.hasKey("blockers") or sigma["blockers"].kind != JArray: sigma["blockers"] = newJArray()
  if not sigma.hasKey("artifacts") or sigma["artifacts"].kind != JArray: sigma["artifacts"] = newJArray()
  if not sigma.hasKey("model_context") or sigma["model_context"].kind != JObject: sigma["model_context"] = newJObject()
  if not sigma.hasKey("runtime") or sigma["runtime"].kind != JObject: sigma["runtime"] = newJObject()
  for entry in [("instavm_session_id", ""), ("instavm_vm_id", ""), ("browser_session_id", ""), ("pty_id", ""), ("pty_ws_url", ""), ("workspace", "/app")]:
    let key = entry[0]
    let value = entry[1]
    if not sigma["runtime"].hasKey(key): sigma["runtime"][key] = %value
  if not sigma.hasKey("system1") or sigma["system1"].kind != JObject: sigma["system1"] = newJObject()
  if not sigma["system1"].hasKey("queued_actions") or sigma["system1"]["queued_actions"].kind != JArray: sigma["system1"]["queued_actions"] = newJArray()
  if not sigma.hasKey("system2") or sigma["system2"].kind != JObject: sigma["system2"] = newJObject()
  if not sigma["system2"].hasKey("gate"): sigma["system2"]["gate"] = %0.0
  if not sigma["system2"].hasKey("subgoal"): sigma["system2"]["subgoal"] = %""
  if not sigma["system2"].hasKey("strategy"): sigma["system2"]["strategy"] = %""
  if not sigma["system2"].hasKey("cognition") or sigma["system2"]["cognition"].kind != JArray: sigma["system2"]["cognition"] = newJArray()
  if not sigma["system2"].hasKey("last_observation_digest"): sigma["system2"]["last_observation_digest"] = %""
  if not sigma.hasKey("completion_checklist") or sigma["completion_checklist"].kind != JArray: sigma["completion_checklist"] = newJArray()
  if not sigma.hasKey("handoffs") or sigma["handoffs"].kind != JArray: sigma["handoffs"] = newJArray()
  if not sigma.hasKey("step_summary"): sigma["step_summary"] = %"initialized"

proc countNodes(n: JsonNode): int =
  if n.isNil:
    return 0
  result = 1
  case n.kind
  of JObject:
    for _, value in n.fields:
      result += countNodes(value)
  of JArray:
    for value in n.elems:
      result += countNodes(value)
  else:
    discard

proc trimArrayTail(arr: JsonNode, keep: int) =
  if arr.isNil or arr.kind != JArray:
    return
  let actualKeep = max(0, keep)
  if arr.elems.len <= actualKeep:
    return
  if actualKeep == 0:
    arr.elems.setLen(0)
  else:
    arr.elems = arr.elems[arr.elems.len - actualKeep .. ^1]

proc pruneSigma(sigma: JsonNode) =
  if sigma.isNil or sigma.kind != JObject:
    return
  if sigma.hasKey("subgoals") and sigma["subgoals"].kind == JArray:
    var active = newJArray()
    var done = newJArray()
    for item in sigma["subgoals"].elems:
      let status = if item.kind == JObject: item{"status"}.getStr("").toLowerAscii() else: ""
      if status in ["done", "completed", "succeeded", "resolved"]:
        done.add(copy(item))
      else:
        active.add(copy(item))
    trimArrayTail(done, 8)
    for item in done.elems:
      active.add(item)
    sigma["subgoals"] = active
  if sigma.hasKey("blockers") and sigma["blockers"].kind == JArray:
    trimArrayTail(sigma["blockers"], 16)
  if sigma.hasKey("handoffs") and sigma["handoffs"].kind == JArray:
    trimArrayTail(sigma["handoffs"], 64)
  if sigma.hasKey("model_context") and sigma["model_context"].kind == JObject:
    for key, value in sigma["model_context"].fields:
      if value.kind == JArray:
        trimArrayTail(sigma["model_context"][key], 32)
  if sigma.hasKey("system1") and sigma["system1"].kind == JObject:
    let queue = sigma["system1"]{"queued_actions"}
    if not queue.isNil and queue.kind == JArray:
      trimArrayTail(queue, 256)
  if sigma.hasKey("facts") and sigma["facts"].kind == JObject and sigma["facts"].len > 512:
    var replacement = newJObject()
    var keys: seq[string] = @[]
    for key, _ in sigma["facts"].fields:
      keys.add(key)
    keys.sort()
    let start = max(0, keys.len - 512)
    for i in start ..< keys.len:
      replacement[keys[i]] = copy(sigma["facts"][keys[i]])
    sigma["facts"] = replacement

proc deepMerge(base, patch: JsonNode): JsonNode =
  if patch.isNil:
    return copyOrEmpty(base)
  if patch.kind != JObject:
    return copy(patch)
  var merged = if not base.isNil and base.kind == JObject: copy(base) else: newJObject()
  for key, value in patch.fields:
    if value.kind == JNull:
      if merged.hasKey(key):
        merged.delete(key)
    elif value.kind == JObject and merged.hasKey(key) and merged[key].kind == JObject:
      merged[key] = deepMerge(merged[key], value)
    else:
      merged[key] = copy(value)
  result = merged

proc collectForbiddenKeys(node: JsonNode, path = ""): seq[string] =
  result = @[]
  if node.isNil:
    return
  case node.kind
  of JObject:
    for key, value in node.fields:
      let lowered = key.toLowerAscii()
      let fullPath = if path.len == 0: key else: path & "." & key
      if lowered in ["history", "transcript", "messages", "chain_of_thought", "chain-of-thought", "private_reasoning", "reasoning_trace"]:
        result.add(fullPath)
      result.add(collectForbiddenKeys(value, fullPath))
  of JArray:
    for i, value in node.elems:
      result.add(collectForbiddenKeys(value, path & "[" & $i & "]"))
  else:
    discard

proc validatePatch(patch: JsonNode): (bool, seq[string]) =
  var errors: seq[string] = @[]
  if patch.isNil or patch.kind != JObject:
    errors.add("state patch must be a JSON object")
    return (false, errors)
  errors.add(collectForbiddenKeys(patch))
  if patch.hasKey("task_id") or patch.hasKey("tenant_id"):
    errors.add("state patch cannot modify runtime identity")
  (errors.len == 0, errors)

proc validateSigma(sigma: JsonNode): (bool, seq[string]) =
  var errors: seq[string] = @[]
  if sigma.isNil or sigma.kind != JObject:
    errors.add("state must be a JSON object")
    return (false, errors)
  for key in ["goal", "progress", "phase", "subgoals", "constraints", "facts", "blockers", "artifacts", "step_summary", "system1", "route", "plan", "runtime", "completion_checklist"]:
    if not sigma.hasKey(key):
      errors.add("missing state key: " & key)
  if sigma.hasKey("goal") and sigma["goal"].kind != JString:
    errors.add("goal must be a string")
  if sigma.hasKey("progress") and sigma["progress"].kind notin {JInt, JFloat}:
    errors.add("progress must be numeric")
  if sigma.hasKey("phase") and sigma["phase"].kind != JString:
    errors.add("phase must be a string")
  for key in ["subgoals", "constraints", "blockers", "artifacts", "completion_checklist"]:
    if sigma.hasKey(key) and sigma[key].kind != JArray:
      errors.add(key & " must be an array")
  for key in ["facts", "system1", "route", "plan", "runtime"]:
    if sigma.hasKey(key) and sigma[key].kind != JObject:
      errors.add(key & " must be an object")
  errors.add(collectForbiddenKeys(sigma))
  (errors.len == 0, errors)

proc decodePointerToken(token: string): string =
  result = token.replace("~1", "/").replace("~0", "~")

proc jsonPointerGet(root: JsonNode, pointer: string): JsonNode =
  if root.isNil:
    return nil
  if pointer.len == 0:
    return root
  if pointer[0] != '/':
    return nil
  var current = root
  for rawToken in pointer[1 .. ^1].split('/'):
    let token = decodePointerToken(rawToken)
    if current.isNil:
      return nil
    case current.kind
    of JObject:
      if not current.hasKey(token):
        return nil
      current = current[token]
    of JArray:
      if token == "-":
        return nil
      try:
        let idx = parseInt(token)
        if idx < 0 or idx >= current.elems.len:
          return nil
        current = current[idx]
      except ValueError:
        return nil
    else:
      return nil
  current

proc jsonEquivalent(a, b: JsonNode): bool =
  if a.isNil or b.isNil:
    return a.isNil and b.isNil
  canonical(a) == canonical(b)

proc persistTask(h: TaskHandle) =
  acquire(h.lock)
  pruneSigma(h.sigma)
  let stateCopy = copy(h.sigma)
  let obsCopy = copy(h.obs)
  let status = h.status
  let reason = h.terminalReason
  let verified = h.verified
  release(h.lock)
  discard store.exec("UPDATE tasks SET state_json=?, latest_obs_json=?, status=?, terminal_reason=?, verified=?, updated_at=? WHERE task_id=?",
    @[%($stateCopy), %($obsCopy), %status, %reason, %(if verified: 1 else: 0), %nowF(), %h.taskId])

proc checkpoint(h: TaskHandle, action, receipt: JsonNode) =
  acquire(h.lock)
  let stateCopy = copy(h.sigma)
  let obsCopy = copy(h.obs)
  release(h.lock)
  let rows = store.query("SELECT COALESCE(MAX(step_index),-1) AS step_index FROM checkpoints WHERE task_id=?", @[%h.taskId])
  let stepIndex = if rows.len > 0: rows[0].getInt("step_index", -1) + 1 else: 0
  let digest = sha1Hex(canonical(stateCopy) & canonical(obsCopy) & canonical(action) & canonical(receipt))
  discard store.exec("INSERT INTO checkpoints (task_id, tenant_id, step_index, state_json, obs_json, action_json, patch_json, receipt_json, digest, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
    @[%h.taskId, %(if h.tenantId.len > 0: h.tenantId else: defaultTenantId), %stepIndex, %($stateCopy), %($obsCopy), %($action), %"{}", %($receipt), %digest, %nowF()])
  discard store.exec("UPDATE tasks SET step_index=?, updated_at=? WHERE task_id=?", @[%stepIndex, %nowF(), %h.taskId])
  acquire(h.lock)
  h.stepIndex = stepIndex.int
  release(h.lock)

proc persistTaskEvent(taskId: string, ev: JsonNode) =
  discard store.exec("INSERT INTO task_events (task_id, sequence, event_json, created_at) SELECT ?, COALESCE(MAX(sequence),0)+1, ?, ? FROM task_events WHERE task_id=?",
    @[%taskId, %($ev), %nowF(), %taskId])

proc emit(h: TaskHandle, ev: JsonNode) =
  var event = copy(ev)
  if event.kind != JObject:
    event = %*{"type": "event", "payload": event}
  if not event.hasKey("task_id"):
    event["task_id"] = %h.taskId
  event["time"] = %nowF()
  persistTaskEvent(h.taskId, event)
  acquire(h.lock)
  let localSubs = h.subscribers
  release(h.lock)
  for sub in localSubs:
    try:
      sub.cb(event)
    except CatchableError:
      discard
  acquire(sseLock)
  let globalSubs = sseSubscribers.getOrDefault("default", @[])
  release(sseLock)
  for sub in globalSubs:
    try:
      sub.cb(event)
    except CatchableError:
      discard

proc transitionOrchestrator(h: TaskHandle, nextState: OrchestratorState) =
  acquire(h.lock)
  let previous = h.orchestratorState
  let graph = buildOrchestratorGraph()
  if not canOrchestratorTransition(graph, previous, nextState):
    release(h.lock)
    raise newException(ValueError, "invalid orchestrator transition: " & orchestratorStateName(previous) & " -> " & orchestratorStateName(nextState))
  h.orchestratorState = nextState
  h.sigma["phase"] = %orchestratorStateName(nextState)
  release(h.lock)
  if previous != nextState:
    h.emit(%*{"type": "orchestrator_state", "from": orchestratorStateName(previous), "to": orchestratorStateName(nextState)})

proc recordModelContext(h: TaskHandle, role: ModelRole, phase, content: string, metadata: JsonNode) =
  let key = modelRoleName(role)
  let meta = if metadata.isNil: newJObject() else: copy(metadata)
  let entry = %*{"time": nowF(), "phase": phase, "content": content, "metadata": meta}
  acquire(h.lock)
  if not h.sigma["model_context"].hasKey(key) or h.sigma["model_context"][key].kind != JArray:
    h.sigma["model_context"][key] = newJArray()
  h.sigma["model_context"][key].add(entry)
  release(h.lock)

proc subAgentTreeJson(taskId: string): JsonNode =
  result = newJArray()
  for r in store.query("SELECT agent_id,parent_agent_id,model_role,name,goal,status,result,error,created_at,updated_at FROM subagents WHERE task_id=? ORDER BY created_at ASC", @[%taskId]):
    result.add(%*{
      "agent_id": r.getStr("agent_id"),
      "parent_agent_id": r.getStr("parent_agent_id"),
      "model": r.getStr("model_role"),
      "name": r.getStr("name"),
      "goal": r.getStr("goal"),
      "status": r.getStr("status"),
      "result": r.getStr("result"),
      "error": r.getStr("error"),
      "created_at": r.getFloat("created_at"),
      "updated_at": r.getFloat("updated_at")
    })

proc runningSubAgentCount(taskId: string): int =
  let rows = store.query("SELECT COUNT(*) AS n FROM subagents WHERE task_id=? AND status IN ('queued','running','stopping')", @[%taskId])
  if rows.len > 0:
    return rows[0].getInt("n").int
  0

proc recordHandoff(h: TaskHandle, fromRole, toRole: ModelRole, reason: string, payload: JsonNode = nil) =
  let data = if payload.isNil: newJObject() else: copy(payload)
  let item = %*{"time": nowF(), "from": modelRoleName(fromRole), "to": modelRoleName(toRole), "reason": reason, "payload": data}
  acquire(h.lock)
  h.sigma["handoffs"].add(item)
  release(h.lock)
  h.emit(%*{"type": "model_handoff", "from": modelRoleName(fromRole), "to": modelRoleName(toRole), "reason": reason})

proc currentTraceStep(taskId: string): int64 =
  let rows = store.query("SELECT COALESCE(MAX(step_index),-1) AS step_index FROM raw_traces WHERE task_id=?", @[%taskId])
  if rows.len == 0: 0'i64 else: rows[0].getInt("step_index", -1) + 1'i64

proc logRawTrace(h: TaskHandle, skillId: string, preState, action, observation, delta, postState, receipt: JsonNode, success: bool, latencyMs: int) =
  let stepIndex = currentTraceStep(h.taskId)
  let traceId = newId("trace")
  let digest = sha1Hex(h.taskId & ":" & $stepIndex & ":" & canonical(preState) & ":" & canonical(action) & ":" & canonical(observation) & ":" & canonical(postState) & ":" & canonical(receipt))
  discard store.exec("INSERT INTO raw_traces (trace_id, task_id, tenant_id, step_index, initial_state_json, skill_id, action_json, obs_json, delta_json, post_state_json, success, latency_ms, receipt_json, immutable_hash, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    @[%traceId, %h.taskId, %(if h.tenantId.len > 0: h.tenantId else: defaultTenantId), %stepIndex, %($preState), %skillId, %($action), %($observation), %($delta), %($postState), %(if success: 1 else: 0), %latencyMs, %($receipt), %digest, %nowF()])

proc persistCognition(h: TaskHandle, vector: JsonNode, gate: float, subgoal, strategy: string, route: JsonNode) =
  let stepIndex = currentTraceStep(h.taskId)
  discard store.exec("INSERT INTO cognition (cog_id, task_id, tenant_id, step_index, vector_json, gate, subgoal, strategy, route_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
    @[%newId("cog"), %h.taskId, %(if h.tenantId.len > 0: h.tenantId else: defaultTenantId), %stepIndex, %($vector), %gate, %subgoal, %strategy, %($route), %nowF()])

proc persistReflection(h: TaskHandle, failurePoint, pivotAction, attribution: string, patch, verifier: JsonNode) =
  discard store.exec("INSERT INTO reflections (reflection_id, task_id, tenant_id, patch_json, failure_point, pivot_action, attribution, verifier_report_json, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
    @[%newId("ref"), %h.taskId, %(if h.tenantId.len > 0: h.tenantId else: defaultTenantId), %($patch), %failurePoint, %pivotAction, %attribution, %($verifier), %nowF()])
  let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
  for token in contentTerms(failurePoint & " " & attribution & " " & pivotAction):
    updatePolicyWeight(tenant, token, -0.02)

proc validatePlanGraph(plan: JsonNode, completedIds: HashSet[string] = initHashSet[string]()) =
  if plan.isNil or plan.kind != JArray:
    raise newException(ValueError, "plan must be an array")
  var ids = completedIds
  var localIds = initHashSet[string]()
  for step in plan.elems:
    if step.kind != JObject:
      raise newException(ValueError, "plan step must be an object")
    let id = step{"id"}.getStr("").strip()
    if id.len == 0:
      raise newException(ValueError, "plan step id is required")
    if id in localIds or id in completedIds:
      raise newException(ValueError, "duplicate plan step id: " & id)
    localIds.incl(id)
    ids.incl(id)
  var dependencies = initTable[string, seq[string]]()
  for step in plan.elems:
    let id = step{"id"}.getStr("").strip()
    let deps = step{"depends_on"}
    if deps.kind != JArray:
      raise newException(ValueError, "plan step depends_on must be an array")
    var depList: seq[string] = @[]
    for dep in deps.elems:
      if dep.kind != JString:
        raise newException(ValueError, "plan dependency must be a string")
      let depId = dep.getStr("").strip()
      if depId.len == 0:
        raise newException(ValueError, "plan dependency must not be empty")
      if depId == id:
        raise newException(ValueError, "plan step cannot depend on itself: " & id)
      if depId notin ids:
        raise newException(ValueError, "plan references unknown dependency: " & depId)
      depList.add(depId)
    dependencies[id] = depList
  var visiting = initHashSet[string]()
  var visited = initHashSet[string]()
  proc visit(id: string) =
    if id in completedIds or id in visited:
      return
    if id in visiting:
      raise newException(ValueError, "plan dependency cycle detected at: " & id)
    visiting.incl(id)
    for depId in dependencies.getOrDefault(id, @[]):
      if depId in localIds:
        visit(depId)
    visiting.excl(id)
    visited.incl(id)
  for id in localIds:
    visit(id)

proc toolCatalog(): JsonNode

proc routeDecisionFromJson(node: JsonNode): RouteDecision =
  if node.isNil or node.kind != JObject:
    raise newException(ValueError, "orchestrator returned no JSON object")
  result.raw = copy(node)
  result.intent = node{"intent"}.getStr("").strip()
  if result.intent.len == 0:
    raise newException(ValueError, "orchestrator route is missing intent")
  let primaryName = node{"primary_model"}.getStr("").strip()
  if primaryName.len == 0:
    raise newException(ValueError, "orchestrator route is missing primary_model")
  result.primaryModel = parseModelRole(primaryName)
  if result.primaryModel == mrOrchestrator:
    raise newException(ValueError, "orchestrator cannot select itself as primary specialist")
  result.secondaryModels = @[]
  if node.hasKey("secondary_models"):
    if node["secondary_models"].kind != JArray:
      raise newException(ValueError, "secondary_models must be an array")
    for it in node["secondary_models"].elems:
      if it.kind != JString:
        raise newException(ValueError, "secondary_models entries must be strings")
      let role = parseModelRole(it.getStr(""))
      if role == mrOrchestrator:
        raise newException(ValueError, "orchestrator cannot be selected as a secondary specialist")
      if role != result.primaryModel and role notin result.secondaryModels:
        result.secondaryModels.add(role)
  result.requiresVm = node{"requires_vm"}.getBool(false)
  result.requiresBrowser = node{"requires_browser"}.getBool(false)
  result.requiresDesktop = node{"requires_desktop"}.getBool(false)
  result.requiresVisualAnalysis = node{"requires_visual_analysis"}.getBool(false)
  result.requiresDocumentAnalysis = node{"requires_document_analysis"}.getBool(false)
  result.plan = newJArray()
  if node.hasKey("plan"):
    if node["plan"].kind != JArray:
      raise newException(ValueError, "plan must be an array")
    for i, item in node["plan"].elems:
      result.plan.add(normalizePlanStep(item, i))
  result.delegations = newJArray()
  if node.hasKey("delegations"):
    if node["delegations"].kind != JArray:
      raise newException(ValueError, "delegations must be an array")
    for item in node["delegations"].elems:
      if item.kind != JObject:
        raise newException(ValueError, "delegation entries must be objects")
      let modelName = item{"model"}.getStr("").strip()
      let goal = item{"goal"}.getStr("").strip()
      if modelName.len == 0 or goal.len == 0:
        raise newException(ValueError, "every delegation requires model and goal")
      let role = parseModelRole(modelName)
      var normalized = copy(item)
      normalized["model"] = %modelRoleName(role)
      result.delegations.add(normalized)
  result.completionCriteria = if node.hasKey("completion_criteria") and node["completion_criteria"].kind == JArray: copy(node["completion_criteria"]) else: newJArray()
  validatePlanGraph(result.plan)
  let executionRequired = result.requiresVm or result.requiresBrowser or result.requiresDesktop or result.requiresVisualAnalysis or result.requiresDocumentAnalysis or result.delegations.elems.len > 0
  if executionRequired and result.plan.elems.len == 0:
    raise newException(ValueError, "execution routes must contain at least one explicit plan step")
  if result.plan.elems.len > 0 and result.completionCriteria.elems.len == 0:
    raise newException(ValueError, "execution routes must contain completion_criteria")
  result.raw["primary_model"] = %modelRoleName(result.primaryModel)
  var normalizedSecondary = newJArray()
  for role in result.secondaryModels:
    normalizedSecondary.add(%modelRoleName(role))
  result.raw["secondary_models"] = normalizedSecondary
  result.raw["plan"] = copy(result.plan)
  result.raw["delegations"] = copy(result.delegations)

proc routeTask(goal: string, sigma: JsonNode, obs: JsonNode, tenantId = "", taskId = ""): Future[RouteDecision] {.async.} =
  let systemText = promptText("orchestrator_router")
  let baseUserText = "GOAL:\n" & goal & "\n\nCURRENT STATE:\n" & canonical(sigma) & "\n\nLATEST OBSERVATION:\n" & canonical(obs) & "\n\nAVAILABLE REAL TOOLS:\n" & canonical(toolCatalog()) & "\n\nAVAILABLE MODEL REFERENCE SKILLS:\n" & referenceSkillCatalog() & "\n\nVALID SPECIALIST MODEL ROLE IDS:\n" & configuredModelRoleNames().join(", ") & "\n\nVALID SUBAGENT MODEL ROLE IDS:\n" & configuredSubAgentModelRoleNames().join(", ")
  var messages = %*[
    {"role": "system", "content": systemText},
    {"role": "user", "content": baseUserText}
  ]
  var lastError = ""
  for attempt in 0 .. 2:
    let resp = await cerebrasCall(messages, true)
    if tenantId.len > 0 and resp.totalTokens > 0 and not chargeTokens(tenantId, taskId, resp.totalTokens):
      raise newException(IOError, "token budget exhausted")
    let node = parseJsonObjectLoose(resp.content)
    try:
      return routeDecisionFromJson(node)
    except CatchableError as e:
      lastError = e.msg
      messages.add(%*{"role": "assistant", "content": resp.content})
      messages.add(%*{"role": "user", "content": "The routing object was invalid: " & e.msg & "\nRe-evaluate the complete task using the reference skills and return a corrected routing JSON object. You must make the model choice yourself; do not ask the backend to infer it."})
  raise newException(ValueError, "orchestrator failed to return a valid model decision: " & lastError)

proc applyRoute(h: TaskHandle, route: RouteDecision) =
  acquire(h.lock)
  h.sigma["route"] = copy(route.raw)
  var steps = copy(route.plan)
  for i in 0 ..< steps.elems.len:
    if steps[i].kind == JObject and not steps[i].hasKey("status"):
      steps[i]["status"] = %"pending"
  h.sigma["plan"] = %*{
    "steps": steps,
    "current_step_id": "",
    "completed_step_ids": newJArray(),
    "completion_criteria": copy(route.completionCriteria)
  }
  h.sigma["completion_checklist"] = newJArray()
  for criterion in route.completionCriteria.elems:
    h.sigma["completion_checklist"].add(%*{"criterion": criterion.getStr($criterion), "status": "pending", "evidence": ""})
  h.sigma["step_summary"] = %("routed: " & route.intent)
  if route.delegations.kind == JArray and route.delegations.elems.len > 0:
    for delegation in route.delegations.elems:
      var args = copy(delegation)
      h.sigma["system1"]["queued_actions"].add(%*{"tool": "spawn_subagent", "args": args})
    h.sigma["system1"]["queued_actions"].add(%*{"tool": "wait_subagents", "args": %*{"scope": "direct_children"}})
  h.lastPlannedDigest = sha1Hex(canonical(h.sigma))
  release(h.lock)
  h.transitionOrchestrator(osDeliberate)
  h.persistTask()
  h.emit(%*{"type": "route", "intent": route.intent, "model": modelRoleName(route.primaryModel), "route": route.raw})
  h.emit(%*{"type": "plan", "steps": route.plan, "completion_criteria": route.completionCriteria})

proc currentPlanStep(sigma: JsonNode): JsonNode =
  if sigma.isNil or sigma.kind != JObject:
    return nil
  let plan = sigma{"plan"}
  if plan.kind != JObject:
    return nil
  let steps = plan{"steps"}
  if steps.kind != JArray:
    return nil
  var byId = initTable[string, string]()
  for step in steps.elems:
    if step.kind != JObject:
      raise newException(ValueError, "persisted plan contains a non-object step")
    let id = step{"id"}.getStr("").strip()
    if id.len == 0:
      raise newException(ValueError, "persisted plan contains a step without id")
    if byId.hasKey(id):
      raise newException(ValueError, "persisted plan contains duplicate step id: " & id)
    byId[id] = step{"status"}.getStr("")
  for step in steps.elems:
    let status = step{"status"}.getStr("pending")
    if status notin ["pending", "running", "blocked", "needs_replan"]:
      continue
    var depsOk = true
    let deps = step{"depends_on"}
    if deps.kind != JArray:
      raise newException(ValueError, "persisted plan depends_on must be an array")
    for dep in deps.elems:
      if dep.kind != JString:
        raise newException(ValueError, "persisted plan dependency must be a string")
      let depId = dep.getStr("").strip()
      if depId.len == 0 or not byId.hasKey(depId):
        raise newException(ValueError, "persisted plan references an invalid dependency")
      if byId[depId] != "completed":
        depsOk = false
        break
    if depsOk:
      return copy(step)
  nil

proc setStepStatus(h: TaskHandle, stepId, status: string) =
  if status notin ["pending", "running", "blocked", "needs_replan", "completed", "failed"]:
    raise newException(ValueError, "invalid plan step status: " & status)
  acquire(h.lock)
  let steps = h.sigma{"plan"}{"steps"}
  var matches = 0
  if steps.kind == JArray:
    for i in 0 ..< steps.elems.len:
      if steps[i].kind == JObject and steps[i]{"id"}.getStr("") == stepId:
        inc matches
        steps[i]["status"] = %status
        if status == "completed":
          var already = false
          let done = h.sigma["plan"]["completed_step_ids"]
          for it in done.elems:
            if it.kind == JString and it.getStr("") == stepId:
              already = true
              break
          if not already:
            done.add(%stepId)
  if matches != 1:
    release(h.lock)
    raise newException(ValueError, "plan step id is missing or ambiguous: " & stepId)
  h.sigma["plan"]["current_step_id"] = %(if status == "running": stepId else: "")
  release(h.lock)
  h.persistTask()

proc updateProgress(h: TaskHandle) =
  acquire(h.lock)
  let steps = h.sigma{"plan"}{"steps"}
  if steps.kind == JArray and steps.elems.len > 0:
    var done = 0
    for step in steps.elems:
      if step.kind == JObject and step{"status"}.getStr("") == "completed":
        inc done
    h.sigma["progress"] = %(done.float / steps.elems.len.float)
  release(h.lock)
  h.persistTask()

proc replaceRemainingPlan(h: TaskHandle, replacement: JsonNode) =
  if replacement.isNil or replacement.kind != JArray or replacement.elems.len == 0:
    raise newException(ValueError, "replacement plan must contain at least one model-selected step")
  acquire(h.lock)
  let existing = copy(h.sigma{"plan"}{"steps"})
  release(h.lock)
  var completedSteps = newJArray()
  var completedIds = initHashSet[string]()
  if existing.kind == JArray:
    for step in existing.elems:
      if step.kind == JObject and step{"status"}.getStr("") == "completed":
        let id = step{"id"}.getStr("").strip()
        if id.len == 0 or id in completedIds:
          raise newException(ValueError, "completed plan contains missing or duplicate step id")
        completedIds.incl(id)
        completedSteps.add(copy(step))
  var normalizedReplacement = newJArray()
  for i, item in replacement.elems:
    normalizedReplacement.add(normalizePlanStep(item, i))
  validatePlanGraph(normalizedReplacement, completedIds)
  var mergedSteps = newJArray()
  for step in completedSteps.elems:
    mergedSteps.add(copy(step))
  for step in normalizedReplacement.elems:
    mergedSteps.add(copy(step))
  acquire(h.lock)
  h.sigma["plan"]["steps"] = mergedSteps
  h.sigma["plan"]["current_step_id"] = %""
  release(h.lock)
  h.updateProgress()

proc registerTool(name, description: string, schema: JsonNode, handler: ToolHandler) =
  toolRegistry[name] = ToolSpec(name: name, description: description, schema: schema, handler: handler)

proc toolCatalog(): JsonNode =
  result = newJArray()
  for name, spec in toolRegistry:
    result.add(%*{"name": name, "description": spec.description, "arguments": spec.schema})

proc instavmHeaders(): HttpHeaders =
  let key = requireEnv("INSTAVM_API_KEY")
  result = newHttpHeaders({
    "X-API-Key": key,
    "Content-Type": "application/json",
    "Accept": "application/json"
  })

proc runtimeNode(h: TaskHandle): JsonNode =
  acquire(h.lock)
  result = copy(h.sigma["runtime"])
  release(h.lock)

proc saveRuntimeField(h: TaskHandle, key, value: string) =
  acquire(h.lock)
  h.sigma["runtime"][key] = %value
  release(h.lock)
  h.persistTask()

proc createInstaVmSession(h: TaskHandle): Future[string] {.async.} =
  let existing = h.runtimeNode(){"instavm_session_id"}.getStr("")
  if existing.len > 0:
    return existing
  let body = %*{
    "api_key": requireEnv("INSTAVM_API_KEY"),
    "vm_lifetime_seconds": VmLifetimeSeconds,
    "memory_mb": VmDefaultMemoryMb,
    "vcpu_count": VmDefaultVcpuCount,
    "metadata": {"task_id": h.taskId},
    "env": newJObject(),
    "prewarm": false
  }
  let (status, raw, _) = await httpRequestAsync(InstaVmBaseUrl & "/v1/sessions/session", HttpPost, $body, instavmHeaders())
  if status < 200 or status >= 300:
    raise newException(IOError, "InstaVM session status " & $status & ": " & raw)
  let j = parseJson(raw)
  let sid = j{"session_id"}.getStr(j{"id"}.getStr(""))
  if sid.len == 0:
    raise newException(IOError, "InstaVM session response has no session_id")
  h.saveRuntimeField("instavm_session_id", sid)
  if j{"vm_id"}.getStr("").len > 0:
    h.saveRuntimeField("instavm_vm_id", j{"vm_id"}.getStr(""))
  h.emit(%*{"type": "runtime", "runtime": "instavm", "session_id": sid})
  return sid

proc instavmExecute(h: TaskHandle, command, language: string): Future[ToolResult] {.async.} =
  let sid = await h.createInstaVmSession()
  let body = %*{
    "command": command,
    "session_id": sid,
    "language": language
  }
  let (status, raw, _) = await httpRequestAsync(InstaVmBaseUrl & "/execute", HttpPost, $body, instavmHeaders())
  if status < 200 or status >= 300:
    return ToolResult(ok: false, payload: %*{"status": status, "body": raw}, receipt: "instavm:execute:error", message: "InstaVM execute status " & $status)
  let j = parseJson(raw)
  let success = j{"success"}.getBool(status >= 200 and status < 300)
  let output = j{"output"}.getStr("")
  var payload = copy(j)
  if payload.kind != JObject:
    payload = %*{"raw": j}
  payload["session_id"] = %sid
  if not payload.hasKey("stdout"):
    payload["stdout"] = %output
  if not payload.hasKey("stderr"):
    payload["stderr"] = %j{"error"}.getStr("")
  if j{"vm_id"}.getStr("").len > 0:
    h.saveRuntimeField("instavm_vm_id", j{"vm_id"}.getStr(""))
  return ToolResult(ok: success, payload: payload, receipt: "instavm:execute:" & sha1Hex(command & $status), message: (if success: "execution completed" else: j{"error"}.getStr(output)))

proc instavmJson(h: TaskHandle, path: string, httpMethodValue: HttpMethod, body: JsonNode = nil): Future[ToolResult] {.async.} =
  discard await h.createInstaVmSession()
  let bodyText = if body.isNil: "" else: $body
  let (status, raw, _) = await httpRequestAsync(InstaVmBaseUrl & path, httpMethodValue, bodyText, instavmHeaders())
  var payload: JsonNode
  try:
    payload = parseJson(raw)
  except CatchableError:
    payload = %*{"body_base64": base64.encode(raw)}
  let ok = status >= 200 and status < 300
  return ToolResult(ok: ok, payload: payload, receipt: "instavm:" & sha1Hex(path & bodyText & $status), message: (if ok: "ok" else: "InstaVM status " & $status))

proc browserSession(h: TaskHandle): Future[string] {.async.} =
  let existing = h.runtimeNode(){"browser_session_id"}.getStr("")
  if existing.len > 0:
    return existing
  let sid = await h.createInstaVmSession()
  let body = %*{
    "session_id": sid,
    "viewport_width": 1920,
    "viewport_height": 1080
  }
  let res = await h.instavmJson("/v1/browser/sessions", HttpPost, body)
  if not res.ok:
    raise newException(IOError, res.message & ": " & canonical(res.payload))
  let bid = res.payload{"session_id"}.getStr(res.payload{"id"}.getStr(res.payload{"browser_session_id"}.getStr("")))
  if bid.len == 0:
    raise newException(IOError, "InstaVM browser session response has no session id")
  h.saveRuntimeField("browser_session_id", bid)
  return bid

proc browserAction(h: TaskHandle, action: string, args: JsonNode): Future[ToolResult] {.async.} =
  let bid = await h.browserSession()
  var body = if args.isNil or args.kind != JObject: newJObject() else: copy(args)
  body["session_id"] = %bid
  return await h.instavmJson("/v1/browser/interactions/" & action, HttpPost, body)

proc desktopProxy(h: TaskHandle, path: string, httpMethodValue: HttpMethod, body: JsonNode = nil): Future[ToolResult] {.async.} =
  let sid = await h.createInstaVmSession()
  return await h.instavmJson("/v1/computeruse/" & encodeUrl(sid) & path, httpMethodValue, body)

proc shellQuote(s: string): string =
  result = "'" & s.replace("'", "'\\''") & "'"

proc addArtifact(h: TaskHandle, name, path, kind, mimeType: string, metadata: JsonNode = nil): string =
  let id = newId("artifact")
  let meta = if metadata.isNil: newJObject() else: copy(metadata)
  discard store.exec("INSERT INTO artifacts (artifact_id, task_id, name, path, kind, mime_type, metadata_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
    @[%id, %h.taskId, %name, %path, %kind, %mimeType, %($meta), %nowF()])
  let item = %*{"artifact_id": id, "name": name, "path": path, "kind": kind, "mime_type": mimeType, "metadata": meta}
  acquire(h.lock)
  h.sigma["artifacts"].add(item)
  release(h.lock)
  h.persistTask()
  h.emit(%*{"type": "artifact", "artifact": item})
  id

proc spawnSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult]
proc waitSubAgentsTool(h: TaskHandle, args: JsonNode): Future[ToolResult]
proc listSubAgentsTool(h: TaskHandle, args: JsonNode): Future[ToolResult]
proc getSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult]
proc messageSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult]
proc stopSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult]

proc registerTools() =
  registerTool("spawn_subagent", "Create and immediately launch a persistent autonomous child agent using any configured model, with the child model and goal explicitly chosen by the caller model. Give every child agent the complete tool registry and allow it to recursively create its own children.", %*{"model": "string", "goal": "string", "name": "string optional", "instructions": "string optional", "context": "object optional", "wait": "bool optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await spawnSubAgentTool(h, args))
  registerTool("wait_subagents", "Wait for the child agents specified by ID, or all direct children of the current actor if no IDs are supplied, to reach a terminal state. Return their actual terminal results.", %*{"agent_ids": "string[] optional", "scope": "direct_children|descendants|task optional", "timeout_ms": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await waitSubAgentsTool(h, args))
  registerTool("list_subagents", "Return the persistent subagent tree for the current task, including model, parent, goal, status and terminal result.", %*{"scope": "direct_children|descendants|task optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await listSubAgentsTool(h, args))
  registerTool("get_subagent", "Return the current persisted state and result of one subagent.", %*{"agent_id": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await getSubAgentTool(h, args))
  registerTool("message_subagent", "Send new instructions or evidence to a running subagent by appending the message to its persistent context without resetting its work.", %*{"agent_id": "string", "message": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await messageSubAgentTool(h, args))
  registerTool("stop_subagent", "Request a subagent to stop after its current awaited operation and preserve its state.", %*{"agent_id": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await stopSubAgentTool(h, args))
  registerTool("get_plan", "Return the complete current dynamic PlanSteps graph, including dependencies, statuses, completion criteria and progress. Every model and subagent may inspect the shared root plan before deciding its next action.", %*{},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      acquire(h.lock)
      let plan = copy(h.sigma{"plan"})
      let checklist = copy(h.sigma{"completion_checklist"})
      let progress = h.sigma{"progress"}.getFloat(0.0)
      release(h.lock)
      let payload = %*{"plan": plan, "completion_checklist": checklist, "progress": progress}
      return ToolResult(ok: true, payload: payload, receipt: "plan:" & sha1Hex(canonical(payload)), message: "current plan returned"))
  registerTool("replace_remaining_plan", "Replace the unfinished portion of the shared dynamic PlanSteps graph with a new model-selected dependency graph. Completed steps are preserved. Every replacement step must explicitly provide id, goal, model, execution_mode and depends_on. The backend validates graph consistency but never chooses the strategy or model.", %*{"steps": "array"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let steps = args{"steps"}
      if steps.kind != JArray or steps.elems.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "steps must be a non-empty array")
      try:
        h.replaceRemainingPlan(steps)
        acquire(h.lock)
        let plan = copy(h.sigma{"plan"})
        release(h.lock)
        h.emit(%*{"type": "plan_mutated", "plan": plan})
        return ToolResult(ok: true, payload: %*{"plan": plan}, receipt: "plan:" & sha1Hex(canonical(plan)), message: "remaining plan replaced")
      except CatchableError as e:
        return ToolResult(ok: false, payload: %*{"error": e.msg}, receipt: "", message: e.msg))
  registerTool("vm_python_exec", "Execute real Python code in the task's persistent InstaVM.", %*{"code": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.instavmExecute(args{"code"}.getStr(""), "python"))
  registerTool("vm_bash_exec", "Execute a real Bash command in the task's persistent InstaVM.", %*{"command": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.instavmExecute(args{"command"}.getStr(""), "bash"))
  registerTool("vm_read_file", "Read a real file from the InstaVM using the persistent task environment.", %*{"path": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("")
      if path.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      return await h.instavmExecute("python3 - <<'PY'\nfrom pathlib import Path\np=Path(" & escapeJson(path) & ")\nprint(p.read_text(errors='replace'))\nPY", "bash"))
  registerTool("vm_write_file", "Write complete text content to a real file in the InstaVM.", %*{"path": "string", "content": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("")
      let content = args{"content"}.getStr("")
      if path.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let encoded = base64.encode(content)
      let code = "import base64, pathlib\np=pathlib.Path(" & escapeJson(path) & ")\np.parent.mkdir(parents=True,exist_ok=True)\np.write_bytes(base64.b64decode(" & escapeJson(encoded) & "))\nprint(str(p))"
      return await h.instavmExecute(code, "python"))
  registerTool("vm_append_file", "Append complete text content to a real file in the persistent InstaVM.", %*{"path": "string", "content": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("")
      let content = args{"content"}.getStr("")
      if path.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let encoded = base64.encode(content)
      let code = "import base64,pathlib\np=pathlib.Path(" & escapeJson(path) & ")\np.parent.mkdir(parents=True,exist_ok=True)\nwith p.open('ab') as f:f.write(base64.b64decode(" & escapeJson(encoded) & "))\nprint(str(p))"
      return await h.instavmExecute(code, "python"))
  registerTool("vm_replace_text", "Replace exact text in a real file in the persistent InstaVM and report the replacement count.", %*{"path": "string", "old": "string", "new": "string", "count": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("")
      let oldText = args{"old"}.getStr("")
      let newText = args{"new"}.getStr("")
      let count = args{"count"}.getInt(-1)
      if path.len == 0 or oldText.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path and old required")
      let old64 = base64.encode(oldText)
      let new64 = base64.encode(newText)
      let code = "import base64,pathlib\np=pathlib.Path(" & escapeJson(path) & ")\ns=p.read_text(errors='replace')\na=base64.b64decode(" & escapeJson(old64) & ").decode()\nb=base64.b64decode(" & escapeJson(new64) & ").decode()\nn=" & $count & "\nc=s.count(a) if n<0 else min(s.count(a),n)\ns=s.replace(a,b,n) if n>=0 else s.replace(a,b)\np.write_text(s)\nprint(c)"
      return await h.instavmExecute(code, "python"))
  registerTool("vm_delete_file", "Delete a real file or directory from the persistent InstaVM.", %*{"path": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("")
      if path.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let code = "import pathlib,shutil\np=pathlib.Path(" & escapeJson(path) & ")\nexists=p.exists() or p.is_symlink()\n(shutil.rmtree(p) if p.is_dir() and not p.is_symlink() else p.unlink()) if exists else None\nprint('deleted' if exists else 'absent')"
      return await h.instavmExecute(code, "python"))
  registerTool("vm_search_files", "Search real files recursively in the persistent InstaVM for exact text and return matching paths and line numbers.", %*{"path": "string optional", "query": "string", "case_sensitive": "bool optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let root = args{"path"}.getStr("/app")
      let query = args{"query"}.getStr("")
      if query.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "query required")
      let q64 = base64.encode(query)
      let sensitive = if args{"case_sensitive"}.getBool(false): "True" else: "False"
      let code = "import base64,json,pathlib\nroot=pathlib.Path(" & escapeJson(root) & ")\nq=base64.b64decode(" & escapeJson(q64) & ").decode(errors='replace')\ncase=" & sensitive & "\nh=[]\nfor p in root.rglob('*'):\n if not p.is_file():continue\n try:s=p.read_text(errors='replace')\n except Exception:continue\n n=q if case else q.lower(); t=s if case else s.lower()\n if n in t:\n  for i,line in enumerate(s.splitlines(),1):\n   if n in (line if case else line.lower()):h.append({'path':str(p),'line':i,'text':line})\nprint(json.dumps({'matches':h},ensure_ascii=False))"
      return await h.instavmExecute(code, "python"))
  registerTool("vm_check_file", "Verify real file existence and required text fragments in the persistent InstaVM.", %*{"path": "string", "contains": "string[] optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("")
      if path.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let required = if args.hasKey("contains") and args["contains"].kind == JArray: args["contains"] else: newJArray()
      let encoded = base64.encode($required)
      let code = "import base64,json,pathlib\np=pathlib.Path(" & escapeJson(path) & ")\nreq=json.loads(base64.b64decode(" & escapeJson(encoded) & ").decode())\nexists=p.exists()\ns=p.read_text(errors='replace') if exists and p.is_file() else ''\nchecks=[{'text':x,'present':x in s} for x in req]\nprint(json.dumps({'exists':exists,'checks':checks,'ok':exists and all(x['present'] for x in checks)},ensure_ascii=False))"
      let res = await h.instavmExecute(code, "python")
      if not res.ok:
        return res
      let output = res.payload{"output"}.getStr(res.payload{"stdout"}.getStr("")).strip()
      try:
        let parsed = parseJson(output)
        return ToolResult(ok: parsed{"ok"}.getBool(false), payload: parsed, receipt: "check:" & sha1Hex(path & output), message: (if parsed{"ok"}.getBool(false): "verified" else: "verification failed"))
      except CatchableError:
        return ToolResult(ok: false, payload: res.payload, receipt: res.receipt, message: "verification output was not valid JSON"))
  registerTool("vm_document_extract", "Extract complete readable content from a real document inside the persistent InstaVM, including OCR-visible PDF content and readable package parts.", %*{"path": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("")
      if path.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let code = """import io, json, pathlib, subprocess, sys, zipfile, xml.etree.ElementTree as ET, shutil
p=pathlib.Path(""" & escapeJson(path) & """)
if not p.exists():
    raise FileNotFoundError(str(p))
ext=p.suffix.lower()
def ensure(module, package):
    try:
        return __import__(module)
    except ImportError:
        subprocess.check_call([sys.executable,'-m','pip','install','-q',package])
        return __import__(module)
def paragraphs(blob):
    root=ET.fromstring(blob)
    out=[]
    for node in root.iter():
        if node.tag.endswith('}p'):
            text=''.join(node.itertext()).strip()
            if text:
                out.append(text)
    return '\n'.join(out)
def package_parts(z, names):
    out=[]
    for name in names:
        if name in z.namelist():
            text=paragraphs(z.read(name))
            if text:
                out.append('=== %s ===\n%s' % (name,text))
    return out
if ext=='.pdf':
    fitz=ensure('fitz','PyMuPDF')
    pytesseract=ensure('pytesseract','pytesseract')
    Image=ensure('PIL.Image','Pillow')
    if shutil.which('tesseract') is None:
        subprocess.check_call(['sudo','apt-get','update','-qq'])
        subprocess.check_call(['sudo','apt-get','install','-y','-qq','tesseract-ocr'])
    doc=fitz.open(str(p))
    pages=[]
    for i,page in enumerate(doc,1):
        native=page.get_text('text').strip()
        pix=page.get_pixmap(matrix=fitz.Matrix(2.0,2.0),alpha=False)
        image=Image.open(io.BytesIO(pix.tobytes('png')))
        ocr=pytesseract.image_to_string(image).strip()
        chunks=[]
        if native:
            chunks.append(native)
        if ocr and ocr not in native:
            chunks.append(ocr)
        pages.append('=== PAGE %d ===\n%s' % (i,'\n'.join(chunks)))
    text='\n\n'.join(pages)
elif ext=='.docx':
    with zipfile.ZipFile(p) as z:
        names=['word/document.xml']
        names += sorted(n for n in z.namelist() if n.startswith('word/header') and n.endswith('.xml'))
        names += sorted(n for n in z.namelist() if n.startswith('word/footer') and n.endswith('.xml'))
        names += [n for n in ['word/footnotes.xml','word/endnotes.xml','word/comments.xml'] if n in z.namelist()]
        text='\n\n'.join(package_parts(z,names))
elif ext=='.pptx':
    pptx=ensure('pptx','python-pptx')
    presentation=pptx.Presentation(str(p))
    slides=[]
    for i,slide in enumerate(presentation.slides,1):
        chunks=[]
        for shape in slide.shapes:
            if hasattr(shape,'text') and shape.text.strip():
                chunks.append(shape.text.strip())
        try:
            notes=slide.notes_slide
            for shape in notes.shapes:
                if hasattr(shape,'text') and shape.text.strip():
                    t=shape.text.strip()
                    if t not in chunks:
                        chunks.append(t)
        except Exception:
            pass
        slides.append('=== SLIDE %d ===\n%s' % (i,'\n'.join(chunks)))
    with zipfile.ZipFile(p) as z:
        note_parts=sorted(n for n in z.namelist() if n.startswith('ppt/notesSlides/notesSlide') and n.endswith('.xml'))
        notes=package_parts(z,note_parts)
    text='\n\n'.join(slides+notes)
elif ext in ('.xlsx','.xlsm','.xltx','.xltm'):
    openpyxl=ensure('openpyxl','openpyxl')
    wb=openpyxl.load_workbook(str(p),data_only=True,read_only=True)
    sheets=[]
    for ws in wb.worksheets:
        rows=[]
        for row in ws.iter_rows(values_only=True):
            rows.append('\t'.join('' if v is None else str(v) for v in row))
        sheets.append('=== SHEET %s ===\n%s' % (ws.title,'\n'.join(rows)))
    text='\n\n'.join(sheets)
elif ext=='.rtf':
    striprtf=ensure('striprtf','striprtf')
    text=striprtf.rtf_to_text(p.read_text(errors='replace'))
else:
    text=p.read_text(errors='replace')
print(json.dumps({'path':str(p),'extension':ext,'text':text},ensure_ascii=False))"""
      let res = await h.instavmExecute(code, "python")
      if not res.ok:
        return res
      let output = res.payload{"output"}.getStr(res.payload{"stdout"}.getStr("")).strip()
      try:
        let payload = parseJson(output)
        return ToolResult(ok: true, payload: payload, receipt: "document:" & sha1Hex(path & output), message: "document extracted")
      except CatchableError as e:
        return ToolResult(ok: false, payload: %*{"path": path, "raw_output": output}, receipt: "", message: "document extractor returned invalid JSON: " & e.msg))
  registerTool("vm_list_files", "List real files recursively in an InstaVM directory.", %*{"path": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let path = args{"path"}.getStr("/app")
      return await h.instavmExecute("find " & shellQuote(path) & " -printf '%y %p %s\\n'", "bash"))
  registerTool("vm_upload_file", "Upload a file from the authenticated task tenant workspace into the task's InstaVM.", %*{"local_path": "string", "remote_path": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let localRel = args{"local_path"}.getStr("")
      let remotePath = args{"remote_path"}.getStr("")
      if localRel.len == 0 or remotePath.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "local_path and remote_path required")
      var localPath: string
      try:
        localPath = safeJoin(h.tenantId, localRel)
      except CatchableError as e:
        return ToolResult(ok: false, payload: %*{"local_path": localRel}, receipt: "", message: e.msg)
      if not fileExists(localPath):
        return ToolResult(ok: false, payload: %*{"local_path": localRel}, receipt: "", message: "local file not found")
      let maxBytes = positiveEnvInt("VM_TRANSFER_MAX_BYTES", 67_108_864)
      let fileBytes = getFileSize(localPath)
      if fileBytes < 0 or fileBytes > maxBytes:
        return ToolResult(ok: false, payload: %*{"size": fileBytes, "max_bytes": maxBytes}, receipt: "", message: "local file exceeds transfer limit")
      let encoded = base64.encode(readFile(localPath))
      let code = "import base64, pathlib\np=pathlib.Path(" & escapeJson(remotePath) & ")\np.parent.mkdir(parents=True,exist_ok=True)\np.write_bytes(base64.b64decode(" & escapeJson(encoded) & "))\nprint(str(p))"
      return await h.instavmExecute(code, "python"))
  registerTool("vm_download_file", "Download a bounded real InstaVM file into the backend artifact directory.", %*{"remote_path": "string", "name": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let remotePath = args{"remote_path"}.getStr("")
      if remotePath.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "remote_path required")
      let maxBytes = positiveEnvInt("VM_TRANSFER_MAX_BYTES", 67_108_864)
      let checkCode = "import json,pathlib\np=pathlib.Path(" & escapeJson(remotePath) & ")\nprint(json.dumps({'exists':p.is_file(),'size':p.stat().st_size if p.is_file() else -1}))"
      let checkRes = await h.instavmExecute(checkCode, "python")
      if not checkRes.ok:
        return checkRes
      let checkOut = checkRes.payload{"output"}.getStr(checkRes.payload{"stdout"}.getStr("")).strip()
      var meta: JsonNode
      try:
        meta = parseJson(checkOut)
      except CatchableError:
        return ToolResult(ok: false, payload: checkRes.payload, receipt: "", message: "could not inspect remote file")
      let remoteSize = meta{"size"}.getInt(-1)
      if not meta{"exists"}.getBool(false) or remoteSize < 0:
        return ToolResult(ok: false, payload: meta, receipt: "", message: "remote file not found")
      if remoteSize > maxBytes:
        return ToolResult(ok: false, payload: %*{"size": remoteSize, "max_bytes": maxBytes}, receipt: "", message: "remote file exceeds transfer limit")
      let code = "import base64, pathlib\np=pathlib.Path(" & escapeJson(remotePath) & ")\nprint(base64.b64encode(p.read_bytes()).decode('ascii'))"
      let res = await h.instavmExecute(code, "python")
      if not res.ok:
        return res
      let encoded = res.payload{"output"}.getStr(res.payload{"stdout"}.getStr("")).strip()
      if encoded.len > ((maxBytes + 2) div 3) * 4 + 16:
        return ToolResult(ok: false, payload: %*{"encoded_size": encoded.len}, receipt: "", message: "encoded remote file exceeds transfer limit")
      var decoded: string
      try:
        decoded = base64.decode(encoded)
      except CatchableError as e:
        return ToolResult(ok: false, payload: res.payload, receipt: res.receipt, message: "invalid downloaded base64: " & e.msg)
      if decoded.len > maxBytes:
        return ToolResult(ok: false, payload: %*{"size": decoded.len}, receipt: "", message: "decoded remote file exceeds transfer limit")
      let requestedName = args{"name"}.getStr(extractFilename(remotePath))
      if requestedName.len == 0 or extractFilename(requestedName) != requestedName or requestedName.contains("..") or requestedName.contains('/') or requestedName.contains('\\') or requestedName.contains('\0'):
        return ToolResult(ok: false, payload: %*{"name": requestedName}, receipt: "", message: "invalid artifact name")
      let taskDir = absolutePath(WorkspaceRoot / "artifacts" / h.taskId)
      createDir(taskDir)
      let localPath = absolutePath(taskDir / requestedName)
      if not (localPath == taskDir or localPath.startsWith(taskDir & DirSep)):
        return ToolResult(ok: false, payload: %*{"name": requestedName}, receipt: "", message: "artifact path escapes task directory")
      writeFile(localPath, decoded)
      let mime = newMimetypes().getMimetype(splitFile(requestedName).ext)
      let artifactId = h.addArtifact(requestedName, localPath, "file", (if mime.len > 0: mime else: "application/octet-stream"))
      return ToolResult(ok: true, payload: %*{"artifact_id": artifactId, "path": localPath, "name": requestedName, "download_url": "/api/artifacts/" & artifactId}, receipt: "download:" & artifactId, message: "downloaded"))
  registerTool("vm_browser_create", "Create or return the persistent browser session for this task.", %*{},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let bid = await h.browserSession()
      return ToolResult(ok: true, payload: %*{"browser_session_id": bid}, receipt: "browser:" & bid, message: "browser ready"))
  registerTool("vm_browser_navigate", "Navigate the persistent InstaVM browser.", %*{"url": "string", "wait_timeout": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("navigate", args))
  registerTool("vm_browser_click", "Click a selector in the persistent InstaVM browser.", %*{"selector": "string", "force": "bool optional", "timeout": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("click", args))
  registerTool("vm_browser_type", "Type text into a selector in the persistent InstaVM browser.", %*{"selector": "string", "text": "string", "delay": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("type", args))
  registerTool("vm_browser_fill", "Fill a selector in the persistent InstaVM browser.", %*{"selector": "string", "value": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("fill", args))
  registerTool("vm_browser_scroll", "Scroll the persistent InstaVM browser.", %*{"x": "int optional", "y": "int optional", "selector": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("scroll", args))
  registerTool("vm_browser_wait", "Wait for browser load or selector visibility.", %*{"state": "string", "selector": "string optional", "timeout": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("wait", args))
  registerTool("vm_browser_screenshot", "Capture a real screenshot from the persistent InstaVM browser.", %*{"full_page": "bool optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("screenshot", args))
  registerTool("vm_browser_content", "Read the current browser page content.", %*{},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("content", args))
  registerTool("vm_browser_extract", "Extract elements from the current browser page.", %*{"selector": "string", "attributes": "string[] optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.browserAction("extract", args))
  registerTool("vm_desktop_state", "Read the real InstaVM desktop state.", %*{},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.desktopProxy("/state", HttpGet))
  registerTool("vm_desktop_screenshot", "Capture the real InstaVM desktop screenshot.", %*{},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.desktopProxy("/screenshot", HttpGet))
  registerTool("vm_desktop_click", "Click the real InstaVM desktop.", %*{"x": "int", "y": "int", "button": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.desktopProxy("/mouse/click", HttpPost, args))
  registerTool("vm_desktop_type", "Type text in the real InstaVM desktop.", %*{"text": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.desktopProxy("/keyboard/type", HttpPost, args))
  registerTool("vm_desktop_key", "Send a keyboard key action to the real InstaVM desktop.", %*{"key": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.desktopProxy("/keyboard/key", HttpPost, args))
  registerTool("vm_desktop_scroll", "Scroll the real InstaVM desktop.", %*{"x": "int optional", "y": "int optional", "delta_x": "int optional", "delta_y": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      return await h.desktopProxy("/mouse/scroll", HttpPost, args))
  registerTool("vm_pty_create", "Create a persistent InstaVM PTY.", %*{"cols": "int optional", "rows": "int optional", "command": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let sid = await h.createInstaVmSession()
      var body = copy(args)
      if body.kind != JObject: body = newJObject()
      body["cols"] = %(body{"cols"}.getInt(120))
      body["rows"] = %(body{"rows"}.getInt(40))
      let res = await h.instavmJson("/v1/sessions/" & encodeUrl(sid) & "/pty/sessions", HttpPost, body)
      if res.ok:
        let ptyId = res.payload{"session_id"}.getStr(res.payload{"pty_id"}.getStr(res.payload{"id"}.getStr("")))
        if ptyId.len > 0:
          h.saveRuntimeField("pty_id", ptyId)
        let wsUrl = res.payload{"ws_url"}.getStr(res.payload{"websocket_url"}.getStr(""))
        if wsUrl.len > 0:
          h.saveRuntimeField("pty_ws_url", wsUrl)
      return res)
  registerTool("vm_pty_write", "Write stdin to the persistent InstaVM PTY and return the resulting terminal output.", %*{"input": "string", "read_seconds": "float optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rt = h.runtimeNode()
      let sid = rt{"instavm_session_id"}.getStr("")
      let ptyId = rt{"pty_id"}.getStr("")
      if sid.len == 0 or ptyId.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "PTY not created")
      var wsUrl = rt{"pty_ws_url"}.getStr("")
      if wsUrl.len == 0:
        let info = await h.instavmJson("/v1/sessions/" & encodeUrl(sid) & "/pty/sessions/" & encodeUrl(ptyId), HttpGet)
        if info.ok:
          wsUrl = info.payload{"ws_url"}.getStr(info.payload{"websocket_url"}.getStr(""))
          if wsUrl.len > 0:
            h.saveRuntimeField("pty_ws_url", wsUrl)
      if wsUrl.len == 0:
        return ToolResult(ok: false, payload: %*{"session_id": sid, "pty_id": ptyId}, receipt: "", message: "PTY websocket URL is not present in the PTY session response")
      let stdinText = args{"input"}.getStr("")
      let readSeconds = max(0.05, args{"read_seconds"}.getFloat(0.35))
      let helper = """import base64,hashlib,json,os,select,socket,ssl,struct,sys,time
from urllib.parse import urlsplit
def recv_exact(sock,n):
 out=b''
 while len(out)<n:
  chunk=sock.recv(n-len(out))
  if not chunk: raise EOFError('websocket closed')
  out+=chunk
 return out
def recv_frame(sock):
 h=recv_exact(sock,2)
 opcode=h[0]&15
 masked=(h[1]&128)!=0
 n=h[1]&127
 if n==126:n=struct.unpack('!H',recv_exact(sock,2))[0]
 elif n==127:n=struct.unpack('!Q',recv_exact(sock,8))[0]
 mask=recv_exact(sock,4) if masked else b''
 data=recv_exact(sock,n) if n else b''
 if masked:data=bytes(b^mask[i%4] for i,b in enumerate(data))
 return opcode,data
def send_frame(sock,data,opcode=2):
 first=128|opcode
 n=len(data)
 if n<126:head=bytes([first,128|n])
 elif n<65536:head=bytes([first,128|126])+struct.pack('!H',n)
 else:head=bytes([first,128|127])+struct.pack('!Q',n)
 mask=os.urandom(4)
 body=bytes(b^mask[i%4] for i,b in enumerate(data))
 sock.sendall(head+mask+body)
u=urlsplit(sys.argv[1])
host=u.hostname
port=u.port or (443 if u.scheme=='wss' else 80)
path=u.path or '/'
if u.query:path+='?'+u.query
sock=socket.create_connection((host,port),timeout=20)
if u.scheme=='wss':sock=ssl.create_default_context().wrap_socket(sock,server_hostname=host)
wskey=base64.b64encode(os.urandom(16)).decode()
host_header=host if port in (80,443) else host+':'+str(port)
request=('GET '+path+' HTTP/1.1\r\nHost: '+host_header+'\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: '+wskey+'\r\nSec-WebSocket-Version: 13\r\nX-API-Key: '+sys.argv[2]+'\r\n\r\n').encode()
sock.sendall(request)
head=b''
while not head.endswith(b'\r\n\r\n'):
 b=sock.recv(1)
 if not b:raise RuntimeError('websocket handshake closed')
 head+=b
 if len(head)>65536:raise RuntimeError('websocket handshake too large')
lines=head.decode('latin1').split('\r\n')
if ' 101 ' not in lines[0]:raise RuntimeError('websocket handshake failed: '+lines[0])
headers={}
for line in lines[1:]:
 if ':' in line:
  k,v=line.split(':',1);headers[k.strip().lower()]=v.strip()
expected=base64.b64encode(hashlib.sha1((wskey+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
if headers.get('sec-websocket-accept')!=expected:raise RuntimeError('invalid websocket accept')
data=base64.b64decode(sys.argv[3])
wait=float(sys.argv[4])
send_frame(sock,data,2)
chunks=[]
deadline=time.monotonic()+wait
while True:
 remaining=deadline-time.monotonic()
 if remaining<=0:break
 ready,_,_=select.select([sock],[],[],remaining)
 if not ready:break
 opcode,payload=recv_frame(sock)
 if opcode==8:break
 if opcode==9:
  send_frame(sock,payload,10);continue
 if opcode==2:chunks.append(payload);continue
 if opcode==1:
  try:
   event=json.loads(payload.decode())
   if event.get('type')=='exit':break
  except Exception:chunks.append(payload)
try:send_frame(sock,b'',8)
except Exception:pass
sock.close()
print(base64.b64encode(b''.join(chunks)).decode())"""
      let command = "python3 -c " & shellQuote(helper) & " " & shellQuote(wsUrl) & " " & shellQuote(requireEnv("INSTAVM_API_KEY")) & " " & shellQuote(base64.encode(stdinText)) & " " & shellQuote($readSeconds)
      let local = execCmdEx(command)
      if local.exitCode != 0:
        return ToolResult(ok: false, payload: %*{"output": local.output}, receipt: "pty:error", message: local.output)
      var decoded = ""
      try:
        decoded = base64.decode(local.output.strip())
      except CatchableError as e:
        return ToolResult(ok: false, payload: %*{"output": local.output}, receipt: "pty:error", message: e.msg)
      return ToolResult(ok: true, payload: %*{"session_id": sid, "pty_id": ptyId, "output": decoded}, receipt: "pty:" & sha1Hex(stdinText & decoded), message: "PTY input written"))
  registerTool("vm_pty_resize", "Resize the persistent InstaVM PTY.", %*{"cols": "int", "rows": "int"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rt = h.runtimeNode()
      let sid = rt{"instavm_session_id"}.getStr("")
      let ptyId = rt{"pty_id"}.getStr("")
      if sid.len == 0 or ptyId.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "PTY not created")
      return await h.instavmJson("/v1/sessions/" & encodeUrl(sid) & "/pty/sessions/" & encodeUrl(ptyId) & "/resize", HttpPost, args))
  registerTool("vm_pty_close", "Close the persistent InstaVM PTY.", %*{},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rt = h.runtimeNode()
      let sid = rt{"instavm_session_id"}.getStr("")
      let ptyId = rt{"pty_id"}.getStr("")
      if sid.len == 0 or ptyId.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "PTY not created")
      let res = await h.instavmJson("/v1/sessions/" & encodeUrl(sid) & "/pty/sessions/" & encodeUrl(ptyId), HttpDelete)
      if res.ok:
        h.saveRuntimeField("pty_id", "")
        h.saveRuntimeField("pty_ws_url", "")
      return res)
  registerTool("memory_search", "Search persisted operational knowledge and skills using hybrid semantic, lexical and learned-policy ranking.", %*{"query": "string", "limit": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let q = args{"query"}.getStr("").strip()
      let limit = max(1, args{"limit"}.getInt(8))
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      var skills = newJArray()
      for r in searchSkills(tenant, q, limit):
        skills.add(%*{"skill_id": r.getStr("skill_id"), "name": r.getStr("name"), "domain": r.getStr("domain"), "trigger": r.getStr("trigger_spec"), "procedure": r.getStr("procedure_spec"), "skill_code": r.getStr("skill_code"), "reward": r.getFloat("reward")})
      var docs = newJArray()
      for r in searchKnowledge(tenant, q, limit):
        docs.add(%*{"doc_id": r.getStr("doc_id"), "slug": r.getStr("slug"), "category": r.getStr("category"), "content": r.getStr("content")})
      let policy = learnedPolicySignals(tenant, q, limit)
      return ToolResult(ok: true, payload: %*{"skills": skills, "knowledge": docs, "policy_signals": policy}, receipt: "memory:" & sha1Hex(tenant & ":" & q), message: "retrieved"))
  registerTool("memory_write", "Persist operational knowledge in the current tenant's injective knowledge repository.", %*{"slug": "string", "category": "string optional", "body": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let slug = args{"slug"}.getStr("").strip()
      let body = args{"body"}.getStr("")
      if slug.len == 0 or body.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "slug and body required")
      let category = args{"category"}.getStr("operational")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      try:
        let docId = commitKnowledgeDoc(tenant, slug, category, body)
        let rows = store.query("SELECT path FROM knowledge_docs WHERE doc_id=? AND tenant_id=?", @[%docId, %tenant])
        let path = if rows.len > 0: rows[0].getStr("path") else: ""
        return ToolResult(ok: true, payload: %*{"doc_id": docId, "slug": slug, "path": path}, receipt: "knowledge:" & docId, message: "persisted")
      except CatchableError as e:
        return ToolResult(ok: false, payload: %*{"slug": slug}, receipt: "", message: e.msg))
proc registerLegacyTools() =
  registerTool("write_file", "Atomically write content to a backend workspace file.", %*{"path": "string", "content": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rel = args{"path"}.getStr("")
      let content = args{"content"}.getStr("")
      if rel.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let full = safeJoin(tenant, rel)
      atomicWrite(full, content)
      return ToolResult(ok: true, payload: %*{"path": rel, "bytes_written": content.len, "sha1": sha1Hex(content)}, receipt: "write:" & rel & ":" & sha1Hex(content), message: "wrote " & $content.len & " bytes"))

  registerTool("read_file", "Read a backend workspace file.", %*{"path": "string", "start_line": "int optional", "end_line": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rel = args{"path"}.getStr("")
      if rel.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let full = safeJoin(tenant, rel)
      if not fileExists(full):
        return ToolResult(ok: false, payload: %*{"path": rel}, receipt: "", message: "file not found")
      let lines = readLinesOf(full)
      let startLine = max(1, args{"start_line"}.getInt(1))
      let requestedEnd = args{"end_line"}.getInt(0)
      let endLine = if requestedEnd > 0: min(requestedEnd, lines.len) else: lines.len
      let startIndex = min(lines.len, startLine - 1)
      let content = if startIndex < endLine: lines[startIndex ..< endLine].join("\n") else: ""
      return ToolResult(ok: true, payload: %*{"path": rel, "content": content, "total_lines": lines.len, "bytes_read": content.len, "sha1": sha1Hex(content)}, receipt: "read:" & rel & ":" & $lines.len, message: "read " & $content.len & " bytes"))

  registerTool("append_file", "Atomically append text to a backend workspace file.", %*{"path": "string", "lines": "string[]", "content": "string optional", "unique": "bool optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rel = args{"path"}.getStr("")
      if rel.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      var incoming: seq[string] = @[]
      if args.hasKey("lines") and args["lines"].kind == JArray:
        for item in args["lines"].elems:
          incoming.add(item.getStr(""))
      elif args{"content"}.getStr("").len > 0:
        incoming = args{"content"}.getStr("").splitLines()
      if incoming.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "lines or content required")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let full = safeJoin(tenant, rel)
      var existing = readLinesOf(full)
      let unique = args{"unique"}.getBool(true)
      var seen = initHashSet[string]()
      if unique:
        for line in existing:
          seen.incl(line)
      var added = 0
      var skipped = 0
      for line in incoming:
        if unique and line in seen:
          inc skipped
        else:
          existing.add(line)
          seen.incl(line)
          inc added
      let finalContent = existing.join("\n") & (if existing.len > 0: "\n" else: "")
      atomicWrite(full, finalContent)
      return ToolResult(ok: true, payload: %*{"path": rel, "added_count": added, "skipped_count": skipped, "total_lines": existing.len, "sha1": sha1Hex(finalContent)}, receipt: "append:" & rel & ":" & $added, message: "appended " & $added & " lines"))

  registerTool("replace_lines", "Replace a 1-based inclusive line range in a backend workspace file.", %*{"path": "string", "start_line": "int", "end_line": "int", "lines": "string[]", "content": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rel = args{"path"}.getStr("")
      let startLine = args{"start_line"}.getInt(0)
      let endLine = args{"end_line"}.getInt(0)
      if rel.len == 0 or startLine < 1 or endLine < startLine - 1:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "invalid path or line range")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let full = safeJoin(tenant, rel)
      var lines = readLinesOf(full)
      if startLine > lines.len + 1:
        return ToolResult(ok: false, payload: %*{"path": rel}, receipt: "", message: "start_line beyond EOF")
      var replacements: seq[string] = @[]
      if args.hasKey("lines") and args["lines"].kind == JArray:
        for item in args["lines"].elems:
          replacements.add(item.getStr(""))
      elif args{"content"}.getStr("").len > 0:
        replacements = args{"content"}.getStr("").splitLines()
      let first = startLine - 1
      let after = min(endLine, lines.len)
      var nextLines: seq[string] = @[]
      for i in 0 ..< first:
        nextLines.add(lines[i])
      for line in replacements:
        nextLines.add(line)
      for i in after ..< lines.len:
        nextLines.add(lines[i])
      let finalContent = nextLines.join("\n") & (if nextLines.len > 0: "\n" else: "")
      atomicWrite(full, finalContent)
      return ToolResult(ok: true, payload: %*{"path": rel, "removed_count": max(0, after - first), "inserted_count": replacements.len, "total_lines": nextLines.len, "sha1": sha1Hex(finalContent)}, receipt: "replace:" & rel & ":" & $startLine & "-" & $endLine, message: "replaced requested line range"))

  registerTool("check_lines", "Check exact line presence in a backend workspace file.", %*{"path": "string", "lines": "string[]"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rel = args{"path"}.getStr("")
      if rel.len == 0 or not args.hasKey("lines") or args["lines"].kind != JArray:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path and lines are required")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let full = safeJoin(tenant, rel)
      let lines = readLinesOf(full)
      var firstLine = initTable[string, int]()
      for i, line in lines:
        if not firstLine.hasKey(line):
          firstLine[line] = i + 1
      var results = newJArray()
      var missing = 0
      for probe in args["lines"].elems:
        let text = probe.getStr("")
        let present = firstLine.hasKey(text)
        if not present:
          inc missing
        results.add(%*{"line": text, "present": present, "line_number": (if present: firstLine[text] else: 0)})
      return ToolResult(ok: true, payload: %*{"path": rel, "results": results, "missing_count": missing}, receipt: "check:" & rel & ":" & $results.len, message: $(results.len - missing) & "/" & $results.len & " lines present"))

  registerTool("search_files", "Search backend workspace files recursively for a substring.", %*{"pattern": "string", "path": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let pattern = args{"pattern"}.getStr("")
      if pattern.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "pattern required")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let base = safeJoin(tenant, ".")
      let root = safeJoin(tenant, args{"path"}.getStr("."))
      if not dirExists(root):
        return ToolResult(ok: false, payload: %*{"path": args{"path"}.getStr(".")}, receipt: "", message: "directory not found")
      let needle = pattern.toLowerAscii()
      var hits = newJArray()
      var scanned = 0
      for path in walkDirRec(root):
        if not fileExists(path):
          continue
        inc scanned
        try:
          let content = readFile(path)
          var lineNo = 0
          for line in content.splitLines():
            inc lineNo
            if needle in line.toLowerAscii():
              hits.add(%*{"path": relativePath(path, base).replace('\\', '/'), "line": lineNo, "text": line})
        except CatchableError:
          discard
      return ToolResult(ok: true, payload: %*{"pattern": pattern, "hits": hits, "scanned_files": scanned}, receipt: "search:" & sha1Hex(pattern & ":" & $scanned), message: "found " & $hits.len & " matches"))

  registerTool("list_dir", "List entries in a backend workspace directory.", %*{"path": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let rel = args{"path"}.getStr(".")
      let target = safeJoin(tenant, rel)
      if not dirExists(target):
        return ToolResult(ok: false, payload: %*{"path": rel}, receipt: "", message: "directory not found")
      var entries = newJArray()
      for kind, path in walkDir(target):
        let name = extractFilename(path)
        var size = 0'i64
        if kind == pcFile:
          try:
            size = getFileSize(path)
          except CatchableError:
            size = 0
        entries.add(%*{"name": name, "type": (if kind in {pcDir, pcLinkToDir}: "dir" else: "file"), "bytes": size})
      return ToolResult(ok: true, payload: %*{"path": rel, "entries": entries}, receipt: "list:" & rel, message: "listed " & $entries.len & " entries"))

  registerTool("delete_file", "Delete a backend workspace file.", %*{"path": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let rel = args{"path"}.getStr("")
      if rel.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "path required")
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      let full = safeJoin(tenant, rel)
      if not fileExists(full):
        return ToolResult(ok: false, payload: %*{"path": rel}, receipt: "", message: "file not found")
      removeFile(full)
      return ToolResult(ok: true, payload: %*{"path": rel, "deleted": true}, receipt: "delete:" & rel, message: "deleted"))

  registerTool("math_eval", "Evaluate a mathematical expression with the deterministic recursive-descent parser.", %*{"expression": "string"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let expression = args{"expression"}.getStr("")
      if expression.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "expression required")
      let evaluated = evalMathExpression(expression)
      if not evaluated[0]:
        return ToolResult(ok: false, payload: %*{"expression": expression}, receipt: "", message: evaluated[2])
      return ToolResult(ok: true, payload: %*{"expression": expression, "value": evaluated[1]}, receipt: "math:" & sha1Hex(expression), message: "= " & formatFloat(evaluated[1], ffDefault, 16)))

  registerTool("http_fetch", "Perform a real outbound HTTP request and return the response.", %*{"url": "string", "method": "string optional", "headers": "object optional", "body": "string optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let url = args{"url"}.getStr("")
      if url.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "url required")
      let parsed = validateOutboundUrl(url)
      discard parsed
      let httpMethodValue = mapHttpMethod(args{"method"}.getStr("GET"))
      let requestBody = args{"body"}.getStr("")
      var headers = newHttpHeaders()
      if args.hasKey("headers") and args["headers"].kind == JObject:
        for key, value in args["headers"].fields:
          headers[key] = value.getStr("")
      try:
        let response = await httpRequestAsync(url, httpMethodValue, requestBody, headers)
        let body = response[1]
        return ToolResult(ok: response[0] < 400, payload: %*{"url": url, "status": response[0], "headers": responseHeadersJson(response[2]), "body": body}, receipt: "http:" & sha1Hex(url & ":" & $response[0] & ":" & body), message: "status " & $response[0])
      except CatchableError as e:
        return ToolResult(ok: false, payload: %*{"url": url}, receipt: "", message: e.msg))

proc initTools() =
  registerTools()
  registerLegacyTools()

proc registerReasonTool() =
  registerTool("reason", "Run a recursive specialist reasoning pass and return its structured result.", %*{"goal": "string", "context": "string optional", "depth": "int optional", "max_depth": "int optional", "branches": "int optional"},
    proc(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
      let goal = args{"goal"}.getStr(h.sigma{"goal"}.getStr(h.title))
      if goal.len == 0:
        return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "goal required")
      let context = args{"context"}.getStr(canonical(h.obs))
      let depth = max(0, args{"depth"}.getInt(0))
      let maxDepth = max(depth + 1, args{"max_depth"}.getInt(depth + 1))
      let branches = max(1, args{"branches"}.getInt(1))
      let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
      try:
        let resultNode = await recursiveReasonCall(tenant, h.taskId, goal, context, depth, maxDepth, branches)
        return ToolResult(ok: true, payload: resultNode, receipt: "reason:" & sha1Hex(goal & ":" & canonical(resultNode)), message: "reasoning completed")
      except CatchableError as e:
        return ToolResult(ok: false, payload: %*{"goal": goal}, receipt: "", message: e.msg))

proc specialistSystem(role: ModelRole): string =
  case role
  of mrGpt6Astra: promptText("gpt6_astra")
  of mrGlm52: promptText("glm52")
  of mrGemini38: promptText("gemini38")
  of mrMiniMaxM3: promptText("minimax_m3")
  of mrGrok43: promptText("grok43")
  of mrOrchestrator: promptText("orchestrator_router")

proc subAgentSystem(role: ModelRole): string =
  let core = promptText("subagent_core")
  if role == mrOrchestrator:
    return core & "\n\n" & promptText("subagent_orchestrator")
  core & "\n\nMODEL SPECIALIZATION:\n" & specialistSystem(role)

proc buildStepMessages(h: TaskHandle, step: JsonNode, role: ModelRole): JsonNode =
  acquire(h.lock)
  let sigma = copy(h.sigma)
  let obs = copy(h.obs)
  let spec = copy(h.spec)
  release(h.lock)
  let queryText = step{"goal"}.getStr("") & " " & sigma{"step_summary"}.getStr("") & " " & canonical(obs)
  let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
  var memory = %*{"skills": newJArray(), "knowledge": newJArray(), "policy_signals": learnedPolicySignals(tenant, queryText, 12)}
  for r in searchSkills(tenant, queryText, 8):
    memory["skills"].add(%*{"name": r.getStr("name"), "domain": r.getStr("domain"), "procedure": r.getStr("procedure_spec"), "skill_code": r.getStr("skill_code"), "reward": r.getFloat("reward")})
  for r in searchKnowledge(tenant, queryText, 8):
    memory["knowledge"].add(%*{"slug": r.getStr("slug"), "category": r.getStr("category"), "content": r.getStr("content")})
  let userText = "TASK SPECIFICATION:\n" & canonical(spec) &
    "\n\nCURRENT AUTONOMOUS STATE:\n" & canonical(sigma) &
    "\n\nACTIVE PLAN STEP:\n" & canonical(step) &
    "\n\nLATEST REAL OBSERVATION:\n" & canonical(obs) &
    "\n\nRELEVANT LEARNED MEMORY:\n" & canonical(memory) &
    "\n\nACTIVE SUBAGENT TREE:\n" & canonical(subAgentTreeJson(h.taskId)) &
    "\n\nAVAILABLE REAL TOOLS:\n" & canonical(toolCatalog()) &
    "\n\nVALID SUBAGENT MODEL ROLE IDS:\n" & configuredSubAgentModelRoleNames().join(", ")
  result = %*[
    {"role": "system", "content": specialistSystem(role)},
    {"role": "user", "content": userText}
  ]
  if role in {mrGemini38, mrGrok43} and spec.hasKey("messages") and spec["messages"].kind == JArray:
    for original in spec["messages"].elems:
      if original.kind == JObject:
        let content = original{"content"}
        if content.kind == JArray:
          var hasMedia = false
          for part in content.elems:
            if part.kind == JObject and part{"type"}.getStr("") in ["image", "image_url", "input_image", "video", "video_url", "input_video"]:
              hasMedia = true
              break
          if hasMedia:
            result.add(copy(original))


proc descriptorAccepts(value: JsonNode, descriptor: string): bool =
  let d = descriptor.strip().toLowerAscii()
  if d.len == 0:
    return true
  let base = d.replace(" optional", "").strip()
  if base == "string": return value.kind == JString
  if base == "int" or base == "integer": return value.kind == JInt
  if base == "number" or base == "float": return value.kind in {JInt, JFloat}
  if base == "bool" or base == "boolean": return value.kind == JBool
  if base == "object": return value.kind == JObject
  if base == "array": return value.kind == JArray
  if base == "string[]":
    if value.kind != JArray: return false
    for item in value.elems:
      if item.kind != JString: return false
    return true
  if '|' in base:
    let choices = base.split('|').mapIt(it.strip())
    if value.kind == JString:
      return value.getStr("") in choices
    return false
  true

proc validateToolArguments(spec: ToolSpec, args: JsonNode): string =
  if args.isNil or args.kind != JObject:
    return "tool arguments must be an object"
  if spec.schema.isNil or spec.schema.kind != JObject:
    return ""
  for key, descriptorNode in spec.schema.fields:
    if descriptorNode.kind != JString:
      continue
    let descriptor = descriptorNode.getStr("")
    let optional = descriptor.toLowerAscii().contains("optional")
    if not args.hasKey(key):
      if not optional:
        return "missing required tool argument: " & key
      continue
    if not descriptorAccepts(args[key], descriptor):
      return "invalid tool argument type or value for " & key & ": expected " & descriptor
  for key, _ in args.fields:
    if key.startsWith("_actor_"):
      continue
    if not spec.schema.hasKey(key):
      return "unknown tool argument: " & key
  ""

proc executeToolAction(h: TaskHandle, action: JsonNode): Future[ToolResult] {.async.} =
  if action.isNil or action.kind != JObject:
    return ToolResult(ok: true, payload: newJObject(), receipt: "none", message: "no action")
  let name = action{"tool"}.getStr("")
  if name.len == 0 or name == "none":
    return ToolResult(ok: true, payload: newJObject(), receipt: "none", message: "no action")
  if not toolRegistry.hasKey(name):
    return ToolResult(ok: false, payload: %*{"tool": name}, receipt: "unknown", message: "unknown tool: " & name)
  if name notin h.allowedTools:
    return ToolResult(ok: false, payload: %*{"tool": name}, receipt: "forbidden", message: "tool is not allowed for this tenant/task: " & name)
  var args = action{"args"}
  if args.isNil:
    args = newJObject()
  if args.kind != JObject:
    return ToolResult(ok: false, payload: %*{"tool": name}, receipt: "invalid_args", message: "tool args must be an object")
  let argumentError = validateToolArguments(toolRegistry[name], args)
  if argumentError.len > 0:
    return ToolResult(ok: false, payload: %*{"tool": name, "args": args}, receipt: "invalid_args", message: argumentError)
  h.emit(%*{"type": "tool_start", "tool": name, "args": args})
  let started = getMonoTime()
  try:
    result = await toolRegistry[name].handler(h, args)
  except CatchableError as e:
    result = ToolResult(ok: false, payload: %*{"error": e.msg}, receipt: "error", message: e.msg)
  let elapsed = int((getMonoTime() - started).inMilliseconds)
  h.emit(%*{"type": "tool_result", "tool": name, "ok": result.ok, "payload": result.payload, "message": result.message, "latency_ms": elapsed})

proc verifyStepCompletion(h: TaskHandle, step, modelNode: JsonNode, toolRes: ToolResult, role: ModelRole): Future[(bool, JsonNode)] {.async.} =
  var evidence = %*{
    "step": copy(step),
    "model_role": modelRoleName(role),
    "model_output": copy(modelNode),
    "tool": {"ok": toolRes.ok, "payload": copy(toolRes.payload), "receipt": toolRes.receipt, "message": toolRes.message}
  }
  if not toolRes.ok:
    return (false, %*{"verified": false, "reason": "tool execution failed", "evidence": evidence})
  let systemText = promptText("step_verifier")
  let userText = "Verify whether this exact plan step is complete from concrete evidence only. Do not infer success from the specialist claiming success.\n\n" & canonical(evidence)
  let resp = await cerebrasCall(%*[{"role": "system", "content": systemText}, {"role": "user", "content": userText}], true)
  if resp.totalTokens > 0 and not chargeTokens(h.tenantId, h.taskId, resp.totalTokens):
    h.haltForBudget()
    return (false, %*{"verified": false, "reason": "token budget exhausted"})
  let node = parseJsonObjectLoose(resp.content)
  if node.isNil or node{"verified"}.kind != JBool:
    return (false, %*{"verified": false, "reason": "step verifier returned invalid structured output", "raw": resp.content})
  let verified = node{"verified"}.getBool(false)
  return (verified, node)

proc modelStep(h: TaskHandle, step: JsonNode): Future[(bool, string)] {.async.} =
  let role = parseModelRole(step{"model"}.getStr(""))
  if role == mrOrchestrator:
    raise newException(ValueError, "orchestrator cannot execute a specialist plan step")
  let stepId = step{"id"}.getStr("step")
  acquire(h.lock)
  let preState = copy(h.sigma)
  release(h.lock)
  h.setStepStatus(stepId, "running")
  h.transitionOrchestrator(osAct)
  h.emit(%*{"type": "model_start", "model": modelRoleName(role), "provider": providerName(modelSpec(role).provider), "step_id": stepId})
  let messages = h.buildStepMessages(step, role)
  let modelStarted = getMonoTime()
  let resp = await invokeModel(role, messages, role != mrGemini38, h.tenantId, h.taskId)
  let modelLatency = int((getMonoTime() - modelStarted).inMilliseconds)
  h.recordModelContext(role, "step_output", resp.content, %*{"step_id": stepId, "provider": providerName(resp.provider), "model": resp.model, "finish_reason": resp.finishReason})
  h.emit(%*{"type": "model_output", "model": modelRoleName(role), "step_id": stepId, "content": resp.content, "latency_ms": modelLatency})
  let node = parseJsonObjectLoose(resp.content)
  if node.isNil:
    acquire(h.lock)
    h.obs = %*{"status": "model_output_invalid", "step_id": stepId, "model": modelRoleName(role), "content": resp.content}
    h.sigma["step_summary"] = %"specialist output was not a usable structured object"
    let postState = copy(h.sigma)
    let observation = copy(h.obs)
    release(h.lock)
    let receipt = %*{"model": resp.model, "provider": providerName(resp.provider), "finish_reason": resp.finishReason}
    h.logRawTrace("", preState, newJObject(), observation, newJObject(), postState, receipt, false, modelLatency)
    h.transitionOrchestrator(osReflect)
    h.persistReflection("model_output_invalid", "ask the orchestrator to decide the next specialist or retry strategy from the exact recorded output", modelRoleName(role), %*{"step_id": stepId, "raw_output": resp.content}, observation)
    h.setStepStatus(stepId, "needs_replan")
    h.persistTask()
    return (false, "")
  var action: JsonNode = newJNull()
  var summary = node{"summary"}.getStr("")
  var finalText = node{"final"}.getStr("")
  var requestedComplete = node{"step_complete"}.getBool(false)
  if role == mrGemini38 and node.hasKey("images"):
    if node["images"].kind != JArray or node["images"].elems.len == 0:
      h.transitionOrchestrator(osReflect)
      h.setStepStatus(stepId, "needs_replan")
      acquire(h.lock)
      h.obs = %*{"status": "model_output_invalid", "step_id": stepId, "error": "Gemini images output must be a non-empty array"}
      release(h.lock)
      h.persistTask()
      return (false, "")
    finalText = canonical(node)
    summary = "Gemini visual analysis returned structured image specifications"
    requestedComplete = true
  else:
    if node.hasKey("action"):
      action = node["action"]
    if node{"step_complete"}.kind != JBool:
      h.transitionOrchestrator(osReflect)
      h.setStepStatus(stepId, "needs_replan")
      acquire(h.lock)
      h.obs = %*{"status": "model_output_invalid", "step_id": stepId, "error": "step_complete boolean required"}
      release(h.lock)
      h.persistTask()
      return (false, finalText)
  var toolRes = ToolResult(ok: true, payload: newJObject(), receipt: "none", message: "no action")
  var totalLatency = modelLatency
  if not action.isNil and action.kind == JObject and action{"tool"}.getStr("").len > 0:
    let toolStarted = getMonoTime()
    toolRes = await h.executeToolAction(action)
    totalLatency += int((getMonoTime() - toolStarted).inMilliseconds)
    acquire(h.lock)
    h.obs = %*{
      "status": (if toolRes.ok: "tool_completed" else: "tool_failed"),
      "step_id": stepId,
      "model": modelRoleName(role),
      "action": copy(action),
      "tool_result": copy(toolRes.payload),
      "message": toolRes.message,
      "receipt": toolRes.receipt
    }
    h.sigma["step_summary"] = %(if summary.len > 0: summary else: toolRes.message)
    let postState = copy(h.sigma)
    let observation = copy(h.obs)
    release(h.lock)
    let receipt = %*{"receipt": toolRes.receipt, "ok": toolRes.ok, "model": resp.model, "provider": providerName(resp.provider)}
    h.logRawTrace("", preState, action, observation, %*{"summary": summary}, postState, receipt, toolRes.ok, totalLatency)
    h.checkpoint(action, receipt)
    h.persistTask()
    if not toolRes.ok:
      h.transitionOrchestrator(osReflect)
      h.persistReflection("tool_failure:" & action{"tool"}.getStr(""), "return the exact tool failure to the strategic orchestrator for a fresh model decision", modelRoleName(role), %*{"step_id": stepId, "action": action, "result": toolRes.payload}, observation)
      h.setStepStatus(stepId, "needs_replan")
      h.persistTask()
      return (false, finalText)
  else:
    acquire(h.lock)
    h.obs = %*{"status": "model_completed", "step_id": stepId, "model": modelRoleName(role), "summary": summary, "content": finalText}
    h.sigma["step_summary"] = %(if summary.len > 0: summary else: finalText)
    let postState = copy(h.sigma)
    let observation = copy(h.obs)
    release(h.lock)
    let receipt = %*{"model": resp.model, "provider": providerName(resp.provider), "finish_reason": resp.finishReason}
    h.logRawTrace("", preState, newJObject(), observation, %*{"summary": summary}, postState, receipt, requestedComplete, modelLatency)
    h.checkpoint(newJObject(), receipt)
    h.persistTask()
  if requestedComplete:
    let verified = await h.verifyStepCompletion(step, node, toolRes, role)
    acquire(h.lock)
    h.obs["step_verification"] = copy(verified[1])
    release(h.lock)
    if verified[0]:
      h.setStepStatus(stepId, "completed")
      h.updateProgress()
      h.emit(%*{"type": "progress", "step_id": stepId, "status": "completed", "verification": verified[1]})
      h.persistTask()
      return (true, finalText)
    h.transitionOrchestrator(osReflect)
    h.setStepStatus(stepId, "needs_replan")
    h.persistReflection("step_verification_failed", "replan from concrete verification evidence", modelRoleName(role), %*{"step_id": stepId, "output": node}, verified[1])
    h.persistTask()
  return (false, finalText)

proc completionCheck(h: TaskHandle): Future[(bool, string)]

proc verifyTerminal(h: TaskHandle): (bool, JsonNode) =
  acquire(h.lock)
  let sigma = copy(h.sigma)
  let spec = copy(h.spec)
  let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
  release(h.lock)
  var report = newJArray()
  var allOk = true
  var configured = false
  if spec.hasKey("verifiers") and spec["verifiers"].kind == JArray:
    for verifier in spec["verifiers"].elems:
      if verifier.kind != JObject:
        allOk = false
        report.add(%*{"verifier": "invalid", "ok": false, "error": "verifier must be an object"})
        continue
      configured = true
      let kind = verifier{"type"}.getStr(verifier{"verifier"}.getStr(""))
      case kind
      of "state_path_equals":
        let path = verifier{"path"}.getStr("")
        let actual = jsonPointerGet(sigma, path)
        let expected = if verifier.hasKey("expected"): verifier["expected"] elif verifier.hasKey("value"): verifier["value"] else: newJNull()
        let ok = not actual.isNil and jsonEquivalent(actual, expected)
        if not ok: allOk = false
        report.add(%*{"verifier": kind, "path": path, "ok": ok, "expected": expected, "actual": (if actual.isNil: newJNull() else: copy(actual))})
      of "file_exists":
        let rel = verifier{"path"}.getStr("")
        var ok = false
        try:
          ok = rel.len > 0 and fileExists(safeJoin(tenant, rel))
        except CatchableError:
          ok = false
        if not ok: allOk = false
        report.add(%*{"verifier": kind, "path": rel, "ok": ok})
      of "file_contains":
        let rel = verifier{"path"}.getStr("")
        let needle = verifier{"needle"}.getStr(verifier{"text"}.getStr(""))
        var ok = false
        try:
          let full = safeJoin(tenant, rel)
          ok = fileExists(full) and needle.len > 0 and needle in readFile(full)
        except CatchableError:
          ok = false
        if not ok: allOk = false
        report.add(%*{"verifier": kind, "path": rel, "needle": needle, "ok": ok})
      of "all_subgoals_resolved":
        var unresolved = 0
        if sigma.hasKey("subgoals") and sigma["subgoals"].kind == JArray:
          for item in sigma["subgoals"].elems:
            if item.kind == JObject and item{"status"}.getStr("open") in ["open", "in_progress", "queued", "running"]:
              inc unresolved
        let ok = unresolved == 0
        if not ok: allOk = false
        report.add(%*{"verifier": kind, "ok": ok, "unresolved": unresolved})
      of "no_blockers":
        let count = if sigma.hasKey("blockers") and sigma["blockers"].kind == JArray: sigma["blockers"].elems.len else: 0
        let ok = count == 0
        if not ok: allOk = false
        report.add(%*{"verifier": kind, "ok": ok, "blockers_count": count})
      else:
        allOk = false
        report.add(%*{"verifier": kind, "ok": false, "error": "unknown verifier"})
  if not configured:
    let progress = sigma{"progress"}.getFloat(0.0)
    let progressOk = progress >= 0.999
    if not progressOk: allOk = false
    report.add(%*{"verifier": "state_path_equals", "path": "/progress", "ok": progressOk, "expected": 1.0, "actual": progress})
    var unresolved = 0
    if sigma.hasKey("subgoals") and sigma["subgoals"].kind == JArray:
      for item in sigma["subgoals"].elems:
        if item.kind == JObject and item{"status"}.getStr("open") in ["open", "in_progress", "queued", "running"]:
          inc unresolved
    let subgoalOk = unresolved == 0
    if not subgoalOk: allOk = false
    report.add(%*{"verifier": "all_subgoals_resolved", "ok": subgoalOk, "unresolved": unresolved})
    let blockers = if sigma.hasKey("blockers") and sigma["blockers"].kind == JArray: sigma["blockers"].elems.len else: 0
    let blockerOk = blockers == 0
    if not blockerOk: allOk = false
    report.add(%*{"verifier": "no_blockers", "ok": blockerOk, "blockers_count": blockers})
  if spec.hasKey("required_files") and spec["required_files"].kind == JArray:
    for item in spec["required_files"].elems:
      let rel = item.getStr("")
      var ok = false
      try:
        ok = rel.len > 0 and fileExists(safeJoin(tenant, rel))
      except CatchableError:
        ok = false
      if not ok: allOk = false
      report.add(%*{"verifier": "file_exists", "path": rel, "ok": ok})
  (allOk, report)

proc finalizeTask(h: TaskHandle, finalText: string = "") {.async.} =
  let deterministic = verifyTerminal(h)
  acquire(h.lock)
  h.obs["verification_report"] = copy(deterministic[1])
  release(h.lock)
  if not deterministic[0]:
    h.verified = false
    h.persistReflection("terminal_verification_failed", "continue work until deterministic terminal requirements are satisfied", "runtime", %*{"final": finalText}, deterministic[1])
    h.persistTask()
    h.emit(%*{"type": "verification_failed", "verification_report": deterministic[1]})
    return
  acquire(h.lock)
  h.verified = true
  h.status = "succeeded"
  h.terminalReason = "all configured verifiers satisfied"
  h.sigma["progress"] = %1.0
  h.sigma["phase"] = %"terminal"
  h.orchestratorState = osTerminal
  if finalText.len > 0:
    h.obs["final"] = %finalText
  release(h.lock)
  h.persistTask()
  h.emit(%*{"type": "done", "status": "succeeded", "verified": true, "content": finalText, "verification_report": deterministic[1]})

proc execute(engine: StateTransitionEngine, h: TaskHandle) {.async.} =
  if engine.isNil or h.isNil:
    return
  if h.transitionBusy:
    return
  h.transitionBusy = true
  try:
    var attempt = 0
    while attempt < max(1, engine.maxRetries):
      acquire(h.lock)
      let step = currentPlanStep(h.sigma)
      release(h.lock)
      if step.isNil:
        return
      try:
        discard await h.modelStep(step)
        return
      except CatchableError:
        inc attempt
        if attempt >= max(1, engine.maxRetries):
          raise
        await sleepAsync(100 * attempt)
  finally:
    h.transitionBusy = false

proc executeStep(h: TaskHandle) {.async.} =
  if transitionEngine.isNil:
    transitionEngine = newStateTransitionEngine()
  await transitionEngine.execute(h)

proc executeMicroAction(h: TaskHandle): Future[bool] {.async.} =
  acquire(h.lock)
  if not h.sigma.hasKey("system1") or h.sigma["system1"].kind != JObject or not h.sigma["system1"].hasKey("queued_actions") or h.sigma["system1"]["queued_actions"].kind != JArray or h.sigma["system1"]["queued_actions"].elems.len == 0:
    release(h.lock)
    return false
  let action = copy(h.sigma["system1"]["queued_actions"][0])
  h.sigma["system1"]["queued_actions"].elems.delete(0)
  release(h.lock)
  if action.kind != JObject:
    return false
  let result = await h.executeToolAction(action)
  acquire(h.lock)
  inc h.stepIndex
  h.obs = %*{"status": (if result.ok: "tool_completed" else: "tool_failed"), "action": action, "tool_result": result.payload, "message": result.message, "receipt": result.receipt}
  release(h.lock)
  h.checkpoint(action, %*{"receipt": result.receipt, "ok": result.ok})
  h.persistTask()
  return result.ok

proc completionCheck(h: TaskHandle): Future[(bool, string)] {.async.} =
  h.transitionOrchestrator(osValidate)
  acquire(h.lock)
  let sigma = copy(h.sigma)
  let obs = copy(h.obs)
  let goal = h.sigma{"goal"}.getStr(h.title)
  var expectedCriteria = newJArray()
  if h.sigma{"plan"}{"completion_criteria"}.kind == JArray:
    expectedCriteria = copy(h.sigma{"plan"}{"completion_criteria"})
  elif h.sigma{"route"}{"completion_criteria"}.kind == JArray:
    expectedCriteria = copy(h.sigma{"route"}{"completion_criteria"})
  release(h.lock)
  if runningSubAgentCount(h.taskId) > 0:
    return (false, "")
  let userText = "GOAL:\n" & goal & "\n\nEXPECTED COMPLETION CRITERIA:\n" & canonical(expectedCriteria) & "\n\nSTATE:\n" & canonical(sigma) & "\n\nLATEST OBSERVATION:\n" & canonical(obs) & "\n\nSUBAGENT TREE:\n" & canonical(subAgentTreeJson(h.taskId))
  let resp = await cerebrasCall(%*[{"role": "system", "content": promptText("completion_evaluator")}, {"role": "user", "content": userText}], true)
  if resp.totalTokens > 0 and not chargeTokens(h.tenantId, h.taskId, resp.totalTokens):
    h.haltForBudget()
    return (false, "")
  h.recordModelContext(mrOrchestrator, "completion_check", resp.content, %*{"model": resp.model, "provider": providerName(resp.provider)})
  let node = parseJsonObjectLoose(resp.content)
  if node.isNil or node{"complete"}.kind != JBool or node{"criteria"}.kind != JArray:
    h.persistReflection("completion_check_invalid", "re-evaluate completion from recorded evidence with one result per configured criterion", "orchestrator", %*{"raw_output": resp.content}, obs)
    h.persistTask()
    return (false, "")
  let criteria = node["criteria"]
  if expectedCriteria.elems.len > 0 and criteria.elems.len != expectedCriteria.elems.len:
    h.persistReflection("completion_check_invalid", "criteria count must exactly match configured completion criteria", "orchestrator", %*{"expected_count": expectedCriteria.elems.len, "actual_count": criteria.elems.len}, node)
    return (false, "")
  var allSatisfied = true
  var normalized = newJArray()
  for i, item in criteria.elems:
    if item.kind != JObject or item{"satisfied"}.kind != JBool:
      allSatisfied = false
      continue
    let satisfied = item{"satisfied"}.getBool(false)
    if not satisfied:
      allSatisfied = false
    var entry = copy(item)
    if expectedCriteria.elems.len > i:
      entry["criterion"] = copy(expectedCriteria[i])
    entry["status"] = %(if satisfied: "satisfied" else: "unsatisfied")
    normalized.add(entry)
  acquire(h.lock)
  h.sigma["completion_checklist"] = normalized
  release(h.lock)
  h.persistTask()
  let complete = node{"complete"}.getBool(false) and allSatisfied and (expectedCriteria.elems.len == 0 or normalized.elems.len == expectedCriteria.elems.len)
  if complete:
    return (true, node{"final_response"}.getStr("Task completed."))
  let nextPlan = node{"next_plan"}
  if nextPlan.kind == JArray and nextPlan.elems.len > 0:
    h.replaceRemainingPlan(nextPlan)
    acquire(h.lock)
    h.sigma["step_summary"] = %node{"reason"}.getStr("additional work required")
    release(h.lock)
    h.persistTask()
    h.emit(%*{"type": "plan_replaced", "steps": nextPlan, "reason": node{"reason"}.getStr("")})
  else:
    h.persistReflection("completion_criteria_unsatisfied", "produce additional concrete plan steps", "orchestrator", %*{"reason": node{"reason"}.getStr("")}, node)
  return (false, "")

proc tryBeginTransition(h: TaskHandle): bool =
  acquire(h.lock)
  if h.transitionBusy or h.status != "running" or h.paused or h.stopRequested:
    release(h.lock)
    return false
  h.transitionBusy = true
  release(h.lock)
  true

proc endTransition(h: TaskHandle) =
  acquire(h.lock)
  h.transitionBusy = false
  release(h.lock)

proc system2Think(h: TaskHandle) {.async.} =
  if not h.tryBeginTransition():
    return
  try:
    acquire(h.lock)
    let needsRoute = h.sigma{"route"}.kind != JObject or h.sigma{"route"}.len == 0
    let goal = h.sigma{"goal"}.getStr(h.title)
    let sigma = copy(h.sigma)
    let obs = copy(h.obs)
    let obsDigest = sha1Hex(canonical(obs))
    let previousDigest = h.sigma{"system2"}{"last_observation_digest"}.getStr("")
    let obsStatus = h.obs{"status"}.getStr("")
    release(h.lock)
    if needsRoute:
      let route = await routeTask(goal, sigma, obs, h.tenantId, h.taskId)
      h.applyRoute(route)
      let vector = newJArray()
      h.persistCognition(vector, 1.0, route.intent, "initial model-decided route and task decomposition", route.raw)
      acquire(h.lock)
      h.sigma["system2"]["last_observation_digest"] = %obsDigest
      h.sigma["system2"]["gate"] = %1.0
      h.sigma["system2"]["subgoal"] = %route.intent
      h.sigma["system2"]["strategy"] = %"initial model-decided route and task decomposition"
      release(h.lock)
      h.persistTask()
      return
    if obsDigest == previousDigest:
      return
    let strategicTrigger = obsStatus in ["tool_failed", "step_error", "model_output_invalid", "orchestrator_error"] or sigma{"blockers"}.len > 0 or currentPlanStep(sigma).isNil
    if not strategicTrigger:
      acquire(h.lock)
      h.sigma["system2"]["last_observation_digest"] = %obsDigest
      release(h.lock)
      h.persistTask()
      return
    let systemText = promptText("orchestrator_system2")
    let userText = "GOAL:\n" & goal & "\n\nSTATE:\n" & canonical(sigma) & "\n\nLATEST OBSERVATION:\n" & canonical(obs) & "\n\nSUBAGENT TREE:\n" & canonical(subAgentTreeJson(h.taskId)) & "\n\nTOOLS:\n" & canonical(toolCatalog()) & "\n\nAVAILABLE MODEL REFERENCE SKILLS:\n" & referenceSkillCatalog() & "\n\nVALID SPECIALIST MODEL ROLE IDS:\n" & configuredModelRoleNames().join(", ") & "\n\nVALID SUBAGENT MODEL ROLE IDS:\n" & configuredSubAgentModelRoleNames().join(", ")
    let resp = await cerebrasCall(%*[{"role": "system", "content": systemText}, {"role": "user", "content": userText}], true)
    if resp.totalTokens > 0 and not chargeTokens(h.tenantId, h.taskId, resp.totalTokens):
      h.haltForBudget()
      return
    h.recordModelContext(mrOrchestrator, "system2", resp.content, %*{"model": resp.model})
    let node = parseJsonObjectLoose(resp.content)
    if node.isNil:
      h.persistReflection("system2_output_invalid", "re-run strategic analysis from exact observation", "orchestrator", %*{"raw_output": resp.content}, obs)
      return
    if not node.hasKey("cognition") or node["cognition"].kind != JArray or node["cognition"].elems.len != 16:
      h.persistReflection("system2_cognition_invalid", "return exactly 16 numeric cognition values", "orchestrator", %*{"raw_output": resp.content}, obs)
      return
    var vector = copy(node["cognition"])
    for item in vector.elems:
      if item.kind notin {JInt, JFloat}:
        h.persistReflection("system2_cognition_invalid", "every cognition entry must be numeric", "orchestrator", %*{"raw_output": resp.content}, obs)
        return
    let gate = max(0.0, min(1.0, node{"gate"}.getFloat(0.5)))
    let subgoal = node{"subgoal"}.getStr("")
    let strategy = node{"strategy"}.getStr("")
    acquire(h.lock)
    h.sigma["system2"]["gate"] = %gate
    h.sigma["system2"]["subgoal"] = %subgoal
    h.sigma["system2"]["strategy"] = %strategy
    h.sigma["system2"]["cognition"] = copy(vector)
    h.sigma["system2"]["last_observation_digest"] = %obsDigest
    if node.hasKey("queued_actions") and node["queued_actions"].kind == JArray:
      for action in node["queued_actions"].elems:
        if action.kind == JObject and action{"tool"}.getStr("").len > 0:
          h.sigma["system1"]["queued_actions"].add(copy(action))
    let shouldReplacePlan = node{"replan"}.getBool(false) and node.hasKey("next_plan") and node["next_plan"].kind == JArray and node["next_plan"].elems.len > 0
    let replacementPlan = if shouldReplacePlan: copy(node["next_plan"]) else: newJArray()
    release(h.lock)
    if shouldReplacePlan:
      h.replaceRemainingPlan(replacementPlan)
      h.emit(%*{"type": "plan_replaced", "steps": replacementPlan, "reason": strategy})
    h.persistCognition(vector, gate, subgoal, strategy, node)
    h.persistTask()
    h.emit(%*{"type": "cognition", "gate": gate, "subgoal": subgoal, "strategy": strategy, "cognition": vector})
  except CatchableError as e:
    acquire(h.lock)
    h.obs = %*{"status": "orchestrator_error", "error": e.msg}
    release(h.lock)
    h.persistTask()
    h.emit(%*{"type": "orchestrator_error", "error": e.msg})
  finally:
    h.endTransition()

proc system1Tick(h: TaskHandle) {.async.} =
  if not h.tryBeginTransition():
    return
  try:
    var queuedAction: JsonNode = nil
    acquire(h.lock)
    if h.maxSteps > 0 and h.stepIndex >= h.maxSteps:
      h.status = "halted"
      h.stopRequested = true
      h.verified = false
      h.terminalReason = "max_steps reached"
      h.sigma["phase"] = %"terminal"
      h.orchestratorState = osTerminal
      release(h.lock)
      h.persistTask()
      h.emit(%*{"type": "halted", "reason": "max_steps reached"})
      return
    let preState = copy(h.sigma)
    let routeReady = h.sigma{"route"}.kind == JObject and h.sigma{"route"}.len > 0
    if h.sigma{"system1"}{"queued_actions"}.kind == JArray and h.sigma["system1"]["queued_actions"].elems.len > 0:
      queuedAction = copy(h.sigma["system1"]["queued_actions"][0])
      h.sigma["system1"]["queued_actions"].elems.delete(0)
    let step = if routeReady: currentPlanStep(h.sigma) else: nil
    release(h.lock)
    if not routeReady:
      return
    if not queuedAction.isNil:
      h.transitionOrchestrator(osAct)
      let started = getMonoTime()
      let toolRes = await h.executeToolAction(queuedAction)
      let latency = int((getMonoTime() - started).inMilliseconds)
      acquire(h.lock)
      h.obs = %*{"status": (if toolRes.ok: "tool_completed" else: "tool_failed"), "step_id": h.sigma{"plan"}{"current_step_id"}.getStr(""), "model": "orchestrator", "action": queuedAction, "tool_result": toolRes.payload, "message": toolRes.message, "receipt": toolRes.receipt}
      h.sigma["step_summary"] = %toolRes.message
      let postState = copy(h.sigma)
      let observation = copy(h.obs)
      release(h.lock)
      let receipt = %*{"receipt": toolRes.receipt, "ok": toolRes.ok, "source": "system2_queue"}
      h.logRawTrace("", preState, queuedAction, observation, %*{"source": "system2_queue"}, postState, receipt, toolRes.ok, latency)
      h.checkpoint(queuedAction, receipt)
      if not toolRes.ok:
        h.persistReflection("queued_tool_failure:" & queuedAction{"tool"}.getStr(""), "return the exact tool failure to the strategic orchestrator for a fresh model decision", "system2", %*{"action": queuedAction, "result": toolRes.payload}, observation)
      h.persistTask()
      return
    if step.isNil:
      let completeResult = await h.completionCheck()
      if completeResult[0]:
        h.transitionOrchestrator(osConsolidate)
        await h.finalizeTask(completeResult[1])
      return
    discard await h.modelStep(step)
  except CatchableError as e:
    acquire(h.lock)
    let failedStepId = h.sigma{"plan"}{"current_step_id"}.getStr("")
    h.obs = %*{"status": "step_error", "step_id": failedStepId, "error": e.msg}
    h.sigma["step_summary"] = %("step error: " & e.msg)
    let verifier = copy(h.obs)
    release(h.lock)
    if failedStepId.len > 0:
      h.setStepStatus(failedStepId, "needs_replan")
    h.persistReflection("step_error", "return the exact runtime exception to the strategic orchestrator for a fresh model decision", "runtime", %*{"step_id": failedStepId, "error": e.msg}, verifier)
    h.persistTask()
    h.emit(%*{"type": "step_error", "step_id": failedStepId, "error": e.msg})
  finally:
    h.endTransition()

proc system2Loop(h: TaskHandle) {.async.} =
  while true:
    await sleepAsync(System2HzInterval)
    acquire(h.lock)
    let keep = h.status in ["running", "queued"] and not h.stopRequested
    let should = h.status == "running" and not h.paused
    release(h.lock)
    if not keep:
      break
    if should:
      await h.system2Think()

proc system1Loop(h: TaskHandle) {.async.} =
  while true:
    await sleepAsync(System1HzInterval)
    acquire(h.lock)
    let keep = h.status in ["running", "queued"] and not h.stopRequested
    let should = h.status == "running" and not h.paused
    release(h.lock)
    if not keep:
      break
    if should:
      await h.system1Tick()
      await sleepAsync(200)

proc taskSupervisor(h: TaskHandle) {.async.} =
  let s2 = h.system2Loop()
  try:
    await h.system1Loop()
  finally:
    acquire(h.lock)
    if h.status notin ["running", "queued"]:
      h.stopRequested = true
    let terminal = h.status in ["succeeded", "failed", "halted", "stopped"]
    release(h.lock)
    try:
      await s2
    except CatchableError:
      discard
    h.loopActive.store(false, moRelease)
    if terminal:
      acquire(tasksLock)
      if activeTasks.hasKey(h.taskId):
        activeTasks.del(h.taskId)
      release(tasksLock)

proc launchTask(h: TaskHandle): bool =
  var expected = false
  if not h.loopActive.compareExchange(expected, true, moAcquireRelease, moAcquire):
    return false
  acquire(h.lock)
  h.status = "running"
  h.stopRequested = false
  h.paused = false
  release(h.lock)
  h.persistTask()
  asyncCheck h.taskSupervisor()
  true

proc attachBroadcast(h: TaskHandle)

proc allowedToolsForTenant(tenantId: string): HashSet[string] =
  result = initHashSet[string]()
  let rows = store.query("SELECT allowed_tools FROM tenants WHERE tenant_id=?", @[%tenantId])
  if rows.len == 0:
    return
  let raw = rows[0].getStr("allowed_tools", "[]")
  let node = parseJson(raw)
  if node.kind != JArray:
    raise newException(DbError, "tenant allowed_tools is not an array")
  for item in node.elems:
    if item.kind != JString:
      raise newException(DbError, "tenant allowed_tools contains a non-string entry")
    let name = item.getStr("")
    if toolRegistry.hasKey(name):
      result.incl(name)

proc createTask(title: string, spec: JsonNode, tenantId: string = ""): TaskHandle =
  let taskId = newId("task")
  let resolvedTenant = if tenantId.len > 0: tenantId else: defaultTenantId
  let nSpec = if spec.isNil or spec.kind != JObject: %*{"goal": title} else: copy(spec)
  let goal = nSpec{"goal"}.getStr(title)
  var sigma = if nSpec.hasKey("initial_state") and nSpec["initial_state"].kind == JObject: copy(nSpec["initial_state"]) else: defaultSigma(goal)
  ensureStateShape(sigma, goal)
  let obs = %*{"status": "initialized", "message": "agent task launched"}
  let ts = nowF()
  let requestedMaxSteps = nSpec{"max_steps"}.getInt(0)
  discard store.exec("INSERT INTO tasks (task_id, tenant_id, title, spec_json, initial_state_json, state_json, latest_obs_json, status, step_index, max_steps, tokens_used, terminal_reason, verified, created_at, updated_at) VALUES (?,?,?,?,?,?,?,'queued',0,?,0,'',0,?,?)",
    @[%taskId, %resolvedTenant, %title, %($nSpec), %($sigma), %($sigma), %($obs), %requestedMaxSteps, %ts, %ts])
  result = TaskHandle(taskId: taskId, tenantId: resolvedTenant, title: title, spec: nSpec, sigma: sigma, obs: obs, stepIndex: 0, maxSteps: requestedMaxSteps, status: "queued", terminalReason: "", verified: false, paused: false, stopRequested: false, transitionBusy: false, orchestratorState: osPerceive, subscribers: @[], cognition: newJObject(), cognitionAt: 0.0, allowedTools: initHashSet[string](), broadcastAttached: false, lastPlannedDigest: "")
  result.allowedTools = allowedToolsForTenant(resolvedTenant)
  initLock(result.lock)
  result.loopActive.store(false, moRelaxed)
  acquire(tasksLock)
  activeTasks[taskId] = result
  release(tasksLock)
  attachBroadcast(result)
  result.checkpoint(newJObject(), %*{"status": "initialized"})

proc restoreTask(taskId: string): TaskHandle =
  acquire(tasksLock)
  if activeTasks.hasKey(taskId):
    result = activeTasks[taskId]
    release(tasksLock)
    return
  release(tasksLock)
  let rows = store.query("SELECT * FROM tasks WHERE task_id=?", @[%taskId])
  if rows.len == 0:
    return nil
  let r = rows[0]
  var sigma = r.getJson("state_json", defaultSigma(r.getStr("title")))
  ensureStateShape(sigma, sigma{"goal"}.getStr(r.getStr("title")))
  let phase = sigma{"phase"}.getStr("perceive").toLowerAscii()
  let orch = case phase
    of "deliberate": osDeliberate
    of "act": osAct
    of "validate": osValidate
    of "reflect": osReflect
    of "consolidate": osConsolidate
    of "terminal": osTerminal
    else: osPerceive
  var restoredCognition = newJObject()
  var cognitionAt = 0.0
  let cognitionRows = store.query("SELECT vector_json,gate,subgoal,strategy,route_json,created_at FROM cognition WHERE task_id=? ORDER BY created_at DESC LIMIT 1", @[%taskId])
  if cognitionRows.len > 0:
    restoredCognition = %*{"vector": cognitionRows[0].getJson("vector_json", newJArray()), "gate": cognitionRows[0].getFloat("gate"), "subgoal": cognitionRows[0].getStr("subgoal"), "strategy": cognitionRows[0].getStr("strategy"), "route": cognitionRows[0].getJson("route_json")}
    cognitionAt = cognitionRows[0].getFloat("created_at")
  result = TaskHandle(taskId: taskId, tenantId: r.getStr("tenant_id", defaultTenantId), title: r.getStr("title"), spec: r.getJson("spec_json"), sigma: sigma, obs: r.getJson("latest_obs_json"), stepIndex: r.getInt("step_index").int, maxSteps: r.getInt("max_steps").int, status: r.getStr("status"), terminalReason: r.getStr("terminal_reason"), verified: r.getInt("verified") == 1, paused: false, stopRequested: false, transitionBusy: false, orchestratorState: orch, subscribers: @[], cognition: restoredCognition, cognitionAt: cognitionAt, allowedTools: initHashSet[string](), broadcastAttached: false, lastPlannedDigest: "")
  result.allowedTools = allowedToolsForTenant(result.tenantId)
  initLock(result.lock)
  result.loopActive.store(false, moRelaxed)
  if result.status notin ["succeeded", "failed", "halted", "stopped"]:
    acquire(tasksLock)
    activeTasks[taskId] = result
    release(tasksLock)
    attachBroadcast(result)

proc subAgentSnapshot(agentId: string): JsonNode =
  let rows = store.query("SELECT * FROM subagents WHERE agent_id=?", @[%agentId])
  if rows.len == 0:
    return nil
  let r = rows[0]
  %*{
    "agent_id": r.getStr("agent_id"),
    "task_id": r.getStr("task_id"),
    "parent_agent_id": r.getStr("parent_agent_id"),
    "model": r.getStr("model_role"),
    "name": r.getStr("name"),
    "goal": r.getStr("goal"),
    "instructions": r.getStr("instructions"),
    "context": r.getJson("context_json"),
    "state": r.getJson("state_json"),
    "status": r.getStr("status"),
    "result": r.getStr("result"),
    "error": r.getStr("error"),
    "stop_requested": r.getInt("stop_requested") == 1,
    "created_at": r.getFloat("created_at"),
    "updated_at": r.getFloat("updated_at")
  }

proc persistSubAgent(a: SubAgentHandle) =
  if a.isNil:
    return
  acquire(a.lock)
  let messages = copy(a.messages)
  let state = copy(a.state)
  let status = a.status
  let resultText = a.resultText
  let errorText = a.errorText
  let stopped = a.stopRequested
  release(a.lock)
  discard store.exec("UPDATE subagents SET messages_json=?,state_json=?,status=?,result=?,error=?,stop_requested=?,updated_at=? WHERE agent_id=?",
    @[%($messages), %($state), %status, %resultText, %errorText, %(if stopped: 1 else: 0), %nowF(), %a.agentId])

proc persistSubAgentEvent(a: SubAgentHandle, ev: JsonNode) =
  if a.isNil:
    return
  discard store.exec("INSERT INTO subagent_events (agent_id,sequence,event_json,created_at) SELECT ?,COALESCE(MAX(sequence),0)+1,?,? FROM subagent_events WHERE agent_id=?",
    @[%a.agentId, %($ev), %nowF(), %a.agentId])
  if not a.rootTask.isNil:
    var rootEvent = copy(ev)
    if rootEvent.kind != JObject:
      rootEvent = %*{"payload": rootEvent}
    rootEvent["type"] = %("subagent_" & ev{"type"}.getStr("event"))
    rootEvent["agent_id"] = %a.agentId
    rootEvent["parent_agent_id"] = %a.parentAgentId
    rootEvent["model"] = %modelRoleName(a.modelRole)
    a.rootTask.emit(rootEvent)

proc appendSubAgentMessage(a: SubAgentHandle, role, content: string) =
  acquire(a.lock)
  if a.messages.isNil or a.messages.kind != JArray:
    a.messages = newJArray()
  a.messages.add(%*{"role": role, "content": content})
  release(a.lock)

proc restoreSubAgent(agentId: string): SubAgentHandle =
  acquire(subAgentsLock)
  if activeSubAgents.hasKey(agentId):
    result = activeSubAgents[agentId]
    release(subAgentsLock)
    return
  release(subAgentsLock)
  let rows = store.query("SELECT * FROM subagents WHERE agent_id=?", @[%agentId])
  if rows.len == 0:
    return nil
  let r = rows[0]
  let taskId = r.getStr("task_id")
  let root = restoreTask(taskId)
  if root.isNil:
    return nil
  let modelRole = parseModelRole(r.getStr("model_role"))
  result = SubAgentHandle(
    agentId: agentId,
    taskId: taskId,
    parentAgentId: r.getStr("parent_agent_id"),
    name: r.getStr("name"),
    goal: r.getStr("goal"),
    instructions: r.getStr("instructions"),
    modelRole: modelRole,
    context: r.getJson("context_json"),
    messages: r.getJson("messages_json", newJArray()),
    state: r.getJson("state_json"),
    status: r.getStr("status"),
    resultText: r.getStr("result"),
    errorText: r.getStr("error"),
    stopRequested: r.getInt("stop_requested") == 1,
    rootTask: root
  )
  initLock(result.lock)
  result.loopActive.store(false, moRelaxed)
  if result.status notin ["succeeded", "failed", "stopped"]:
    acquire(subAgentsLock)
    activeSubAgents[agentId] = result
    release(subAgentsLock)

proc createSubAgent(h: TaskHandle, args: JsonNode): SubAgentHandle =
  let modelName = args{"model"}.getStr("").strip()
  let goal = args{"goal"}.getStr("").strip()
  if modelName.len == 0:
    raise newException(ValueError, "subagent model is required and must be chosen explicitly by the calling model")
  if goal.len == 0:
    raise newException(ValueError, "subagent goal is required")
  let role = parseModelRole(modelName)
  let id = newId("agent")
  let parentId = args{"_actor_agent_id"}.getStr("").strip()
  let name = args{"name"}.getStr(modelRoleName(role) & " subagent").strip()
  let instructions = args{"instructions"}.getStr("")
  let context = if args.hasKey("context") and args["context"].kind in {JObject, JArray}: copy(args["context"]) else: newJObject()
  var messages = newJArray()
  messages.add(%*{"role": "system", "content": subAgentSystem(role)})
  let initial = "SUBAGENT ID:\n" & id &
    "\n\nROOT TASK ID:\n" & h.taskId &
    "\n\nPARENT SUBAGENT ID:\n" & parentId &
    "\n\nDELEGATED GOAL:\n" & goal &
    "\n\nCALLER INSTRUCTIONS:\n" & instructions &
    "\n\nDELEGATED CONTEXT:\n" & canonical(context) &
    "\n\nCURRENT SUBAGENT TREE:\n" & canonical(subAgentTreeJson(h.taskId)) &
    "\n\nAVAILABLE REAL TOOLS:\n" & canonical(toolCatalog()) &
    "\n\nVALID SUBAGENT MODEL ROLE IDS:\n" & configuredSubAgentModelRoleNames().join(", ") &
    "\n\nMODEL REFERENCE SKILLS:\n" & referenceSkillCatalog()
  messages.add(%*{"role": "user", "content": initial})
  if role in {mrGemini38, mrGrok43} and h.spec.hasKey("messages") and h.spec["messages"].kind == JArray:
    for original in h.spec["messages"].elems:
      if original.kind != JObject:
        continue
      let content = original{"content"}
      if content.kind != JArray:
        continue
      var hasMedia = false
      for part in content.elems:
        if part.kind == JObject and part{"type"}.getStr("") in ["image", "image_url", "input_image", "video", "video_url", "input_video", "document", "file", "input_file"]:
          hasMedia = true
          break
      if hasMedia:
        messages.add(copy(original))
  let ts = nowF()
  let state = %*{
    "cycle": "analyze",
    "iteration": 0,
    "last_action": newJNull(),
    "last_observation": newJNull(),
    "summary": ""
  }
  discard store.exec("INSERT INTO subagents (agent_id,task_id,parent_agent_id,model_role,name,goal,instructions,context_json,messages_json,state_json,status,result,error,stop_requested,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,'queued','','',0,?,?)",
    @[%id, %h.taskId, %parentId, %modelRoleName(role), %name, %goal, %instructions, %($context), %($messages), %($state), %ts, %ts])
  result = SubAgentHandle(
    agentId: id,
    taskId: h.taskId,
    parentAgentId: parentId,
    name: name,
    goal: goal,
    instructions: instructions,
    modelRole: role,
    context: context,
    messages: messages,
    state: state,
    status: "queued",
    resultText: "",
    errorText: "",
    stopRequested: false,
    rootTask: h
  )
  initLock(result.lock)
  result.loopActive.store(false, moRelaxed)
  acquire(subAgentsLock)
  activeSubAgents[id] = result
  release(subAgentsLock)
  result.persistSubAgentEvent(%*{"type": "created", "goal": goal, "name": name})

proc subAgentActionWithActor(a: SubAgentHandle, action: JsonNode): JsonNode =
  result = copy(action)
  if result.isNil or result.kind != JObject:
    result = newJObject()
  var args = result{"args"}
  if args.isNil or args.kind != JObject:
    args = newJObject()
  else:
    args = copy(args)
  args["_actor_agent_id"] = %a.agentId
  args["_actor_model"] = %modelRoleName(a.modelRole)
  result["args"] = args

proc pruneSubAgentMessages(messages: JsonNode, keepRecent: int): JsonNode =
  result = newJArray()
  if messages.isNil or messages.kind != JArray:
    return
  if messages.elems.len > 0 and messages[0].kind == JObject and messages[0]{"role"}.getStr("") == "system":
    result.add(copy(messages[0]))
  let startAt = max(1, messages.elems.len - max(2, keepRecent))
  for i in startAt ..< messages.elems.len:
    result.add(copy(messages[i]))

proc verifySubAgentResult(a: SubAgentHandle, candidate: string, modelNode: JsonNode): Future[(bool, JsonNode)] {.async.} =
  if a.isNil or a.rootTask.isNil:
    return (false, %*{"verified": false, "reason": "missing root task"})
  let descendantsRunning = store.query("SELECT COUNT(*) AS c FROM subagents WHERE task_id=? AND parent_agent_id=? AND status NOT IN ('succeeded','failed','stopped')", @[%a.taskId, %a.agentId])
  if descendantsRunning.len > 0 and descendantsRunning[0].getInt("c", 0) > 0:
    return (false, %*{"verified": false, "reason": "child subagents are still running"})
  acquire(a.lock)
  let state = copy(a.state)
  let context = copy(a.context)
  release(a.lock)
  let payload = %*{"agent_id": a.agentId, "goal": a.goal, "instructions": a.instructions, "context": context, "state": state, "candidate_result": candidate, "model_output": copy(modelNode)}
  let resp = await cerebrasCall(%*[{"role": "system", "content": promptText("subagent_verifier")}, {"role": "user", "content": canonical(payload)}], true)
  if resp.totalTokens > 0 and not chargeTokens(a.rootTask.tenantId, a.taskId, resp.totalTokens):
    a.rootTask.haltForBudget()
    return (false, %*{"verified": false, "reason": "token budget exhausted"})
  let node = parseJsonObjectLoose(resp.content)
  if node.isNil or node{"verified"}.kind != JBool:
    return (false, %*{"verified": false, "reason": "invalid verifier output", "raw": resp.content})
  return (node{"verified"}.getBool(false), node)

proc subAgentSupervisor(a: SubAgentHandle) {.async.} =
  let startedAt = nowF()
  let maxSeconds = positiveEnvInt("SUBAGENT_MAX_SECONDS", 3600)
  let maxIterations = positiveEnvInt("SUBAGENT_MAX_ITERATIONS", 1000)
  let keepMessages = positiveEnvInt("SUBAGENT_CONTEXT_MESSAGES", 40)
  try:
    acquire(a.lock)
    if a.stopRequested or a.status == "stopping":
      a.status = "stopped"
      release(a.lock)
      a.persistSubAgent()
      a.persistSubAgentEvent(%*{"type": "stopped"})
      return
    a.status = "running"
    a.errorText = ""
    release(a.lock)
    a.persistSubAgent()
    a.persistSubAgentEvent(%*{"type": "started"})
    while true:
      acquire(a.lock)
      let shouldStop = a.stopRequested
      let iteration = a.state{"iteration"}.getInt(0)
      let messages = pruneSubAgentMessages(a.messages, keepMessages)
      release(a.lock)
      if shouldStop:
        acquire(a.lock)
        a.status = "stopped"
        release(a.lock)
        a.persistSubAgent()
        a.persistSubAgentEvent(%*{"type": "stopped"})
        break
      acquire(a.rootTask.lock)
      let rootStopping = a.rootTask.stopRequested or a.rootTask.status in ["halted", "failed", "stopped", "succeeded"]
      release(a.rootTask.lock)
      if rootStopping:
        acquire(a.lock)
        a.status = "stopped"
        a.stopRequested = true
        a.errorText = "root task is terminal or stopping"
        release(a.lock)
        a.persistSubAgent()
        a.persistSubAgentEvent(%*{"type": "stopped", "reason": "root task terminal"})
        break
      if iteration >= maxIterations or nowF() - startedAt >= float(maxSeconds):
        acquire(a.lock)
        a.status = "failed"
        a.errorText = "subagent liveness bound reached before verified completion"
        release(a.lock)
        a.persistSubAgent()
        a.persistSubAgentEvent(%*{"type": "failed", "error": "subagent liveness bound reached before verified completion"})
        break
      let started = getMonoTime()
      var resp: LlmResponse
      try:
        resp = await invokeModel(a.modelRole, messages, a.modelRole != mrGemini38, a.rootTask.tenantId, a.taskId)
      except CatchableError as e:
        acquire(a.lock)
        a.status = "failed"
        a.errorText = e.msg
        release(a.lock)
        a.persistSubAgent()
        a.persistSubAgentEvent(%*{"type": "failed", "error": e.msg})
        break
      let latency = int((getMonoTime() - started).inMilliseconds)
      a.appendSubAgentMessage("assistant", resp.content)
      let node = parseJsonObjectLoose(resp.content)
      if node.isNil:
        a.appendSubAgentMessage("user", "Your previous output was not a valid autonomous subagent action object. Return exactly one JSON object following the subagent protocol. Preserve the delegated goal and continue from the current real state.")
        acquire(a.lock)
        a.state["cycle"] = %"reflect"
        a.state["last_observation"] = %*{"status": "invalid_model_output", "raw": resp.content}
        a.state["iteration"] = %(iteration + 1)
        release(a.lock)
        a.persistSubAgent()
        a.persistSubAgentEvent(%*{"type": "invalid_output", "content": resp.content, "latency_ms": latency})
        continue
      let action = node{"action"}
      if not action.isNil and action.kind == JObject and action{"tool"}.getStr("").len > 0:
        let actorAction = subAgentActionWithActor(a, action)
        acquire(a.lock)
        a.state["cycle"] = %"act"
        a.state["last_action"] = copy(action)
        release(a.lock)
        a.persistSubAgentEvent(%*{"type": "tool_start", "action": action, "latency_ms": latency})
        let toolResult = await a.rootTask.executeToolAction(actorAction)
        let observation = %*{"ok": toolResult.ok, "payload": copy(toolResult.payload), "message": toolResult.message, "receipt": toolResult.receipt}
        a.appendSubAgentMessage("user", "TOOL OBSERVATION:\n" & canonical(observation) & "\nContinue the atomic agentic cycle from this exact observation. You may use any available tool, create further subagents with any configured model, or finish only after verification.")
        acquire(a.lock)
        a.state["cycle"] = %"observe"
        a.state["last_observation"] = observation
        a.state["summary"] = %node{"summary"}.getStr(toolResult.message)
        a.state["iteration"] = %(iteration + 1)
        release(a.lock)
        a.persistSubAgent()
        a.persistSubAgentEvent(%*{"type": "tool_result", "action": action, "observation": observation})
        continue
      var done = node{"done"}.getBool(node{"step_complete"}.getBool(false))
      var finalText = node{"final"}.getStr("")
      if a.modelRole == mrGemini38 and node.hasKey("images") and node["images"].kind == JArray and node["images"].elems.len > 0:
        done = true
        finalText = canonical(node)
      if done:
        if finalText.len == 0:
          finalText = node{"summary"}.getStr(resp.content)
        let verification = await a.verifySubAgentResult(finalText, node)
        acquire(a.lock)
        a.state["cycle"] = %"verify"
        a.state["last_verification"] = copy(verification[1])
        a.state["iteration"] = %(iteration + 1)
        release(a.lock)
        if verification[0]:
          acquire(a.lock)
          a.status = "succeeded"
          a.resultText = finalText
          a.errorText = ""
          a.state["summary"] = %node{"summary"}.getStr(finalText)
          release(a.lock)
          a.persistSubAgent()
          a.persistSubAgentEvent(%*{"type": "done", "result": finalText, "verification": verification[1], "latency_ms": latency})
          break
        a.appendSubAgentMessage("user", "VERIFICATION FAILED:\n" & canonical(verification[1]) & "\nContinue working from this evidence. Do not return done=true until the delegated goal is verified.")
        acquire(a.lock)
        a.state["cycle"] = %"reflect"
        release(a.lock)
        a.persistSubAgent()
        a.persistSubAgentEvent(%*{"type": "verification_failed", "verification": verification[1]})
        continue
      a.appendSubAgentMessage("user", "No external action was requested and the delegated goal is not complete. Continue the atomic agentic cycle. Select the next real tool or subagent action yourself, or return done=true only after the goal is actually verified.")
      acquire(a.lock)
      a.state["cycle"] = %"reflect"
      a.state["summary"] = %node{"summary"}.getStr("")
      a.state["iteration"] = %(iteration + 1)
      release(a.lock)
      a.persistSubAgent()
  except CatchableError as e:
    acquire(a.lock)
    a.status = "failed"
    a.errorText = e.msg
    release(a.lock)
    try:
      a.persistSubAgent()
      a.persistSubAgentEvent(%*{"type": "failed", "error": e.msg})
    except CatchableError:
      discard
  finally:
    a.loopActive.store(false, moRelease)
    acquire(subAgentsLock)
    if activeSubAgents.hasKey(a.agentId):
      activeSubAgents.del(a.agentId)
    release(subAgentsLock)

proc launchSubAgent(a: SubAgentHandle): bool =
  if a.isNil:
    return false
  var expected = false
  if not a.loopActive.compareExchange(expected, true, moAcquireRelease, moAcquire):
    return false
  acquire(a.lock)
  if a.status in ["succeeded", "failed", "stopped"] or a.status == "stopping" or a.stopRequested:
    if a.status == "stopping" or a.stopRequested:
      a.status = "stopped"
      a.stopRequested = true
    release(a.lock)
    a.loopActive.store(false, moRelease)
    a.persistSubAgent()
    return false
  a.status = "running"
  a.stopRequested = false
  release(a.lock)
  a.persistSubAgent()
  asyncCheck a.subAgentSupervisor()
  true

proc selectedSubAgentIds(h: TaskHandle, args: JsonNode): seq[string] =
  result = @[]
  let actorId = args{"_actor_agent_id"}.getStr("").strip()
  if args.hasKey("agent_ids") and args["agent_ids"].kind == JArray:
    for item in args["agent_ids"].elems:
      if item.kind != JString:
        continue
      let id = item.getStr("").strip()
      if id.len == 0 or id == actorId or id in result:
        continue
      let rows = store.query("SELECT agent_id FROM subagents WHERE agent_id=? AND task_id=?", @[%id, %h.taskId])
      if rows.len > 0:
        result.add(id)
    return
  let scope = args{"scope"}.getStr("direct_children").strip().toLowerAscii()
  let rows = store.query("SELECT agent_id,parent_agent_id FROM subagents WHERE task_id=? ORDER BY created_at ASC", @[%h.taskId])
  if scope == "task":
    for r in rows:
      let id = r.getStr("agent_id")
      if id.len > 0 and id != actorId:
        result.add(id)
    return
  var direct: seq[string] = @[]
  for r in rows:
    let id = r.getStr("agent_id")
    if r.getStr("parent_agent_id") == actorId and id != actorId:
      direct.add(id)
  if scope != "descendants":
    return direct
  result = direct
  var cursor = 0
  while cursor < result.len:
    let parent = result[cursor]
    for r in rows:
      let id = r.getStr("agent_id")
      if id != actorId and r.getStr("parent_agent_id") == parent and id notin result:
        result.add(id)
    inc cursor

proc spawnSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
  try:
    let agent = createSubAgent(h, args)
    discard launchSubAgent(agent)
    if args{"wait"}.getBool(false):
      var waitArgs = %*{"agent_ids": [agent.agentId]}
      waitArgs["_actor_agent_id"] = %args{"_actor_agent_id"}.getStr("")
      return await waitSubAgentsTool(h, waitArgs)
    let snapshot = subAgentSnapshot(agent.agentId)
    return ToolResult(ok: true, payload: snapshot, receipt: "subagent:" & agent.agentId, message: "subagent launched")
  except CatchableError as e:
    return ToolResult(ok: false, payload: %*{"error": e.msg}, receipt: "", message: e.msg)

proc waitSubAgentsTool(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
  let ids = selectedSubAgentIds(h, args)
  if ids.len == 0:
    return ToolResult(ok: true, payload: %*{"agents": newJArray()}, receipt: "subagents:none", message: "no matching subagents")
  let timeoutMs = max(100, args{"timeout_ms"}.getInt(positiveEnvInt("SUBAGENT_WAIT_TIMEOUT_MS", 600000)))
  let started = getMonoTime()
  while true:
    acquire(h.lock)
    let cancelled = h.stopRequested or h.status in ["halted", "failed", "stopped"]
    release(h.lock)
    if cancelled:
      return ToolResult(ok: false, payload: %*{"agents": ids}, receipt: "subagents:cancelled", message: "root task stopped while waiting")
    if int((getMonoTime() - started).inMilliseconds) >= timeoutMs:
      var snapshots = newJArray()
      for id in ids:
        let snapshot = subAgentSnapshot(id)
        if not snapshot.isNil:
          snapshots.add(snapshot)
      return ToolResult(ok: false, payload: %*{"agents": snapshots, "timeout_ms": timeoutMs}, receipt: "subagents:timeout", message: "subagent wait timeout")
    var allTerminal = true
    var snapshots = newJArray()
    for id in ids:
      let snapshot = subAgentSnapshot(id)
      if snapshot.isNil:
        snapshots.add(%*{"agent_id": id, "status": "missing"})
        continue
      snapshots.add(snapshot)
      if snapshot{"status"}.getStr("") notin ["succeeded", "failed", "stopped"]:
        allTerminal = false
    if allTerminal:
      var allSucceeded = true
      for snapshot in snapshots.elems:
        if snapshot.kind == JObject and snapshot{"status"}.getStr("") != "succeeded":
          allSucceeded = false
      return ToolResult(ok: allSucceeded, payload: %*{"agents": snapshots}, receipt: "subagents:" & sha1Hex(canonical(snapshots)), message: (if allSucceeded: "subagents completed" else: "one or more subagents did not succeed"))
    await sleepAsync(100)

proc listSubAgentsTool(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
  let ids = selectedSubAgentIds(h, args)
  var arr = newJArray()
  for id in ids:
    let snapshot = subAgentSnapshot(id)
    if not snapshot.isNil:
      arr.add(snapshot)
  return ToolResult(ok: true, payload: %*{"agents": arr}, receipt: "subagent-list:" & sha1Hex(canonical(arr)), message: "subagent tree returned")

proc getSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
  let id = args{"agent_id"}.getStr("").strip()
  if id.len == 0:
    return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "agent_id required")
  let rows = store.query("SELECT task_id FROM subagents WHERE agent_id=?", @[%id])
  if rows.len == 0 or rows[0].getStr("task_id") != h.taskId:
    return ToolResult(ok: false, payload: %*{"agent_id": id}, receipt: "", message: "subagent not found for task")
  let snapshot = subAgentSnapshot(id)
  return ToolResult(ok: true, payload: snapshot, receipt: "subagent:" & id, message: "subagent state returned")

proc messageSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
  let id = args{"agent_id"}.getStr("").strip()
  let message = args{"message"}.getStr("").strip()
  if id.len == 0 or message.len == 0:
    return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "agent_id and message required")
  let agent = restoreSubAgent(id)
  if agent.isNil or agent.taskId != h.taskId:
    return ToolResult(ok: false, payload: %*{"agent_id": id}, receipt: "", message: "subagent not found for task")
  acquire(agent.lock)
  let terminal = agent.status in ["succeeded", "failed", "stopped"]
  release(agent.lock)
  if terminal:
    return ToolResult(ok: false, payload: subAgentSnapshot(id), receipt: "subagent:" & id, message: "subagent is already terminal")
  agent.appendSubAgentMessage("user", "PARENT MESSAGE:\n" & message)
  agent.persistSubAgent()
  agent.persistSubAgentEvent(%*{"type": "message", "message": message})
  discard launchSubAgent(agent)
  return ToolResult(ok: true, payload: subAgentSnapshot(id), receipt: "subagent-message:" & id, message: "message delivered")

proc stopSubAgentTool(h: TaskHandle, args: JsonNode): Future[ToolResult] {.async.} =
  let id = args{"agent_id"}.getStr("").strip()
  if id.len == 0:
    return ToolResult(ok: false, payload: newJObject(), receipt: "", message: "agent_id required")
  let agent = restoreSubAgent(id)
  if agent.isNil or agent.taskId != h.taskId:
    return ToolResult(ok: false, payload: %*{"agent_id": id}, receipt: "", message: "subagent not found for task")
  acquire(agent.lock)
  if agent.status notin ["succeeded", "failed", "stopped"]:
    agent.stopRequested = true
    agent.status = "stopping"
  release(agent.lock)
  agent.persistSubAgent()
  agent.persistSubAgentEvent(%*{"type": "stop_requested"})
  return ToolResult(ok: true, payload: subAgentSnapshot(id), receipt: "subagent-stop:" & id, message: "stop requested")

proc resumePendingSubAgents() =
  for r in store.query("SELECT agent_id FROM subagents WHERE status IN ('queued','running','stopping') ORDER BY created_at ASC"):
    let agent = restoreSubAgent(r.getStr("agent_id"))
    if not agent.isNil:
      acquire(agent.lock)
      if agent.status == "stopping":
        agent.stopRequested = true
      release(agent.lock)
      discard launchSubAgent(agent)

proc resumePendingTasks() =
  for r in store.query("SELECT task_id FROM tasks WHERE status IN ('queued','running') ORDER BY created_at ASC"):
    let h = restoreTask(r.getStr("task_id"))
    if h != nil:
      discard h.launchTask()

var
  skillGateLock: Lock
  skillGateBusy = false
  validationGate = ValidationGate(epsilon: 1e-9)
  metaAgent = MetaAgent(minOccurrences: 3, lookback: 12, maxCandidates: 3, gate: validationGate)

proc removeTree(path: string) =
  if not dirExists(path):
    return
  var children: seq[(PathComponent, string)] = @[]
  for kind, child in walkDir(path):
    children.add((kind, child))
  for item in children:
    case item[0]
    of pcDir:
      removeTree(item[1])
    of pcFile, pcLinkToFile, pcLinkToDir:
      try:
        removeFile(item[1])
      except CatchableError:
        try:
          removeDir(item[1])
        except CatchableError:
          discard
  try:
    removeDir(path)
  except CatchableError:
    discard

proc ensureDiagnosticSuite(tenantId: string) =
  let existing = store.query("SELECT domain FROM diagnostics WHERE tenant_id=?", @[%tenantId])
  var domains = initHashSet[string]()
  for row in existing:
    domains.incl(row.getStr("domain"))
  let definitions = @[
    ("arithmetic_rollout", %*{"goal": "Compute (17 * 6) + 5 with the deterministic math evaluator and store 107 in facts.answer.", "operation": "math", "expression": "(17 * 6) + 5"}, %*{"verifiers": [{"type": "state_path_equals", "path": "/facts/answer", "expected": 107}, {"type": "state_path_equals", "path": "/progress", "expected": 1.0}]}),
    ("filesystem_rollout", %*{"goal": "Create regression/output.txt with validation-gate-ok and verify the real file.", "operation": "file", "path": "regression/output.txt", "content": "validation-gate-ok\n"}, %*{"verifiers": [{"type": "file_contains", "path": "regression/output.txt", "needle": "validation-gate-ok"}, {"type": "state_path_equals", "path": "/progress", "expected": 1.0}]}),
    ("memory_rollout", %*{"goal": "Retrieve a reusable skill for file operations.", "operation": "memory", "query": "sandboxed file operations"}, %*{"verifiers": [{"type": "state_path_equals", "path": "/facts/memory_routed", "expected": true}, {"type": "state_path_equals", "path": "/progress", "expected": 1.0}]})
  ]
  for definition in definitions:
    if definition[0] in domains:
      continue
    discard store.exec("INSERT INTO diagnostics (diagnostic_id,tenant_id,domain,spec_json,expectation_json,created_at) VALUES (?,?,?,?,?,?)",
      @[%newId("diag"), %tenantId, %definition[0], %canonical(definition[1]), %canonical(definition[2]), %nowF()])

proc verifyDiagnosticSnapshot(sandboxTenant: string, expectation, sigma: JsonNode): (bool, JsonNode) =
  var report = newJArray()
  var allOk = true
  let verifiers = if not expectation.isNil and expectation.kind == JObject and expectation.hasKey("verifiers") and expectation["verifiers"].kind == JArray:
      expectation["verifiers"]
    elif not expectation.isNil and expectation.kind == JArray:
      expectation
    else:
      newJArray()
  if verifiers.elems.len == 0:
    return (false, %*[{"verifier": "configuration", "ok": false, "error": "diagnostic has no verifiers"}])
  for verifier in verifiers.elems:
    if verifier.kind != JObject:
      allOk = false
      report.add(%*{"verifier": "invalid", "ok": false})
      continue
    let kind = verifier{"type"}.getStr("")
    case kind
    of "state_path_equals":
      let path = verifier{"path"}.getStr("")
      let actual = jsonPointerGet(sigma, path)
      let expected = if verifier.hasKey("expected"): verifier["expected"] else: newJNull()
      let ok = not actual.isNil and jsonEquivalent(actual, expected)
      if not ok: allOk = false
      report.add(%*{"verifier": kind, "path": path, "ok": ok, "expected": expected, "actual": (if actual.isNil: newJNull() else: copy(actual))})
    of "file_exists":
      let rel = verifier{"path"}.getStr("")
      var ok = false
      try:
        ok = rel.len > 0 and fileExists(safeJoin(sandboxTenant, rel))
      except CatchableError:
        ok = false
      if not ok: allOk = false
      report.add(%*{"verifier": kind, "path": rel, "ok": ok})
    of "file_contains":
      let rel = verifier{"path"}.getStr("")
      let needle = verifier{"needle"}.getStr("")
      var ok = false
      try:
        let full = safeJoin(sandboxTenant, rel)
        ok = fileExists(full) and needle.len > 0 and needle in readFile(full)
      except CatchableError:
        ok = false
      if not ok: allOk = false
      report.add(%*{"verifier": kind, "path": rel, "needle": needle, "ok": ok})
    of "all_subgoals_resolved":
      var unresolved = 0
      if sigma.hasKey("subgoals") and sigma["subgoals"].kind == JArray:
        for item in sigma["subgoals"].elems:
          if item.kind == JObject and item{"status"}.getStr("open") in ["open", "in_progress", "queued", "running"]:
            inc unresolved
      let ok = unresolved == 0
      if not ok: allOk = false
      report.add(%*{"verifier": kind, "ok": ok, "unresolved": unresolved})
    of "no_blockers":
      let count = if sigma.hasKey("blockers") and sigma["blockers"].kind == JArray: sigma["blockers"].elems.len else: 0
      let ok = count == 0
      if not ok: allOk = false
      report.add(%*{"verifier": kind, "ok": ok, "blockers_count": count})
    else:
      allOk = false
      report.add(%*{"verifier": kind, "ok": false, "error": "unsupported diagnostic verifier"})
  (allOk, report)

proc validateSkillDsl(code: string): (bool, seq[string])

proc diagnosticAllowedTools(tenantId: string): HashSet[string] =
  allowedToolsForTenant(tenantId)

proc runDiagnosticRolloutCase(tenantId: string, diagnostic: Row, candidateOverride: JsonNode = nil): Future[JsonNode] {.async.} =
  let spec = diagnostic.getJson("spec_json")
  let expectation = diagnostic.getJson("expectation_json")
  let sandboxTenant = sanitizeKnowledgeName(tenantId) & "_gate_" & newId("rollout")
  var sigma = defaultSigma(spec{"goal"}.getStr("diagnostic"))
  let operation = spec{"operation"}.getStr("")
  var operationResult = newJObject()
  var candidateEvidence = newJObject()
  if not candidateOverride.isNil and candidateOverride.kind == JObject:
    let skillCode = candidateOverride{"skill_code"}.getStr("")
    let validation = validateSkillDsl(skillCode)
    if not validation[0]:
      return %*{"passed": false, "domain": diagnostic.getStr("domain"), "candidate_evaluated": true, "candidate_error": "invalid candidate DSL", "validation_errors": validation[1]}
    let domain = candidateOverride{"domain"}.getStr("general")
    let trigger = candidateOverride{"trigger_spec"}.getStr(candidateOverride{"trigger"}.getStr(""))
    let procedure = candidateOverride{"procedure_spec"}.getStr(candidateOverride{"procedure"}.getStr(""))
    let relevantText = (domain & " " & trigger & " " & procedure & " " & skillCode).toLowerAscii()
    let diagnosticText = (diagnostic.getStr("domain") & " " & spec{"goal"}.getStr("") & " " & operation).toLowerAscii()
    var overlap = 0
    for term in contentTerms(diagnosticText):
      if term in relevantText:
        inc overlap
    candidateEvidence = %*{"name": candidateOverride{"name"}.getStr(""), "domain": domain, "overlap": overlap, "dsl_valid": true}
  try:
    case operation
    of "math":
      let expression = spec{"expression"}.getStr("")
      let evaluated = evalMathExpression(expression)
      if evaluated[0]:
        sigma["facts"]["answer"] = %evaluated[1]
        sigma["progress"] = %1.0
        operationResult = %*{"ok": true, "value": evaluated[1]}
      else:
        operationResult = %*{"ok": false, "error": evaluated[2]}
    of "file":
      let rel = spec{"path"}.getStr("")
      let content = spec{"content"}.getStr("")
      let full = safeJoin(sandboxTenant, rel)
      atomicWrite(full, content)
      let verified = fileExists(full) and readFile(full) == content
      if verified:
        sigma["progress"] = %1.0
      operationResult = %*{"ok": verified, "path": rel, "sha1": sha1Hex(content)}
    of "memory":
      let query = spec{"query"}.getStr("file operations")
      let hits = searchSkills(tenantId, query, 5)
      sigma["facts"]["memory_routed"] = %(hits.len > 0)
      if hits.len > 0:
        sigma["progress"] = %1.0
      operationResult = %*{"ok": hits.len > 0, "hits": hits.len}
    else:
      operationResult = %*{"ok": false, "error": "unsupported diagnostic operation"}
    let verified = verifyDiagnosticSnapshot(sandboxTenant, expectation, sigma)
    let candidateOk = candidateOverride.isNil or candidateOverride.kind != JObject or candidateEvidence{"dsl_valid"}.getBool(false)
    return %*{"passed": verified[0] and candidateOk, "domain": diagnostic.getStr("domain"), "operation": operationResult, "verification": verified[1], "state": sigma, "candidate_evaluated": not candidateOverride.isNil, "candidate_evidence": candidateEvidence}
  finally:
    try:
      removeTree(safeJoin(sandboxTenant, "."))
    except CatchableError:
      discard

proc validateSkillDsl(code: string): (bool, seq[string]) =
  var errors: seq[string] = @[]
  let text = code.strip()
  if text.len == 0:
    errors.add("skill_code is empty")
    return (false, errors)
  var sawWhen = false
  var sawStep = false
  var sawVerify = false
  for rawLine in text.splitLines():
    let line = rawLine.strip()
    if line.len == 0:
      continue
    let upper = line.toUpperAscii()
    if upper.startsWith("WHEN "):
      sawWhen = true
    elif upper.startsWith("REQUIRE "):
      discard
    elif upper.startsWith("STEP "):
      sawStep = true
    elif upper.startsWith("VERIFY "):
      sawVerify = true
    elif upper.startsWith("RECOVER "):
      discard
    else:
      errors.add("unsupported DSL statement: " & line)
  if not sawWhen: errors.add("missing WHEN statement")
  if not sawStep: errors.add("missing STEP statement")
  if not sawVerify: errors.add("missing VERIFY statement")
  (errors.len == 0, errors)

proc runRegressionGate(tenantId: string, candidateOverride: JsonNode = nil): Future[JsonNode] {.async.} =
  ensureDiagnosticSuite(tenantId)
  var tests = newJArray()
  var passed = 0
  var total = 0
  let diagnostics = store.query("SELECT * FROM diagnostics WHERE tenant_id=? ORDER BY created_at ASC", @[%tenantId])
  for diagnostic in diagnostics:
    let resultNode = await runDiagnosticRolloutCase(tenantId, diagnostic, candidateOverride)
    let ok = resultNode{"passed"}.getBool(false)
    inc total
    if ok: inc passed
    tests.add(%*{"name": "micro_rollout:" & diagnostic.getStr("domain"), "passed": ok, "executed": true, "result": resultNode})
  let stateProbe = defaultSigma("state regression")
  let patch = %*{"progress": 0.5, "facts": {"gate": true}}
  let patchValidation = validatePatch(patch)
  let merged = deepMerge(stateProbe, patch)
  let stateValidation = validateSigma(merged)
  let stateOk = patchValidation[0] and stateValidation[0] and merged{"progress"}.getFloat(0.0) == 0.5 and merged{"facts"}{"gate"}.getBool(false)
  inc total
  if stateOk: inc passed
  tests.add(%*{"name": "state_merge_validation", "passed": stateOk, "executed": true})
  let mathProbe = evalMathExpression("(17 * 6) + 5")
  let mathOk = mathProbe[0] and abs(mathProbe[1] - 107.0) < 1e-9
  inc total
  if mathOk: inc passed
  tests.add(%*{"name": "deterministic_math", "passed": mathOk, "executed": true})
  let score = if total > 0: passed.float / total.float else: 0.0
  return %*{"total": total, "passed": passed, "score": score, "tests": tests, "all_passed": passed == total}

proc coreRegressionPassed(report: JsonNode): bool =
  if report.isNil or report.kind != JObject or not report.hasKey("tests") or report["tests"].kind != JArray:
    return false
  var sawCore = false
  for test in report["tests"].elems:
    if test.kind != JObject:
      continue
    let name = test{"name"}.getStr("")
    if name.startsWith("micro_rollout:"):
      continue
    sawCore = true
    if not test{"passed"}.getBool(false):
      return false
  sawCore

proc validateAndActivate(gate: ValidationGate, tenantId: string, candidate: JsonNode): Future[JsonNode] {.async.} =
  if candidate.isNil or candidate.kind != JObject:
    return %*{"accepted": false, "error": "candidate must be an object"}
  let name = candidate{"name"}.getStr("").strip()
  let domain = candidate{"domain"}.getStr("general").strip()
  let trigger = candidate{"trigger_spec"}.getStr(candidate{"trigger"}.getStr("")).strip()
  let procedure = candidate{"procedure_spec"}.getStr(candidate{"procedure"}.getStr("")).strip()
  let skillCode = candidate{"skill_code"}.getStr("").strip()
  if name.len < 3 or trigger.len < 5 or procedure.len < 20 or skillCode.len < 20:
    return %*{"accepted": false, "error": "candidate is incomplete"}
  let dsl = validateSkillDsl(skillCode)
  if not dsl[0]:
    return %*{"accepted": false, "error": "invalid skill DSL", "validation_errors": dsl[1]}
  acquire(skillGateLock)
  if skillGateBusy:
    release(skillGateLock)
    return %*{"accepted": false, "error": "validation gate busy"}
  skillGateBusy = true
  release(skillGateLock)
  try:
    let before = await runRegressionGate(tenantId)
    let after = await runRegressionGate(tenantId, candidate)
    let accepted = coreRegressionPassed(after) and after{"all_passed"}.getBool(false) and after{"score"}.getFloat(0.0) + gate.epsilon >= before{"score"}.getFloat(0.0)
    if not accepted:
      return %*{"accepted": false, "before": before, "after": after, "reason": "candidate failed regression validation"}
    let preconditions = if candidate.hasKey("preconditions") and candidate["preconditions"].kind == JArray: canonical(candidate["preconditions"]) else: "[]"
    let postconditions = if candidate.hasKey("postconditions") and candidate["postconditions"].kind == JArray: canonical(candidate["postconditions"]) else: "[]"
    let failureModes = if candidate.hasKey("failure_modes") and candidate["failure_modes"].kind == JArray: canonical(candidate["failure_modes"]) else: "[]"
    let embedding = embToJson(textEmbedding(name & " " & domain & " " & trigger & " " & procedure & " " & skillCode))
    let existing = store.query("SELECT skill_id FROM skills WHERE tenant_id=? AND name=?", @[%tenantId, %name])
    let ts = nowF()
    var skillId = ""
    if existing.len > 0:
      skillId = existing[0].getStr("skill_id")
      discard store.exec("UPDATE skills SET domain=?,trigger_spec=?,procedure_spec=?,skill_code=?,preconditions_json=?,postconditions_json=?,failure_modes_json=?,version=version+1,active=1,embedding_json=?,updated_at=? WHERE skill_id=? AND tenant_id=?",
        @[%domain, %trigger, %procedure, %skillCode, %preconditions, %postconditions, %failureModes, %embedding, %ts, %skillId, %tenantId])
    else:
      skillId = newId("skill")
      discard store.exec("INSERT INTO skills (skill_id,tenant_id,name,domain,trigger_spec,procedure_spec,skill_code,preconditions_json,postconditions_json,failure_modes_json,version,active,success_count,failure_count,reward,embedding_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,1,1,0,0,0.0,?,?,?)",
        @[%skillId, %tenantId, %name, %domain, %trigger, %procedure, %skillCode, %preconditions, %postconditions, %failureModes, %embedding, %ts, %ts])
    return %*{"accepted": true, "skill_id": skillId, "before": before, "after": after}
  finally:
    acquire(skillGateLock)
    skillGateBusy = false
    release(skillGateLock)

proc consider(agent: MetaAgent, tenantId: string, sourceTaskId: string = ""): Future[JsonNode] {.async.} =
  let groups = store.query("SELECT failure_point,attribution,COUNT(*) AS occurrences,MAX(created_at) AS latest FROM reflections WHERE tenant_id=? GROUP BY failure_point,attribution HAVING COUNT(*)>=? ORDER BY latest DESC LIMIT ?", @[%tenantId, %agent.minOccurrences, %agent.maxCandidates])
  var decisions = newJArray()
  for group in groups:
    let failurePoint = group.getStr("failure_point").strip()
    let attribution = group.getStr("attribution").strip()
    let occurrences = group.getInt("occurrences", 0)
    if failurePoint.len == 0:
      continue
    let signature = sha1Hex(failurePoint.toLowerAscii() & "|" & attribution.toLowerAscii())
    let prior = store.query("SELECT occurrences,status FROM meta_agent_events WHERE tenant_id=? AND signature=?", @[%tenantId, %signature])
    if prior.len > 0 and occurrences <= prior[0].getInt("occurrences", 0):
      continue
    let evidenceRows = store.query("SELECT task_id,patch_json,failure_point,pivot_action,attribution,verifier_report_json FROM reflections WHERE tenant_id=? AND failure_point=? AND attribution=? ORDER BY created_at DESC LIMIT ?", @[%tenantId, %failurePoint, %attribution, %agent.lookback])
    var evidence = newJArray()
    for row in evidenceRows:
      evidence.add(%*{"task_id": row.getStr("task_id"), "failure_point": row.getStr("failure_point"), "pivot_action": row.getStr("pivot_action"), "attribution": row.getStr("attribution"), "patch": row.getJson("patch_json"), "verifier_report": row.getJson("verifier_report_json")})
    let response = await invokeModel(mrGpt6Astra, %*[
      {"role": "system", "content": promptText("meta_skill_synthesis")},
      {"role": "user", "content": "Source task: " & sourceTaskId & "\nFailure: " & failurePoint & "\nAttribution: " & attribution & "\nOccurrences: " & $occurrences & "\nEvidence:\n" & canonical(evidence)}
    ], true, tenantId, sourceTaskId)
    let candidate = parseJsonObjectLoose(response.content)
    var validation = %*{"accepted": false, "error": "candidate generation returned invalid JSON"}
    if not candidate.isNil:
      validation = await agent.gate.validateAndActivate(tenantId, candidate)
    let status = if validation{"accepted"}.getBool(false): "accepted" else: "rejected"
    let candidateText = if candidate.isNil: "{}" else: canonical(candidate)
    discard store.exec("INSERT INTO meta_agent_events (event_id,tenant_id,signature,occurrences,candidate_json,validation_json,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,signature) DO UPDATE SET occurrences=excluded.occurrences,candidate_json=excluded.candidate_json,validation_json=excluded.validation_json,status=excluded.status,updated_at=excluded.updated_at",
      @[%newId("meta"), %tenantId, %signature, %occurrences, %candidateText, %canonical(validation), %status, %nowF(), %nowF()])
    decisions.add(%*{"signature": signature, "status": status, "candidate": (if candidate.isNil: newJObject() else: candidate), "validation": validation})
  return %*{"tenant_id": tenantId, "decisions": decisions, "considered": groups.len}

proc upsertSeed(name, domain, trigger, procedure, skillCode: string) =
  let existing = store.query("SELECT skill_id FROM skills WHERE tenant_id=? AND name=?", @[%defaultTenantId, %name])
  let embedding = embToJson(textEmbedding(name & " " & domain & " " & trigger & " " & procedure & " " & skillCode))
  let ts = nowF()
  if existing.len > 0:
    discard store.exec("UPDATE skills SET domain=?,trigger_spec=?,procedure_spec=?,skill_code=?,embedding_json=?,active=1,updated_at=? WHERE skill_id=?",
      @[%domain, %trigger, %procedure, %skillCode, %embedding, %ts, %existing[0].getStr("skill_id")])
  else:
    discard store.exec("INSERT INTO skills (skill_id,tenant_id,name,domain,trigger_spec,procedure_spec,skill_code,reward,active,embedding_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,0.0,1,?,?,?)",
      @[%newId("skill"), %defaultTenantId, %name, %domain, %trigger, %procedure, %skillCode, %embedding, %ts, %ts])

proc consolidateKnowledgeOnce() {.async.} =
  let tenantId = defaultTenantId
  let reflections = store.query("SELECT failure_point,pivot_action,attribution,verifier_report_json,created_at FROM reflections WHERE tenant_id=? ORDER BY created_at DESC LIMIT 64", @[%tenantId])
  if reflections.len == 0:
    return
  var evidence = newJArray()
  for row in reflections:
    evidence.add(%*{"failure_point": row.getStr("failure_point"), "pivot_action": row.getStr("pivot_action"), "attribution": row.getStr("attribution"), "verifier_report": row.getJson("verifier_report_json"), "created_at": row.getFloat("created_at")})
  let response = await invokeModel(mrOrchestrator, %*[
    {"role": "system", "content": promptText("knowledge_consolidation")},
    {"role": "user", "content": "Reflections:\n" & canonical(evidence)}
  ], true, tenantId, "")
  let node = parseJsonObjectLoose(response.content)
  if node.isNil:
    return
  let slug = node{"slug"}.getStr("").strip()
  let body = node{"body"}.getStr("").strip()
  if slug.len == 0 or body.len == 0:
    return
  discard commitKnowledgeDoc(tenantId, slug, node{"category"}.getStr("operational"), body)
  discard await metaAgent.consider(tenantId)

proc knowledgeConsolidationLoop() {.async.} =
  while true:
    try:
      await consolidateKnowledgeOnce()
    except CatchableError:
      discard
    await sleepAsync(300000)

proc persistChatJobEvent(jobId: string, ev: JsonNode) =
  let eventType = ev{"type"}.getStr("")
  let contentDelta = ev{"content"}.getStr(ev{"delta"}.getStr(""))
  let reasoningDelta = ev{"reasoning_content"}.getStr(ev{"reasoning"}.getStr(""))
  let usage = if ev.hasKey("usage") and ev["usage"].kind == JObject: ev["usage"] else: newJObject()
  var ops: seq[SqlOperation] = @[]
  ops.add(SqlOperation(
    sql: "INSERT INTO chat_job_events (job_id, sequence, event_json, created_at) SELECT ?, COALESCE(MAX(sequence),0)+1, ?, ? FROM chat_job_events WHERE job_id=?",
    params: @[%jobId, %($ev), %nowF(), %jobId]))
  case eventType
  of "started":
    ops.add(SqlOperation(sql: "UPDATE chat_jobs SET status='running', updated_at=? WHERE job_id=?", params: @[%nowF(), %jobId]))
  of "delta":
    ops.add(SqlOperation(sql: "UPDATE chat_jobs SET content=content||?, reasoning=reasoning||?, updated_at=? WHERE job_id=?", params: @[%contentDelta, %reasoningDelta, %nowF(), %jobId]))
  of "usage":
    ops.add(SqlOperation(sql: "UPDATE chat_jobs SET prompt_tokens=?, completion_tokens=?, total_tokens=?, usage_json=?, updated_at=? WHERE job_id=?",
      params: @[%usage{"prompt_tokens"}.getInt(usage{"input_tokens"}.getInt(0)), %usage{"completion_tokens"}.getInt(usage{"output_tokens"}.getInt(0)), %usage{"total_tokens"}.getInt(0), %($usage), %nowF(), %jobId]))
  of "done":
    ops.add(SqlOperation(sql: "UPDATE chat_jobs SET status='succeeded', model=?, task_id=?, usage_json=?, updated_at=? WHERE job_id=?",
      params: @[%ev{"model"}.getStr(""), %ev{"task_id"}.getStr(""), %($usage), %nowF(), %jobId]))
  of "stopped":
    ops.add(SqlOperation(sql: "UPDATE chat_jobs SET status='stopped', error='', updated_at=? WHERE job_id=?", params: @[%nowF(), %jobId]))
  of "error":
    ops.add(SqlOperation(sql: "UPDATE chat_jobs SET status='failed', error=?, updated_at=? WHERE job_id=?", params: @[%ev{"message"}.getStr("chat job failed"), %nowF(), %jobId]))
  else:
    discard
  store.execTransaction(ops)

proc chooseSimpleSpecialist(route: RouteDecision): ModelRole = route.primaryModel

proc specialistChatMessages(role: ModelRole, messages: JsonNode): JsonNode =
  result = newJArray()
  result.add(%*{"role": "system", "content": specialistSystem(role)})
  if not messages.isNil and messages.kind == JArray:
    for message in messages.elems:
      result.add(copy(message))

proc routeNeedsExecution(route: RouteDecision): bool =
  if route.delegations.kind == JArray and route.delegations.elems.len > 0:
    return true
  if route.plan.kind == JArray and route.plan.elems.len > 0:
    return true
  route.requiresVm or route.requiresBrowser or route.requiresDesktop or route.requiresDocumentAnalysis or route.requiresVisualAnalysis

proc routingGoal(messages: JsonNode): string =
  var parts: seq[string] = @[]
  if messages.isNil or messages.kind != JArray:
    return ""
  for msg in messages.elems:
    if msg.kind != JObject:
      continue
    let text = openAiContent(msg)
    if text.len > 0:
      parts.add(msg{"role"}.getStr("user") & ": " & text)
    let content = msg{"content"}
    if content.kind == JArray:
      for item in content.elems:
        if item.kind != JObject:
          continue
        let typ = item{"type"}.getStr("")
        if typ in ["image", "image_url", "input_image"]:
          parts.add("[image input present]")
        elif typ in ["video", "video_url", "input_video"]:
          parts.add("[video input present]")
        elif typ in ["file", "document", "input_file"]:
          parts.add("[document input present: " & item{"name"}.getStr(item{"filename"}.getStr(item{"path"}.getStr(""))) & "]")
  parts.join("\n")

proc taskUsage(taskId: string): JsonNode =
  let rows = store.query("SELECT tokens_used FROM tasks WHERE task_id=?", @[%taskId])
  let total = if rows.len > 0: rows[0].getInt("tokens_used", 0) else: 0
  %*{"total_tokens": total}

proc directChat(messages: JsonNode, tenantId: string): Future[DirectChatResult] {.async.} =
  let goal = routingGoal(messages)
  let route = await routeTask(goal, defaultSigma(goal), %*{"status": "chat"}, tenantId, "")
  if routeNeedsExecution(route):
    let spec = %*{"goal": goal, "messages": copy(messages)}
    let h = createTask(goal, spec, tenantId)
    h.applyRoute(route)
    discard h.launchTask()
    let timeoutMs = positiveEnvInt("CHAT_SYNC_TIMEOUT_MS", 1_800_000)
    let started = getMonoTime()
    while true:
      if int((getMonoTime() - started).inMilliseconds) >= timeoutMs:
        acquire(h.lock)
        h.stopRequested = true
        h.status = "halted"
        h.terminalReason = "synchronous chat timeout"
        h.orchestratorState = osTerminal
        h.sigma["phase"] = %"terminal"
        release(h.lock)
        h.persistTask()
        raise newException(IOError, "synchronous routed chat timed out")
      await sleepAsync(250)
      acquire(h.lock)
      let status = h.status
      let obs = copy(h.obs)
      let reason = h.terminalReason
      release(h.lock)
      if status == "succeeded":
        return DirectChatResult(content: obs{"final"}.getStr(obs{"content"}.getStr("Task completed.")), reasoningContent: "", model: modelRoleName(route.primaryModel), usage: taskUsage(h.taskId), taskId: h.taskId)
      if status in ["failed", "halted", "stopped"]:
        raise newException(IOError, obs{"error"}.getStr(reason))
  let role = chooseSimpleSpecialist(route)
  let specialistMessages = specialistChatMessages(role, messages)
  let resp = await invokeModel(role, specialistMessages, false, tenantId, "")
  return DirectChatResult(content: resp.content, reasoningContent: resp.reasoningContent, model: modelRoleName(role), usage: copy(resp.usage), taskId: "")

proc createChatJob(requestNode: JsonNode, tenantId: string): string =
  let id = newId("chatjob")
  let ts = nowF()
  discard store.exec("INSERT INTO chat_jobs (job_id, tenant_id, request_json, status, created_at, updated_at) VALUES (?,?,?,'queued',?,?)",
    @[%id, %tenantId, %($requestNode), %ts, %ts])
  id

proc claimChatJob(jobId: string): bool =
  acquire(chatJobsLock)
  if jobId in activeChatJobs:
    release(chatJobsLock)
    return false
  activeChatJobs.incl(jobId)
  release(chatJobsLock)
  let rows = store.query("SELECT status FROM chat_jobs WHERE job_id=?", @[%jobId])
  if rows.len == 0 or rows[0].getStr("status") notin ["queued", "running"]:
    acquire(chatJobsLock)
    activeChatJobs.excl(jobId)
    release(chatJobsLock)
    return false
  true

proc releaseChatJob(jobId: string) =
  acquire(chatJobsLock)
  activeChatJobs.excl(jobId)
  release(chatJobsLock)

proc streamRequestyJob(jobId: string, role: ModelRole, messages: JsonNode, tenantId: string): Future[LlmResponse] {.async.} =
  let key = requireEnv("REQUESTY_API_KEY")
  let requestRows = store.query("SELECT request_json FROM chat_jobs WHERE job_id=? AND tenant_id=?", @[%jobId, %tenantId])
  if requestRows.len == 0:
    raise newException(IOError, "chat job not found for tenant")
  let requestNode = requestRows[0].getJson("request_json")
  let requestedMax = requestNode{"max_tokens"}.getInt(0)
  let requestedTemperature = requestNode{"temperature"}.getFloat(NaN)
  let requestedTopP = requestNode{"top_p"}.getFloat(NaN)
  var body = requestyBody(role, messages, false, true)
  if requestedMax > 0:
    body["max_tokens"] = %effectiveMaxTokens(requestedMax, "stream")
  if not requestedTemperature.isNaN:
    body["temperature"] = %requestedTemperature
  if not requestedTopP.isNaN:
    body["top_p"] = %requestedTopP
  var client = newAsyncHttpClient(maxRedirects = 0)
  defer: client.close()
  client.timeout = positiveEnvInt("MODEL_HTTP_TIMEOUT_MS", 600000)
  client.headers = newHttpHeaders({"Authorization": "Bearer " & key, "Content-Type": "application/json", "Accept": "text/event-stream"})
  let upstream = await client.request(RequestyBaseUrl & "/chat/completions", httpMethod = HttpPost, body = $body)
  if upstream.code.int < 200 or upstream.code.int >= 300:
    let (raw, _) = await readBoundedBody(upstream, positiveEnvInt("MODEL_MAX_RESPONSE_BYTES", 16_777_216))
    raise newException(IOError, "Requesty stream status " & $upstream.code.int & ": " & raw)
  var pending = ""
  var content = ""
  var reasoning = ""
  var usage = newJObject()
  var finishReason = ""
  var model = modelSpec(role).model
  var rawEvents = newJArray()
  var sawDone = false
  proc consume(eventText: string): bool =
    var dataLines: seq[string] = @[]
    for line in eventText.splitLines():
      if line.startsWith("data:"):
        dataLines.add(line[5 .. ^1].strip())
    if dataLines.len == 0:
      return false
    let data = dataLines.join("\n")
    if data == "[DONE]":
      sawDone = true
      return true
    let event = parseJson(data)
    rawEvents.add(copy(event))
    if event.hasKey("model"):
      model = event{"model"}.getStr(model)
    if event.hasKey("usage") and event["usage"].kind == JObject:
      usage = copy(event["usage"])
      persistChatJobEvent(jobId, %*{"type": "usage", "usage": usage})
    if event.hasKey("error"):
      let message = if event["error"].kind == JObject: event["error"]{"message"}.getStr($event["error"]) else: $event["error"]
      raise newException(IOError, message)
    if event.hasKey("choices") and event["choices"].kind == JArray and event["choices"].elems.len > 0:
      let choice = event["choices"][0]
      let delta = if choice.hasKey("delta") and choice["delta"].kind == JObject: choice["delta"] else: newJObject()
      let contentDelta = streamText(delta{"content"})
      let reasoningDelta = streamText(if delta.hasKey("reasoning_content"): delta["reasoning_content"] else: delta{"reasoning"})
      if contentDelta.len > 0: content.add(contentDelta)
      if reasoningDelta.len > 0: reasoning.add(reasoningDelta)
      if choice.hasKey("finish_reason") and choice["finish_reason"].kind == JString:
        finishReason = choice["finish_reason"].getStr("")
      if contentDelta.len > 0 or reasoningDelta.len > 0:
        var deltaEvent = %*{"type": "delta", "model": model}
        if contentDelta.len > 0: deltaEvent["content"] = %contentDelta
        if reasoningDelta.len > 0: deltaEvent["reasoning_content"] = %reasoningDelta
        if finishReason.len > 0: deltaEvent["finish_reason"] = %finishReason
        persistChatJobEvent(jobId, deltaEvent)
    false
  var ended = false
  while not ended:
    let stateRows = store.query("SELECT status FROM chat_jobs WHERE job_id=? AND tenant_id=?", @[%jobId, %tenantId])
    if stateRows.len == 0 or stateRows[0].getStr("status") in ["stopping", "stopped"]:
      raise newException(IOError, "chat job stopped")
    let readResult = await upstream.bodyStream.read()
    if not readResult[0]:
      break
    pending.add(readResult[1])
    pending = pending.replace("\r\n", "\n")
    while true:
      let separator = pending.find("\n\n")
      if separator < 0: break
      let eventText = pending[0 ..< separator]
      pending = if separator + 2 < pending.len: pending[separator + 2 .. ^1] else: ""
      if consume(eventText):
        ended = true
        break
  if pending.strip().len > 0 and not sawDone:
    discard consume(pending.replace("\r\n", "\n").strip())
  let promptTokens = usage{"prompt_tokens"}.getInt(usage{"input_tokens"}.getInt(0))
  let completionTokens = usage{"completion_tokens"}.getInt(usage{"output_tokens"}.getInt(0))
  let totalTokens = usage{"total_tokens"}.getInt(promptTokens + completionTokens)
  if totalTokens > 0 and not chargeTokens(tenantId, "", totalTokens):
    raise newException(IOError, "tenant token budget exhausted")
  if not sawDone and content.len == 0 and reasoning.len == 0:
    raise newException(IOError, "Requesty stream ended without model output")
  result = LlmResponse(content: content, reasoningContent: reasoning, raw: rawEvents, usage: usage, model: model, provider: pkRequesty, finishReason: finishReason, promptTokens: promptTokens, completionTokens: completionTokens, totalTokens: totalTokens, logprobs: @[])

proc runChatJob(jobId: string) {.async.} =
  if not claimChatJob(jobId):
    return
  try:
    let rows = store.query("SELECT request_json,tenant_id FROM chat_jobs WHERE job_id=?", @[%jobId])
    if rows.len == 0:
      return
    let tenantId = rows[0].getStr("tenant_id")
    let req = rows[0].getJson("request_json")
    let messages = req{"messages"}
    if messages.kind != JArray or messages.elems.len == 0:
      raise newException(ValueError, "messages array required")
    persistChatJobEvent(jobId, %*{"type": "started"})
    let goal = routingGoal(messages)
    let route = await routeTask(goal, defaultSigma(goal), %*{"status": "chat_job"}, tenantId, "")
    persistChatJobEvent(jobId, %*{"type": "route", "intent": route.intent, "model": modelRoleName(route.primaryModel), "route": route.raw})
    if routeNeedsExecution(route):
      let spec = %*{"goal": goal, "messages": copy(messages), "max_steps": req{"max_steps"}.getInt(0)}
      let h = createTask(goal, spec, tenantId)
      h.applyRoute(route)
      discard store.exec("UPDATE chat_jobs SET model=?, task_id=?, updated_at=? WHERE job_id=?", @[%modelRoleName(route.primaryModel), %h.taskId, %nowF(), %jobId])
      discard h.launchTask()
      var lastTaskSequence = 0'i64
      let timeoutMs = positiveEnvInt("CHAT_JOB_TIMEOUT_MS", 3_600_000)
      let started = getMonoTime()
      while true:
        if int((getMonoTime() - started).inMilliseconds) >= timeoutMs:
          acquire(h.lock)
          h.stopRequested = true
          h.status = "halted"
          h.terminalReason = "chat job timeout"
          h.orchestratorState = osTerminal
          h.sigma["phase"] = %"terminal"
          release(h.lock)
          h.persistTask()
          raise newException(IOError, "routed chat job timed out")
        for r in store.query("SELECT sequence,event_json FROM task_events WHERE task_id=? AND sequence>? ORDER BY sequence ASC", @[%h.taskId, %lastTaskSequence]):
          lastTaskSequence = r.getInt("sequence", lastTaskSequence)
          persistChatJobEvent(jobId, %*{"type": "agent_event", "task_id": h.taskId, "event": r.getJson("event_json")})
        acquire(h.lock)
        let status = h.status
        let obs = copy(h.obs)
        let terminalReason = h.terminalReason
        release(h.lock)
        if status == "succeeded":
          let content = obs{"final"}.getStr(obs{"content"}.getStr("Task completed."))
          if content.len > 0:
            persistChatJobEvent(jobId, %*{"type": "delta", "model": modelRoleName(route.primaryModel), "content": content})
          let usage = taskUsage(h.taskId)
          persistChatJobEvent(jobId, %*{"type": "usage", "usage": usage})
          persistChatJobEvent(jobId, %*{"type": "done", "model": modelRoleName(route.primaryModel), "task_id": h.taskId, "usage": usage, "finish_reason": "stop"})
          break
        if status in ["failed", "halted", "stopped"]:
          raise newException(IOError, obs{"error"}.getStr(terminalReason))
        let state = store.query("SELECT status FROM chat_jobs WHERE job_id=?", @[%jobId])
        if state.len == 0 or state[0].getStr("status") in ["stopping", "stopped"]:
          acquire(h.lock)
          h.stopRequested = true
          h.status = "halted"
          h.terminalReason = "chat job stopped"
          h.orchestratorState = osTerminal
          h.sigma["phase"] = %"terminal"
          release(h.lock)
          h.persistTask()
          raise newException(IOError, "chat job stopped")
        await sleepAsync(200)
    else:
      let role = chooseSimpleSpecialist(route)
      discard store.exec("UPDATE chat_jobs SET model=?, updated_at=? WHERE job_id=?", @[%modelRoleName(role), %nowF(), %jobId])
      let specialistMessages = specialistChatMessages(role, messages)
      var resp: LlmResponse
      if modelSpec(role).provider == pkRequesty:
        resp = await streamRequestyJob(jobId, role, specialistMessages, tenantId)
      else:
        resp = await invokeModel(role, specialistMessages, false, tenantId, "")
        if resp.reasoningContent.len > 0 or resp.content.len > 0:
          var deltaEvent = %*{"type": "delta", "model": modelRoleName(role)}
          if resp.content.len > 0: deltaEvent["content"] = %resp.content
          if resp.reasoningContent.len > 0: deltaEvent["reasoning_content"] = %resp.reasoningContent
          persistChatJobEvent(jobId, deltaEvent)
        if resp.usage.kind == JObject and resp.usage.len > 0:
          persistChatJobEvent(jobId, %*{"type": "usage", "usage": resp.usage})
      persistChatJobEvent(jobId, %*{"type": "done", "model": modelRoleName(role), "task_id": "", "usage": resp.usage, "finish_reason": (if resp.finishReason.len > 0: resp.finishReason else: "stop")})
  except CatchableError as e:
    try:
      let stateRows = store.query("SELECT status FROM chat_jobs WHERE job_id=?", @[%jobId])
      if stateRows.len == 0 or stateRows[0].getStr("status") notin ["stopping", "stopped"]:
        persistChatJobEvent(jobId, %*{"type": "error", "message": e.msg})
      elif stateRows[0].getStr("status") == "stopping":
        persistChatJobEvent(jobId, %*{"type": "stopped", "message": "stopped by client"})
    except CatchableError:
      discard
  finally:
    releaseChatJob(jobId)

proc launchChatJob(jobId: string): bool =
  acquire(chatJobsLock)
  let alreadyActive = jobId in activeChatJobs
  release(chatJobsLock)
  if alreadyActive:
    return false
  asyncCheck runChatJob(jobId)
  true

proc resumePendingChatJobs() =
  for r in store.query("SELECT job_id FROM chat_jobs WHERE status IN ('queued','running') ORDER BY created_at ASC"):
    discard launchChatJob(r.getStr("job_id"))

proc respondJson(req: Request, code: HttpCode, body: JsonNode): Future[void] {.async.} =
  let headers = newHttpHeaders({
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Api-Key"
  })
  await req.respond(code, $body, headers)

proc sendChunked(req: Request, payload: string): Future[void] {.async.} =
  if payload.len == 0:
    return
  await req.client.send(toHex(payload.len) & "\c\L" & payload & "\c\L")

proc handleTaskEvents(req: Request, taskId: string) {.async.} =
  let h = restoreTask(taskId)
  if h.isNil:
    await respondJson(req, Http404, %*{"error": "task not found"})
    return
  let headers = newHttpHeaders({
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
    "Transfer-Encoding": "chunked",
    "Access-Control-Allow-Origin": "*"
  })
  await req.client.send("HTTP/1.1 200 OK\c\L")
  await req.sendHeaders(headers)
  await req.client.send("\c\L")
  let past = store.query("SELECT event_json FROM task_events WHERE task_id=? ORDER BY sequence ASC", @[%taskId])
  for r in past:
    await sendChunked(req, "data: " & r.getStr("event_json") & "\c\L\c\L")
  var queue = initDeque[JsonNode]()
  var queueLock: Lock
  initLock(queueLock)
  var alive = true
  let subId = newId("sub")
  let cb = proc(ev: JsonNode) {.closure.} =
    acquire(queueLock)
    queue.addLast(copy(ev))
    release(queueLock)
  acquire(h.lock)
  h.subscribers.add((subId, cb))
  release(h.lock)
  try:
    while alive:
      await sleepAsync(200)
      var events: seq[JsonNode] = @[]
      acquire(queueLock)
      while queue.len > 0:
        events.add(queue.popFirst())
      release(queueLock)
      for ev in events:
        try:
          await sendChunked(req, "data: " & canonical(ev) & "\c\L\c\L")
        except CatchableError:
          alive = false
          break
      acquire(h.lock)
      let terminal = h.status in ["succeeded", "failed", "halted"]
      release(h.lock)
      if terminal and events.len == 0:
        break
  finally:
    acquire(h.lock)
    var nextSubs: seq[tuple[id: string, cb: proc(ev: JsonNode) {.closure.}]] = @[]
    for existing in h.subscribers:
      if existing.id != subId:
        nextSubs.add(existing)
    h.subscribers = nextSubs
    release(h.lock)
    deinitLock(queueLock)
    try:
      await req.client.send("0\c\L\c\L")
    except CatchableError:
      discard

proc handleChatJobEvents(req: Request, jobId: string) {.async.} =
  let rows = store.query("SELECT job_id FROM chat_jobs WHERE job_id=?", @[%jobId])
  if rows.len == 0:
    await respondJson(req, Http404, %*{"error": "chat job not found"})
    return
  var lastSeq = 0'i64
  let lastEventHeader = req.headers.getOrDefault("Last-Event-ID")
  if lastEventHeader.len > 0:
    try:
      lastSeq = max(0'i64, parseBiggestInt($lastEventHeader[0]).int64)
    except ValueError:
      lastSeq = 0
  let headers = newHttpHeaders({
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
    "Transfer-Encoding": "chunked",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Last-Event-ID"
  })
  try:
    await req.client.send("HTTP/1.1 200 OK\c\L")
    await req.sendHeaders(headers)
    await req.client.send("\c\L")
    await sendChunked(req, "data: " & canonical(%*{"type": "connected", "job_id": jobId}) & "\c\L\c\L")
    var lastPing = nowF()
    while not req.client.isClosed():
      let events = store.query("SELECT sequence, event_json FROM chat_job_events WHERE job_id=? AND sequence>? ORDER BY sequence ASC", @[%jobId, %lastSeq])
      for r in events:
        lastSeq = r.getInt("sequence", lastSeq)
        await sendChunked(req, "id: " & $lastSeq & "\c\Ldata: " & r.getStr("event_json") & "\c\L\c\L")
      let job = store.query("SELECT status FROM chat_jobs WHERE job_id=?", @[%jobId])
      let status = if job.len > 0: job[0].getStr("status") else: "failed"
      if status in ["succeeded", "failed", "stopped"] and events.len == 0:
        break
      if nowF() - lastPing >= 15.0:
        lastPing = nowF()
        await sendChunked(req, ": ping\c\L\c\L")
      await sleepAsync(200)
  except CatchableError:
    discard
  finally:
    try:
      await req.client.send("0\c\L\c\L")
    except CatchableError:
      discard

var sseClients {.threadvar.}: seq[SseClient]

proc pushSse(c: SseClient, payload: string) =
  if c.isNil or not c.alive:
    return
  acquire(c.lock)
  c.queue.addLast(payload)
  release(c.lock)

proc broadcastTenant(tenantId: string, ev: JsonNode) =
  let payload = "data: " & canonical(ev) & "\n\n"
  acquire(sseLock)
  var live: seq[SseClient] = @[]
  for client in sseClients:
    if client.alive:
      if client.tenantId == tenantId:
        pushSse(client, payload)
      live.add(client)
  sseClients = live
  release(sseLock)

proc attachBroadcast(h: TaskHandle) =
  if h.isNil:
    return
  acquire(h.lock)
  if not h.broadcastAttached:
    let tenant = if h.tenantId.len > 0: h.tenantId else: defaultTenantId
    h.subscribers.add((id: "broadcast:" & h.taskId, cb: proc(ev: JsonNode) {.closure.} = broadcastTenant(tenant, ev)))
    h.broadcastAttached = true
  release(h.lock)

proc cookieValue(req: Request, name: string): string =
  let raw = req.headers.getOrDefault("Cookie")
  for part in raw.split(';'):
    let pair = part.strip()
    let idx = pair.find('=')
    if idx > 0 and pair[0 ..< idx].strip() == name:
      return decodeUrl(pair[idx + 1 .. ^1].strip())
  ""

proc createBrowserSession(tenantId: string): string =
  let token = newId("session") & newId("token")
  let sessionId = newId("browser")
  let lifetime = positiveEnvInt("BROWSER_SESSION_SECONDS", 2_592_000)
  let now = nowF()
  discard store.exec("DELETE FROM browser_sessions WHERE expires_at<=?", @[%now])
  discard store.exec("INSERT INTO browser_sessions (session_id,tenant_id,token_hash,expires_at,created_at) VALUES (?,?,?,?,?)", @[%sessionId, %tenantId, %sha1Hex(token), %(now + float(lifetime)), %now])
  token

proc sessionCookie(token: string): string =
  let lifetime = positiveEnvInt("BROWSER_SESSION_SECONDS", 2_592_000)
  var value = "agent_session=" & encodeUrl(token) & "; Path=/; HttpOnly; SameSite=Strict; Max-Age=" & $lifetime
  if getEnv("AGENT_COOKIE_SECURE", "").strip().toLowerAscii() in ["1", "true", "yes", "on"]:
    value.add("; Secure")
  value

proc authenticate(req: Request): Option[Row] =
  var credential = ""
  let auth = req.headers.getOrDefault("Authorization").strip()
  if auth.toLowerAscii().startsWith("bearer ") and auth.len > 7:
    credential = auth[7 .. ^1].strip()
  if credential.len == 0:
    credential = req.headers.getOrDefault("X-Api-Key").strip()
  if credential.len > 0:
    let hash = sha1Hex(credential)
    let rows = store.query("SELECT * FROM tenants WHERE api_key_hash=? AND api_key_hash<>'' LIMIT 1", @[%hash])
    if rows.len > 0:
      return some(rows[0])
  let sessionToken = cookieValue(req, "agent_session")
  if sessionToken.len > 0:
    let rows = store.query("SELECT t.* FROM browser_sessions s JOIN tenants t ON t.tenant_id=s.tenant_id WHERE s.token_hash=? AND s.expires_at>? LIMIT 1", @[%sha1Hex(sessionToken), %nowF()])
    if rows.len > 0:
      return some(rows[0])
  none(Row)

proc ensureDefaultTenant(): Row =
  let tenantId = ensureLocalTenant()
  let configuredKey = getEnv("AGENT_API_KEY", "").strip()
  if configuredKey.len > 0:
    discard store.exec("UPDATE tenants SET api_key_hash=? WHERE tenant_id=?", @[%sha1Hex(configuredKey), %tenantId])
  let rows = store.query("SELECT * FROM tenants WHERE tenant_id=?", @[%tenantId])
  if rows.len == 0:
    raise newException(DbError, "default tenant could not be created")
  let allowedRaw = rows[0].getStr("allowed_tools", "[]")
  var allowed = parseJson(allowedRaw)
  if allowed.kind != JArray:
    raise newException(DbError, "default tenant allowed_tools is invalid")
  if allowed.elems.len == 0:
    var tools = newJArray()
    for name, _ in toolRegistry:
      tools.add(%name)
    discard store.exec("UPDATE tenants SET allowed_tools=? WHERE tenant_id=? AND allowed_tools='[]'", @[%canonical(tools), %tenantId])
  let fresh = store.query("SELECT * FROM tenants WHERE tenant_id=?", @[%tenantId])
  if fresh.len == 0:
    raise newException(DbError, "default tenant could not be loaded")
  fresh[0]

proc handleSse(req: Request, tenantId: string) {.async.} =
  let client = SseClient(req: req, tenantId: tenantId, alive: true, queue: initDeque[string]())
  initLock(client.lock)
  acquire(sseLock)
  sseClients.add(client)
  release(sseLock)
  let headers = newHttpHeaders({
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
    "Transfer-Encoding": "chunked",
    "Access-Control-Allow-Origin": "*"
  })
  try:
    await req.client.send("HTTP/1.1 200 OK\c\L")
    await req.sendHeaders(headers)
    await req.client.send("\c\L")
    await sendChunked(req, "data: " & canonical(%*{"type": "connected", "tenant_id": tenantId, "time": nowF()}) & "\c\L\c\L")
    var lastPing = nowF()
    while client.alive and not req.client.isClosed():
      var batch: seq[string] = @[]
      acquire(client.lock)
      while client.queue.len > 0:
        batch.add(client.queue.popFirst())
      release(client.lock)
      for payload in batch:
        await sendChunked(req, payload.replace("\n", "\c\L"))
      if nowF() - lastPing > 15.0:
        lastPing = nowF()
        await sendChunked(req, ": ping\c\L\c\L")
      await sleepAsync(100)
  except CatchableError:
    discard
  finally:
    client.alive = false
    acquire(sseLock)
    var live: seq[SseClient] = @[]
    for item in sseClients:
      if item != client:
        live.add(item)
    sseClients = live
    release(sseLock)
    try:
      await req.client.send("0\c\L\c\L")
    except CatchableError:
      discard

proc sendChatStreamEvent(req: Request, event: JsonNode): Future[void] {.async.} =
  await sendChunked(req, "data: " & canonical(event) & "\c\L\c\L")

proc stopTaskTree(taskId: string)

proc streamChatCompletionsAsync(req: Request, messages: JsonNode,
                                maxTokens: int = 0,
                                temperature: float = 0.96,
                                topP: float = 1.0,
                                tenantId: string = "local"): Future[void] {.async.} =
  let requestNode = %*{"messages": copy(messages), "stream": true, "max_tokens": maxTokens, "temperature": temperature, "top_p": topP}
  let jobId = createChatJob(requestNode, tenantId)
  discard launchChatJob(jobId)
  let headers = newHttpHeaders({
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
    "Transfer-Encoding": "chunked"
  })
  await req.client.send("HTTP/1.1 200 OK\c\L")
  await req.sendHeaders(headers)
  await req.client.send("\c\L")
  await sendChatStreamEvent(req, %*{"type": "meta", "job_id": jobId})
  var lastSequence = 0'i64
  var finished = false
  var model = ""
  var lastPing = nowF()
  try:
    while not finished:
      let rows = store.query("SELECT sequence,event_json FROM chat_job_events WHERE job_id=? AND sequence>? ORDER BY sequence ASC", @[%jobId, %lastSequence])
      for row in rows:
        lastSequence = row.getInt("sequence", lastSequence)
        let event = row.getJson("event_json")
        let eventType = event{"type"}.getStr("")
        if event.hasKey("model"):
          model = event{"model"}.getStr(model)
        case eventType
        of "delta":
          var delta = newJObject()
          let content = event{"content"}.getStr(event{"delta"}.getStr(""))
          let reasoning = event{"reasoning_content"}.getStr(event{"reasoning"}.getStr(""))
          if content.len > 0: delta["content"] = %content
          if reasoning.len > 0: delta["reasoning_content"] = %reasoning
          let chunk = %*{"id": jobId, "object": "chat.completion.chunk", "created": getTime().toUnix(), "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": newJNull()}]}
          await sendChatStreamEvent(req, chunk)
        of "agent_event", "route", "usage", "started":
          await sendChatStreamEvent(req, event)
        of "stopped":
          await sendChatStreamEvent(req, %*{"type": "stopped", "job_id": jobId})
          await sendChunked(req, "data: [DONE]\c\L\c\L")
          finished = true
        of "done":
          let finishReason = event{"finish_reason"}.getStr("stop")
          let chunk = %*{"id": jobId, "object": "chat.completion.chunk", "created": getTime().toUnix(), "model": event{"model"}.getStr(model), "choices": [{"index": 0, "delta": newJObject(), "finish_reason": finishReason}], "usage": (if event.hasKey("usage"): event["usage"] else: newJObject())}
          await sendChatStreamEvent(req, chunk)
          await sendChatStreamEvent(req, %*{"type": "done", "job_id": jobId, "task_id": event{"task_id"}.getStr(""), "usage": (if event.hasKey("usage"): event["usage"] else: newJObject())})
          await sendChunked(req, "data: [DONE]\c\L\c\L")
          finished = true
        of "error":
          await sendChatStreamEvent(req, %*{"type": "error", "message": event{"message"}.getStr("chat job failed"), "job_id": jobId})
          await sendChunked(req, "data: [DONE]\c\L\c\L")
          finished = true
        else:
          discard
      if not finished:
        let stateRows = store.query("SELECT status,error FROM chat_jobs WHERE job_id=? AND tenant_id=?", @[%jobId, %tenantId])
        if stateRows.len == 0:
          await sendChatStreamEvent(req, %*{"type": "error", "message": "chat job disappeared", "job_id": jobId})
          await sendChunked(req, "data: [DONE]\c\L\c\L")
          finished = true
        elif stateRows[0].getStr("status") == "failed":
          await sendChatStreamEvent(req, %*{"type": "error", "message": stateRows[0].getStr("error", "chat job failed"), "job_id": jobId})
          await sendChunked(req, "data: [DONE]\c\L\c\L")
          finished = true
        elif stateRows[0].getStr("status") == "stopped":
          await sendChatStreamEvent(req, %*{"type": "stopped", "job_id": jobId})
          await sendChunked(req, "data: [DONE]\c\L\c\L")
          finished = true
      if not finished and nowF() - lastPing > 15.0:
        lastPing = nowF()
        await sendChunked(req, ": ping\c\L\c\L")
      if not finished:
        await sleepAsync(100)
  except CatchableError:
    try:
      let rows = store.query("SELECT task_id,status FROM chat_jobs WHERE job_id=? AND tenant_id=?", @[%jobId, %tenantId])
      if rows.len > 0 and rows[0].getStr("status") in ["queued", "running"]:
        discard store.exec("UPDATE chat_jobs SET status='stopping',updated_at=? WHERE job_id=? AND tenant_id=?", @[%nowF(), %jobId, %tenantId])
        let taskId = rows[0].getStr("task_id")
        if taskId.len > 0:
          stopTaskTree(taskId)
        persistChatJobEvent(jobId, %*{"type": "stopped", "message": "stream disconnected"})
    except CatchableError:
      discard
  finally:
    try:
      await req.client.send("0\c\L\c\L")
    except CatchableError:
      discard

proc mimeForPath(path: string): string =
  let ext = splitFile(path).ext.toLowerAscii()
  case ext
  of ".html": "text/html; charset=utf-8"
  of ".webmanifest", ".json": "application/manifest+json; charset=utf-8"
  of ".js": "text/javascript; charset=utf-8"
  of ".css": "text/css; charset=utf-8"
  of ".svg": "image/svg+xml"
  of ".png": "image/png"
  of ".jpg", ".jpeg": "image/jpeg"
  of ".webp": "image/webp"
  of ".ico": "image/x-icon"
  else: "application/octet-stream"

proc staticPath(urlPath: string): string =
  var rel = urlPath
  if rel == "/": rel = "/index.html"
  rel = decodeUrl(rel)
  if '\0' in rel or rel.contains("..") or rel.contains('\\'):
    raise newException(ValueError, "invalid static path")
  while rel.startsWith("/"):
    rel = rel[1 .. ^1]
  let base = absolutePath(PublicRoot)
  let candidate = absolutePath(base / rel)
  if not (candidate == base or candidate.startsWith(base & DirSep)):
    raise newException(ValueError, "static path escapes public root")
  candidate

proc serveStatic(req: Request, urlPath: string, issueSession: bool): Future[bool] {.async.} =
  var path: string
  try:
    path = staticPath(urlPath)
  except CatchableError:
    return false
  if not fileExists(path):
    return false
  let maxBytes = positiveEnvInt("STATIC_MAX_BYTES", 33_554_432)
  let size = getFileSize(path)
  if size < 0 or size > maxBytes:
    return false
  var headers = newHttpHeaders({"Content-Type": mimeForPath(path), "Cache-Control": (if urlPath in ["/", "/index.html"]: "no-cache" else: "public, max-age=86400")})
  if issueSession:
    let token = createBrowserSession(defaultTenantId)
    headers["Set-Cookie"] = sessionCookie(token)
  await req.respond(Http200, readFile(path), headers)
  return true

proc stopTaskTree(taskId: string) =
  let h = restoreTask(taskId)
  if not h.isNil:
    acquire(h.lock)
    h.stopRequested = true
    if h.status notin ["succeeded", "failed", "halted", "stopped"]:
      h.status = "halted"
    h.verified = false
    h.terminalReason = "stopped by client"
    h.orchestratorState = osTerminal
    h.sigma["phase"] = %"terminal"
    release(h.lock)
    h.persistTask()
  discard store.exec("UPDATE subagents SET stop_requested=1,status=CASE WHEN status IN ('succeeded','failed','stopped') THEN status ELSE 'stopping' END,updated_at=? WHERE task_id=?", @[%nowF(), %taskId])
  acquire(subAgentsLock)
  for _, agent in activeSubAgents.mpairs:
    if agent.taskId == taskId:
      acquire(agent.lock)
      if agent.status notin ["succeeded", "failed", "stopped"]:
        agent.stopRequested = true
        agent.status = "stopping"
      release(agent.lock)
  release(subAgentsLock)

proc handleHttpRequest(req: Request) {.async, gcsafe.} =
  let path = req.url.path
  if req.reqMethod == HttpOptions:
    let headers = newHttpHeaders({"Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Api-Key"})
    await req.respond(Http204, "", headers)
    return
  if req.reqMethod == HttpGet and (path in ["/", "/index.html"] or path == "/manifest.webmanifest" or path.startsWith("/icons/") or path.startsWith("/splash/")):
    if await serveStatic(req, path, path in ["/", "/index.html"]):
      return
    await respondJson(req, Http404, %*{"error": "static asset not found"})
    return
  if path == "/api/health" and req.reqMethod == HttpGet:
    let dbReady = not store.isNil and not store.handle.isNil
    await respondJson(req, Http200, %*{"ok": dbReady, "status": (if dbReady: "ready" else: "not_ready"), "time": nowF(), "models": {"orchestrator": CerebrasGemma4Model, "gpt6_astra": Gpt6AstraModel, "glm52": Glm52Model, "gemini38": Gemini38Model, "minimax_m3": MiniMaxM3Model, "grok43": Grok43Model}})
    return
  let auth = authenticate(req)
  if auth.isNone:
    await respondJson(req, Http401, %*{"error": "authentication required"})
    return
  let tenant = auth.get()
  let tenantId = tenant.getStr("tenant_id")

  if path == "/api/diagnostics/run" and req.reqMethod == HttpPost:
    try:
      let report = await runRegressionGate(tenantId)
      await respondJson(req, Http200, report)
    except CatchableError as e:
      await respondJson(req, Http500, %*{"error": e.msg})
    return
  if path == "/api/chat" and req.reqMethod == HttpPost:
    var body: JsonNode
    try:
      body = parseJson(req.body)
    except CatchableError:
      await respondJson(req, Http400, %*{"error": {"message": "invalid JSON body"}})
      return
    if not body.hasKey("messages") or body["messages"].kind != JArray or body["messages"].elems.len == 0:
      await respondJson(req, Http400, %*{"error": {"message": "messages array required"}})
      return
    if body{"stream"}.getBool(false):
      await streamChatCompletionsAsync(req, body["messages"], body{"max_tokens"}.getInt(0), body{"temperature"}.getFloat(0.96), body{"top_p"}.getFloat(1.0), tenantId)
      return
    try:
      let chat = await directChat(body["messages"], tenantId)
      var msg = %*{"role": "assistant", "content": chat.content}
      if chat.reasoningContent.len > 0:
        msg["reasoning_content"] = %chat.reasoningContent
      await respondJson(req, Http200, %*{
        "id": newId("chatcmpl"),
        "object": "chat.completion",
        "created": getTime().toUnix(),
        "model": chat.model,
        "choices": [{"index": 0, "message": msg, "finish_reason": "stop"}],
        "usage": chat.usage,
        "task_id": chat.taskId
      })
    except CatchableError as e:
      await respondJson(req, Http502, %*{"error": {"message": e.msg}})
    return
  let chatJobPrefix = "/api/chat/jobs/"
  if path.startsWith(chatJobPrefix):
    let rest = path[chatJobPrefix.len .. ^1]
    let parts = rest.split('/')
    if parts.len == 2 and parts[1] == "events" and req.reqMethod == HttpGet:
      let owned = store.query("SELECT job_id FROM chat_jobs WHERE job_id=? AND tenant_id=?", @[%parts[0], %tenantId])
      if owned.len == 0:
        await respondJson(req, Http404, %*{"error": "chat job not found"})
        return
      await handleChatJobEvents(req, parts[0])
      return
    if parts.len == 2 and parts[1] == "stop" and req.reqMethod == HttpPost:
      let rows = store.query("SELECT task_id,status FROM chat_jobs WHERE job_id=? AND tenant_id=?", @[%parts[0], %tenantId])
      if rows.len == 0:
        await respondJson(req, Http404, %*{"error": "chat job not found"})
        return
      let taskId = rows[0].getStr("task_id")
      discard store.exec("UPDATE chat_jobs SET status='stopping',updated_at=? WHERE job_id=? AND tenant_id=? AND status IN ('queued','running')", @[%nowF(), %parts[0], %tenantId])
      if taskId.len > 0:
        stopTaskTree(taskId)
      persistChatJobEvent(parts[0], %*{"type": "stopped", "message": "stopped by client"})
      await respondJson(req, Http200, %*{"job_id": parts[0], "status": "stopped", "task_id": taskId})
      return
    if parts.len == 1 and req.reqMethod == HttpGet:
      let rows = store.query("SELECT * FROM chat_jobs WHERE job_id=? AND tenant_id=?", @[%parts[0], %tenantId])
      if rows.len == 0:
        await respondJson(req, Http404, %*{"error": "chat job not found"})
        return
      let r = rows[0]
      await respondJson(req, Http200, %*{"job_id": r.getStr("job_id"), "status": r.getStr("status"), "content": r.getStr("content"), "reasoning_content": r.getStr("reasoning"), "error": r.getStr("error"), "model": r.getStr("model"), "task_id": r.getStr("task_id"), "usage": r.getJson("usage_json"), "prompt_tokens": r.getInt("prompt_tokens"), "completion_tokens": r.getInt("completion_tokens"), "total_tokens": r.getInt("total_tokens"), "created_at": r.getFloat("created_at"), "updated_at": r.getFloat("updated_at")})
      return

  if path == "/api/events" and req.reqMethod == HttpGet:
    await handleSse(req, tenantId)
    return
  let artifactPrefix = "/api/artifacts/"
  if path.startsWith(artifactPrefix) and req.reqMethod == HttpGet:
    let artifactId = path[artifactPrefix.len .. ^1]
    let rows = store.query("SELECT a.* FROM artifacts a JOIN tasks t ON t.task_id=a.task_id WHERE a.artifact_id=? AND t.tenant_id=?", @[%artifactId, %tenantId])
    if rows.len == 0:
      await respondJson(req, Http404, %*{"error": "artifact not found"})
      return
    let r = rows[0]
    let artifactPath = absolutePath(r.getStr("path"))
    let artifactRoot = absolutePath(WorkspaceRoot / "artifacts")
    if not artifactPath.startsWith(artifactRoot & DirSep) or not fileExists(artifactPath):
      await respondJson(req, Http404, %*{"error": "artifact file not found"})
      return
    let maxBytes = positiveEnvInt("ARTIFACT_DOWNLOAD_MAX_BYTES", 268_435_456)
    if getFileSize(artifactPath) > maxBytes:
      await respondJson(req, Http413, %*{"error": "artifact exceeds download limit"})
      return
    let headers = newHttpHeaders({"Content-Type": r.getStr("mime_type", "application/octet-stream"), "Content-Disposition": "attachment; filename=\"" & r.getStr("name").replace("\"", "") & "\"", "Cache-Control": "private, no-store"})
    await req.respond(Http200, readFile(artifactPath), headers)
    return

  if path == "/api/runs" and req.reqMethod == HttpPost:
    var body: JsonNode
    try:
      body = parseJson(req.body)
    except CatchableError:
      await respondJson(req, Http400, %*{"error": "invalid JSON body"})
      return
    let title = body{"title"}.getStr(body{"goal"}.getStr(body{"message"}.getStr("autonomous task")))
    let spec = if body.hasKey("spec") and body["spec"].kind == JObject: copy(body["spec"]) else: %*{"goal": title}
    let h = createTask(title, spec, tenantId)
    discard h.launchTask()
    await respondJson(req, Http201, %*{"task_id": h.taskId, "status": h.status, "goal": title})
    return
  if path == "/api/runs" and req.reqMethod == HttpGet:
    var arr = newJArray()
    for r in store.query("SELECT * FROM tasks WHERE tenant_id=? ORDER BY updated_at DESC", @[%tenantId]):
      arr.add(%*{
        "task_id": r.getStr("task_id"),
        "title": r.getStr("title"),
        "status": r.getStr("status"),
        "step_index": r.getInt("step_index"),
        "max_steps": r.getInt("max_steps"),
        "tokens_used": r.getInt("tokens_used"),
        "verified": r.getInt("verified") == 1,
        "created_at": r.getFloat("created_at"),
        "updated_at": r.getFloat("updated_at")
      })
    await respondJson(req, Http200, %*{"runs": arr})
    return
  if path.startsWith("/api/runs/"):
    let rest = path[10 .. ^1]
    let parts = rest.split('/')
    let taskId = parts[0]
    let h = restoreTask(taskId)
    if h.isNil or h.tenantId != tenantId:
      await respondJson(req, Http404, %*{"error": "task not found"})
      return
    if parts.len == 1 and req.reqMethod == HttpGet:
      let taskRows = store.query("SELECT step_index,max_steps,tokens_used FROM tasks WHERE task_id=?", @[%taskId])
      let persistedStep = if taskRows.len > 0: taskRows[0].getInt("step_index") else: 0
      let persistedMax = if taskRows.len > 0: taskRows[0].getInt("max_steps") else: 0
      let persistedTokens = if taskRows.len > 0: taskRows[0].getInt("tokens_used") else: 0
      acquire(h.lock)
      let payload = %*{
        "task_id": h.taskId,
        "title": h.title,
        "status": h.status,
        "step_index": persistedStep,
        "max_steps": persistedMax,
        "tokens_used": persistedTokens,
        "verified": h.verified,
        "terminal_reason": h.terminalReason,
        "paused": h.paused,
        "transition_busy": h.transitionBusy,
        "loop_active": h.loopActive.load(moAcquire),
        "sigma": copy(h.sigma),
        "obs": copy(h.obs)
      }
      release(h.lock)
      await respondJson(req, Http200, payload)
      return
    if parts.len == 2 and parts[1] == "events" and req.reqMethod == HttpGet:
      await handleTaskEvents(req, taskId)
      return
    if parts.len == 2 and parts[1] == "stop" and req.reqMethod == HttpPost:
      stopTaskTree(taskId)
      h.checkpoint(newJObject(), %*{"operator": "stop"})
      h.emit(%*{"type": "done", "status": "halted", "verified": false, "reason": "stopped by operator"})
      await respondJson(req, Http200, %*{"task_id": taskId, "status": "halted"})
      return
    if parts.len == 2 and parts[1] == "pause" and req.reqMethod == HttpPost:
      acquire(h.lock)
      h.paused = true
      let status = h.status
      release(h.lock)
      h.persistTask()
      await respondJson(req, Http200, %*{"task_id": taskId, "status": status, "paused": true})
      return
    if parts.len == 2 and parts[1] == "resume" and req.reqMethod == HttpPost:
      acquire(h.lock)
      let terminal = h.status in ["succeeded"] and h.verified
      if not terminal:
        h.paused = false
        h.stopRequested = false
        h.status = "running"
        h.terminalReason = ""
        if h.sigma{"phase"}.getStr("") == "terminal": h.sigma["phase"] = %"perceive"
        h.orchestratorState = osPerceive
      release(h.lock)
      if terminal:
        await respondJson(req, Http409, %*{"error": "task is already completed", "task_id": taskId})
        return
      h.persistTask()
      discard h.launchTask()
      await respondJson(req, Http200, %*{"task_id": taskId, "status": h.status, "paused": false, "loop_active": h.loopActive.load(moAcquire)})
      return
    if parts.len == 2 and parts[1] == "message" and req.reqMethod == HttpPost:
      var body: JsonNode
      try:
        body = parseJson(req.body)
      except CatchableError:
        await respondJson(req, Http400, %*{"error": "invalid JSON body"})
        return
      let msg = body{"message"}.getStr(body{"content"}.getStr(""))
      if msg.len == 0:
        await respondJson(req, Http400, %*{"error": "message required"})
        return
      acquire(h.lock)
      h.obs["operator_message"] = %msg
      h.obs["operator_message_at"] = %nowF()
      h.sigma["route"] = newJObject()
      h.paused = false
      h.stopRequested = false
      if h.status in ["halted", "failed", "queued"]: h.status = "running"
      h.orchestratorState = osPerceive
      h.sigma["phase"] = %"perceive"
      release(h.lock)
      h.persistTask()
      discard h.launchTask()
      await respondJson(req, Http200, %*{"task_id": taskId, "injected": true, "status": h.status})
      return
    if parts.len == 2 and parts[1] == "traces" and req.reqMethod == HttpGet:
      var arr = newJArray()
      for r in store.query("SELECT * FROM raw_traces WHERE task_id=? ORDER BY step_index ASC,created_at ASC", @[%taskId]):
        arr.add(%*{
          "trace_id": r.getStr("trace_id"),
          "task_id": r.getStr("task_id"),
          "tenant_id": r.getStr("tenant_id"),
          "step_index": r.getInt("step_index"),
          "initial_state": r.getJson("initial_state_json"),
          "skill_id": r.getStr("skill_id"),
          "action": r.getJson("action_json"),
          "obs": r.getJson("obs_json"),
          "delta": r.getJson("delta_json"),
          "post_state": r.getJson("post_state_json"),
          "success": r.getInt("success") == 1,
          "latency_ms": r.getInt("latency_ms"),
          "receipt": r.getJson("receipt_json"),
          "immutable_hash": r.getStr("immutable_hash"),
          "created_at": r.getFloat("created_at")
        })
      await respondJson(req, Http200, %*{"traces": arr})
      return
    if parts.len == 2 and parts[1] == "checkpoints" and req.reqMethod == HttpGet:
      var arr = newJArray()
      for r in store.query("SELECT * FROM checkpoints WHERE task_id=? ORDER BY step_index ASC", @[%taskId]):
        arr.add(%*{"checkpoint_id": r.getInt("ckpt_id"), "step_index": r.getInt("step_index"), "state": r.getJson("state_json"), "obs": r.getJson("obs_json"), "action": r.getJson("action_json"), "patch": r.getJson("patch_json"), "receipt": r.getJson("receipt_json"), "digest": r.getStr("digest"), "created_at": r.getFloat("created_at")})
      await respondJson(req, Http200, %*{"checkpoints": arr})
      return
    if parts.len == 2 and parts[1] == "artifacts" and req.reqMethod == HttpGet:
      var arr = newJArray()
      for r in store.query("SELECT * FROM artifacts WHERE task_id=? ORDER BY created_at ASC", @[%taskId]):
        arr.add(%*{"artifact_id": r.getStr("artifact_id"), "name": r.getStr("name"), "path": r.getStr("path"), "kind": r.getStr("kind"), "mime_type": r.getStr("mime_type"), "metadata": r.getJson("metadata_json"), "created_at": r.getFloat("created_at")})
      await respondJson(req, Http200, %*{"artifacts": arr})
      return
  if path == "/api/tools" and req.reqMethod == HttpGet:
    await respondJson(req, Http200, %*{"tools": toolCatalog()})
    return
  if path == "/api/skills" and req.reqMethod == HttpGet:
    var arr = newJArray()
    for r in store.query("SELECT * FROM skills WHERE tenant_id=? AND active=1 ORDER BY reward DESC, updated_at DESC", @[%tenantId]):
      arr.add(%*{"skill_id": r.getStr("skill_id"), "name": r.getStr("name"), "domain": r.getStr("domain"), "trigger": r.getStr("trigger_spec"), "procedure": r.getStr("procedure_spec"), "skill_code": r.getStr("skill_code"), "reward": r.getFloat("reward")})
    await respondJson(req, Http200, %*{"skills": arr})
    return
  if path == "/api/skills" and req.reqMethod == HttpPost:
    var body: JsonNode
    try:
      body = parseJson(req.body)
    except CatchableError:
      await respondJson(req, Http400, %*{"error": "invalid JSON body"})
      return
    let name = body{"name"}.getStr("").strip()
    let trigger = body{"trigger_spec"}.getStr(body{"trigger"}.getStr(""))
    let procedure = body{"procedure_spec"}.getStr(body{"procedure"}.getStr(""))
    let skillCode = body{"skill_code"}.getStr("")
    if name.len == 0 or trigger.len == 0 or procedure.len == 0 or skillCode.len == 0:
      await respondJson(req, Http400, %*{"error": "name, trigger_spec, procedure_spec and skill_code are required"})
      return
    let id = newId("skill")
    let ts = nowF()
    discard store.exec("INSERT INTO skills (skill_id, tenant_id, name, domain, trigger_spec, procedure_spec, skill_code, reward, active, created_at, updated_at) VALUES (?,?,?,?,?,?,?,0.0,1,?,?) ON CONFLICT(tenant_id,name) DO UPDATE SET domain=excluded.domain,trigger_spec=excluded.trigger_spec,procedure_spec=excluded.procedure_spec,skill_code=excluded.skill_code,active=1,updated_at=excluded.updated_at",
      @[%id, %tenantId, %name, %body{"domain"}.getStr("general"), %trigger, %procedure, %skillCode, %ts, %ts])
    await respondJson(req, Http201, %*{"accepted": true, "name": name})
    return
  await respondJson(req, Http404, %*{"error": "endpoint not found"})

proc seedDefaultSkills() =
  let seeds = @[
    ("filesystem_operations", "filesystem", "task modifies or verifies files", "Inspect actual files, perform the required mutation, then re-read or execute to establish the resulting state.", "WHEN workspace_file_change\nREQUIRE actual_file_state\nSTEP inspect\nSTEP modify\nVERIFY reread_or_execute\nRECOVER inspect_failure_and_repair"),
    ("iterative_code_repair", "coding", "generated or existing code fails", "Execute the real program, read the exact failure, modify the same files, and rerun until the assigned technical criterion is satisfied.", "WHEN code_failure\nREQUIRE real_runtime_output\nSTEP execute\nSTEP diagnose\nSTEP repair\nSTEP rerun\nVERIFY requested_behavior\nRECOVER continue_from_exact_failure"),
    ("browser_workflow", "browser", "task requires web interaction", "Reuse one browser session, inspect the current state, perform one concrete interaction, and inspect the resulting state before continuing.", "WHEN browser_task\nREQUIRE browser_session\nSTEP inspect\nSTEP interact\nSTEP observe\nVERIFY target_state\nRECOVER inspect_current_page")
  ]
  for item in seeds:
    let existing = store.query("SELECT skill_id FROM skills WHERE tenant_id=? AND name=?", @[%defaultTenantId, %item[0]])
    if existing.len == 0:
      let ts = nowF()
      discard store.exec("INSERT INTO skills (skill_id, tenant_id, name, domain, trigger_spec, procedure_spec, skill_code, reward, active, created_at, updated_at) VALUES (?,?,?,?,?,?,?,0.0,1,?,?)",
        @[%newId("skill"), %defaultTenantId, %item[0], %item[1], %item[2], %item[3], %item[4], %ts, %ts])

proc main() =
  randomize()
  initLock(rngLock)
  initLock(tasksLock)
  initLock(sseLock)
  initLock(chatJobsLock)
  initLock(subAgentsLock)
  initLock(skillGateLock)
  transitionEngine = newStateTransitionEngine()
  globalRng = initRand(int64(epochTime() * 1_000_000.0))
  DbFile = envTrim("AGENT_DB_PATH", envTrim("AGENT_DB", "agent_runtime.db"))
  WorkspaceRoot = envTrim("AGENT_WORKSPACE", "workspace")
  knowledgeRoot = envTrim("AGENT_KNOWLEDGE", "knowledge")
  RequestyBaseUrl = stripTrailingSlash(envTrim("REQUESTY_BASE_URL", DefaultRequestyBaseUrl))
  CerebrasBaseUrl = stripTrailingSlash(envTrim("CEREBRAS_BASE_URL", DefaultCerebrasBaseUrl))
  GeminiBaseUrl = stripTrailingSlash(envTrim("GEMINI_BASE_URL", DefaultGeminiBaseUrl))
  InstaVmBaseUrl = stripTrailingSlash(envTrim("INSTAVM_BASE_URL", DefaultInstaVmBaseUrl))
  CerebrasGemma4Model = getEnv("CEREBRAS_GEMMA4_MODEL", "").strip()
  PromptConfigFile = envTrim("AGENT_PROMPTS_FILE", "config/prompts.yaml")
  ReferenceSkillRoot = envTrim("AGENT_REFERENCE_SKILLS", "skills")
  PublicRoot = envTrim("AGENT_PUBLIC_ROOT", ".")
  serverPort = parseInt(envTrim("PORT", "8080"))
  promptRegistry = loadPromptRegistry(PromptConfigFile)
  referenceSkills = loadReferenceSkills(ReferenceSkillRoot)
  for requiredPrompt in ["orchestrator_router", "orchestrator_system2", "completion_evaluator", "subagent_core", "subagent_orchestrator", "gpt6_astra", "glm52", "gemini38", "minimax_m3", "grok43", "recursive_reason", "distillation_teacher", "distillation_student", "failure_diagnosis", "meta_skill_synthesis", "knowledge_consolidation", "step_verifier", "subagent_verifier"]:
    discard promptText(requiredPrompt)
  createDir(WorkspaceRoot)
  createDir(WorkspaceRoot / "artifacts")
  if not dirExists(PublicRoot):
    raise newException(IOError, "public root not found: " & PublicRoot)
  if not fileExists(PublicRoot / "index.html"):
    raise newException(IOError, "frontend index not found: " & (PublicRoot / "index.html"))
  createDir(knowledgeRoot)
  store = openStore(DbFile)
  migrate(store)
  defaultTenantId = ensureLocalTenant()
  toolRegistry = initOrderedTable[string, ToolSpec]()
  registerTools()
  registerLegacyTools()
  registerReasonTool()
  discard ensureDefaultTenant()
  seedDefaultSkills()
  ensureDiagnosticSuite(defaultTenantId)
  resumePendingTasks()
  resumePendingSubAgents()
  resumePendingChatJobs()
  asyncCheck knowledgeConsolidationLoop()
  let server = newAsyncHttpServer(maxBody = positiveEnvInt("HTTP_MAX_BODY_BYTES", DefaultMaxRequestBodyBytes))
  echo "Runtime listening on port ", serverPort
  waitFor server.serve(Port(serverPort), handleHttpRequest, address = "0.0.0.0")

when isMainModule:
  main()
