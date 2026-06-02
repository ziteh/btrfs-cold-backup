# bcr (btrfs cold backup) — Rust 實作計劃

`bcr` 是以 Rust 實作的 btrfs 冷備份工具，功能對應 Go 版 [zrb](../zfs-remote-backup)，但以 btrfs send/receive 取代 ZFS。

## 設計決策

| 項目 | 選擇 |
|------|------|
| Binary 名稱 | `bcr` |
| Async runtime | tokio |
| S3 SDK | aws-sdk-s3 |
| Snapshot dir | config 每個 task 各自指定 `snapshot_dir` |
| 錯誤處理 | `anyhow::Result` + `thiserror` typed errors |
| Mock 測試 | `mockall` derive macro |

## btrfs vs ZFS 對應

| ZFS | bcr (btrfs) |
|-----|-------------|
| `pool` + `dataset` | `subvolume`（絕對路徑，如 `/mnt/data/movies`）|
| `snapshot_dir` | task config 的 `snapshot_dir` |
| `zfs recv pool/ds` | `btrfs receive {target_dir}` |
| `ZFSVersion` | `BtrfsVersion`（`btrfs --version`）|

## 快照命名

```
bcr_level{N}_{YYYY-MM-DD}_{HH-MM}
```

例：`bcr_level0_2026-06-02_14-30`

## S3 路徑結構

```
{prefix}backup-data/{sanitized_subvol}/{level}/{YYYYMMDD}/snapshot.part-{N}.age
{prefix}manifests/{sanitized_subvol}/{level}/{YYYYMMDD}/task_manifest.yaml
{prefix}manifests/{sanitized_subvol}/last_backup_manifest.yaml
```

## Config 格式

```yaml
base_dir: /var/lib/bcr/
age_public_key: age1...
s3:
  enabled: true
  bucket: my-backup-bucket
  region: us-east-1
  prefix: btrfs-backups/
  endpoint: ""
  storage_class:
    manifest: STANDARD
    backup_data:
      - DEEP_ARCHIVE  # Level 0
      - DEEP_ARCHIVE  # Level 1
      - GLACIER       # Level 2
  retry:
    max_attempts: 8
tasks:
  - name: movies
    description: Movies subvolume backup
    subvolume: /mnt/data/movies
    snapshot_dir: /mnt/data/.snapshots/movies
    enabled: true
```

---

## 專案結構

```
btrfs-cold-backup/
├── Cargo.toml
├── src/
│   ├── main.rs          # 入口，dispatch 子命令
│   ├── cli.rs           # clap CLI 定義
│   ├── config.rs        # Config/Task/S3Config + validate() + #[cfg(test)] Layer 1 tests
│   ├── manifest.rs      # Backup/Last/State/PartInfo + serde + #[cfg(test)] Layer 1 tests
│   ├── util.rs          # 路徑建構、時間格式等 + #[cfg(test)] Layer 1 tests
│   ├── btrfs.rs         # BtrfsOps trait + BtrfsDriver 實作
│   ├── crypto.rs        # Age encrypt/decrypt + BLAKE3
│   ├── remote.rs        # RemoteBackend trait + S3Backend 實作
│   ├── lock.rs          # PID lock file（guard 模式） + #[cfg(test)] 並行測試
│   ├── backup.rs        # 備份編排
│   ├── restore.rs       # 還原編排
│   ├── list.rs          # 列出備份
│   ├── check.rs         # 前置驗證
│   ├── keys.rs          # age 金鑰生成
│   └── lib.rs           # 公開 API（為了 tests/ 能呼叫）
└── tests/
    ├── integration/     # Layer 2：mockall mock FS + Remote（業務邏輯整合測試）
    ├── vm/              # Layer 3：真實 btrfs（CI privileged VM）
    └── e2e/             # Layer 4：VM + MinIO 完整生命週期
```

## 關鍵 Crates

```toml
[dependencies]
clap = { version = "4", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
aws-sdk-s3 = "1"
aws-config = "1"
age = "0.10"
blake3 = "1"
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
anyhow = "1"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
mockall = "0.13"
tempfile = "3"
tokio = { version = "1", features = ["full", "test-util"] }
```

---

## 開發階段

### Phase 1 — Foundation（純型別，無外部依賴）

1. 初始化 Cargo project，設定 `Cargo.toml` + `lib.rs`
2. `config.rs`：定義型別與 `validate()` 方法，內含 `#[cfg(test)]` Layer 1 tests
   - 設定驗證（空欄位、非法 age key、重複 task name、負層級）
3. `manifest.rs`：定義 `Backup`、`Last`、`State`、`PartInfo`，全部實作 serde，內含 `#[cfg(test)]` Layer 1 tests
   - Serde 往返一致性（含 null 層級）
   - 時間戳記格式驗證
4. `util.rs`：路徑建構（subvolume path sanitize → S3 key、`task_dir_name(level, ts)`），內含 `#[cfg(test)]` Layer 1 tests
   - 特殊字元處理、manifest 路徑 vs 資料路徑不衝突
5. `lock.rs`：PID lock（drop guard 自動釋放，陳舊鎖回收），內含 `#[cfg(test)]` 並行測試

步驟 2/3/4/5 可並行實作。`cargo test` 即可執行所有 Layer 1 tests。

### Phase 2 — 核心抽象層（trait + 實作）

6. `btrfs.rs`：定義 `BtrfsOps` trait（`list_snapshots`, `send_full`, `send_incremental`, `create_snapshot`, `delete_snapshot`, `subvolume_exists`），實作 `BtrfsDriver`（呼叫 `btrfs` CLI）
7. `remote.rs`：定義 `RemoteBackend` trait（`upload`, `head`, `download`, `verify_credentials`），實作 `S3Backend`（aws-sdk-s3）
8. `crypto.rs`：`encrypt_file`、`decrypt_file`、`blake3_file`

步驟 6/7/8 可並行實作。

### Phase 2.5 — Layer 2 整合測試（使用 mockall）

9. `tests/integration/backup_test.rs`：測試 backup 業務邏輯（使用 mock BtrfsOps + RemoteBackend）
   - Level 0 完整備份流程、快照不存在時拒絕、上傳失敗重試
   - Level 1 增量備份（Level 0 不存在時拒絕、parent 遺失偵測）
   - 中斷續傳（各種恢復場景）
   - 還原流程（hash 不符中止、目標已存在確認）

執行：`cargo test --test backup_test` 或 `cargo test integration`

### Phase 3 — 業務邏輯

10. `backup.rs`：備份編排（load config → lock → list snapshots → send → encrypt → upload）
11. `restore.rs`：還原編排（download → verify → decrypt → concat → `btrfs receive`）
12. `check.rs`：前置驗證（config validate → subvolume_exists → S3 verify_credentials）
13. `list.rs`：列出備份（S3 prefix 列舉 manifest，格式化輸出）
14. `keys.rs`：age 金鑰生成

### Phase 4 — CLI 接線

15. `cli.rs`：clap 定義所有子命令（`genkey`, `check`, `backup`, `list`, `restore`）
16. `main.rs`：dispatch + tracing 日誌初始化

### Phase 5 — VM / E2E 測試

17. `tests/vm/btrfs_driver_test.rs`：對真實 btrfs loop device 測試 `BtrfsDriver`（需 CI privileged VM）
18. `tests/e2e/`：完整生命週期（Level 0 + Level 1 + 還原、中途中斷續傳、加密驗證；需 VM + MinIO）

---

## TDD 執行節奏

開發新功能時：
1. 先寫 Layer 2 的失敗測試（描述期望的業務行為）
2. 寫最簡單能通過的實作（含 BtrfsOps/RemoteBackend 的 mock 實作）
3. 補充 Layer 1 的邊界情況測試
4. 進 VM 補充 Layer 3 驗證 btrfs 實作
5. 最後跑 Layer 4 確認整體流程

Layer 3/4 需求：Linux VM 搭載 btrfs（loop device），2 CPU / 4GB RAM / 10GB 磁碟最低規格。

---

## CLI 子命令

| 命令 | 說明 |
|------|------|
| `bcr genkey` | 生成 age 金鑰對 |
| `bcr check` | 驗證設定與連線 |
| `bcr backup` | 執行備份（`--level N`, `--task NAME`）|
| `bcr list` | 列出備份（`--source s3\|local`, `--level N`）|
| `bcr restore` | 還原（`--level N`, `--target DIR`, `--private-key FILE`）|

---

## 範圍說明

**包含**：備份（full + incremental）、還原、金鑰生成、前置驗證、列表、中斷續傳  
**不包含**：快照建立/刪除（由使用者以 `btrfs subvolume snapshot/delete` 管理）、自動排程（由外部 cron 負責）、GUI、多後端（僅 S3/S3-compatible）

## 參考

- [zrb README](../zfs-remote-backup/README.md) — 原始 Go 版功能說明
- [btrfs 差異分析](../zfs-remote-backup/btrfs.md) — Hold 機制缺失與替代方案
- [TDD 手冊](../zfs-remote-backup/TDD_HANDBOOK.md) — 各層測試案例清單
