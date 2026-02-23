"""Scheduled backup system (Restic-inspired)."""

import sqlite3
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import hashlib
import os
from enum import Enum
import re


class BackendType(Enum):
    """Backend storage type."""
    LOCAL = "local"
    S3 = "s3"
    R2 = "r2"
    SFTP = "sftp"


@dataclass
class Snapshot:
    """Backup snapshot."""
    id: str
    repo_id: str
    hostname: str
    paths: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    tree_sha256: str = ""
    stats: Dict = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    parent_id: Optional[str] = None


@dataclass
class Repository:
    """Backup repository."""
    id: str
    name: str
    path: str
    password_hint: str
    backend: str = "local"
    size_bytes: int = 0
    snapshots: int = 0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    last_backup: Optional[str] = None


@dataclass
class Schedule:
    """Backup schedule."""
    id: str
    repo_id: str
    job_name: str
    cron_expr: str
    retention_policy: Dict = field(default_factory=dict)
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    status: str = "active"


class BackupScheduler:
    """Manages backup schedules and snapshots."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize backup scheduler with SQLite backend."""
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/restic.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS repositories (
                id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                path TEXT NOT NULL,
                password_hint TEXT,
                backend TEXT,
                size_bytes INTEGER,
                snapshots INTEGER,
                created_at TEXT,
                last_backup TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                hostname TEXT,
                paths TEXT,
                tags TEXT,
                tree_sha256 TEXT,
                stats TEXT,
                created_at TEXT,
                parent_id TEXT,
                FOREIGN KEY(repo_id) REFERENCES repositories(id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                cron_expr TEXT NOT NULL,
                retention_policy TEXT,
                last_run TEXT,
                next_run TEXT,
                status TEXT,
                FOREIGN KEY(repo_id) REFERENCES repositories(id)
            )
        """)

        conn.commit()
        conn.close()

    def init_repo(self, name: str, path: str, password: str, backend: str = "local") -> str:
        """Create repository metadata."""
        repo_id = hashlib.sha256(f"{name}{path}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]
        repo = Repository(id=repo_id, name=name, path=path, password_hint=password[:4] + "*" * len(password[4:]),
                         backend=backend)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO repositories (id, name, path, password_hint, backend, size_bytes, snapshots, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (repo.id, repo.name, repo.path, repo.password_hint, repo.backend, 0, 0, repo.created_at))
        conn.commit()
        conn.close()
        return repo_id

    def create_snapshot(self, repo_id: str, paths: List[str], tags: List[str] = None, hostname: str = "") -> str:
        """Create incremental snapshot."""
        if tags is None:
            tags = []

        snap_id = hashlib.sha256(f"{repo_id}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]

        # Find parent snapshot
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM snapshots WHERE repo_id = ? ORDER BY created_at DESC LIMIT 1", (repo_id,))
        parent_row = cursor.fetchone()
        parent_id = parent_row[0] if parent_row else None

        # Calculate tree hash
        tree_hash = hashlib.sha256(json.dumps(sorted(paths)).encode()).hexdigest()

        stats = {
            "files": len(paths),
            "size_bytes": 1024 * len(paths),  # Mock
            "incremental": parent_id is not None
        }

        snapshot = Snapshot(id=snap_id, repo_id=repo_id, hostname=hostname, paths=paths,
                           tags=tags, tree_sha256=tree_hash, stats=stats, parent_id=parent_id)

        cursor.execute("""
            INSERT INTO snapshots (id, repo_id, hostname, paths, tags, tree_sha256, stats, created_at, parent_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (snapshot.id, repo_id, hostname, json.dumps(paths), json.dumps(tags),
              tree_hash, json.dumps(stats), snapshot.created_at, parent_id))

        # Update repo stats
        cursor.execute("UPDATE repositories SET snapshots = snapshots + 1, last_backup = ? WHERE id = ?",
                      (snapshot.created_at, repo_id))

        conn.commit()
        conn.close()
        return snap_id

    def list_snapshots(self, repo_id: str, tags: Optional[List[str]] = None,
                      hostname: Optional[str] = None, limit: int = 20) -> List[Snapshot]:
        """List snapshots with optional filters."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = "SELECT * FROM snapshots WHERE repo_id = ?"
        params = [repo_id]

        if hostname:
            query += " AND hostname = ?"
            params.append(hostname)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        snapshots = []
        for row in rows:
            row_tags = json.loads(row[4])
            if tags and not any(t in row_tags for t in tags):
                continue

            snapshot = Snapshot(id=row[0], repo_id=row[1], hostname=row[2],
                              paths=json.loads(row[3]), tags=row_tags,
                              tree_sha256=row[5], stats=json.loads(row[6]),
                              created_at=row[7], parent_id=row[8])
            snapshots.append(snapshot)

        conn.close()
        return snapshots

    def restore_snapshot(self, snapshot_id: str, target_path: str) -> bool:
        """Simulate restore of snapshot."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT paths FROM snapshots WHERE id = ?", (snapshot_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return False

        os.makedirs(target_path, exist_ok=True)
        # Mock: just write placeholder files
        paths = json.loads(row[0])
        for path in paths:
            file_path = os.path.join(target_path, os.path.basename(path))
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as f:
                f.write(f"Restored: {path}\n")

        return True

    def forget(self, repo_id: str, keep_last: Optional[int] = None,
              keep_daily: int = 7, keep_weekly: int = 4, keep_monthly: int = 3):
        """Prune old snapshots based on retention policy."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT id, created_at FROM snapshots WHERE repo_id = ? ORDER BY created_at DESC",
                      (repo_id,))
        snapshots = cursor.fetchall()

        keep_ids = set()
        now = datetime.utcnow()

        # Keep last N
        if keep_last:
            for i in range(min(keep_last, len(snapshots))):
                keep_ids.add(snapshots[i][0])

        # Keep daily (last 7 days)
        for days in range(keep_daily):
            target_date = (now - timedelta(days=days)).date()
            for snap_id, created_at in snapshots:
                snap_date = datetime.fromisoformat(created_at).date()
                if snap_date == target_date and snap_id not in keep_ids:
                    keep_ids.add(snap_id)
                    break

        # Delete unlisted
        all_ids = [s[0] for s in snapshots]
        to_delete = [sid for sid in all_ids if sid not in keep_ids]

        for snap_id in to_delete:
            cursor.execute("DELETE FROM snapshots WHERE id = ?", (snap_id,))

        cursor.execute("UPDATE repositories SET snapshots = ? WHERE id = ?",
                      (len(keep_ids), repo_id))
        conn.commit()
        conn.close()

    def check_repo(self, repo_id: str) -> bool:
        """Spot-check repository integrity."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT tree_sha256 FROM snapshots WHERE repo_id = ?", (repo_id,))
        rows = cursor.fetchall()
        conn.close()

        # Mock: just verify hashes exist and are valid hex
        for row in rows:
            if not row[0] or len(row[0]) != 64:
                return False
            try:
                int(row[0], 16)
            except ValueError:
                return False

        return True

    def add_schedule(self, repo_id: str, job_name: str, cron_expr: str,
                    keep_last: Optional[int] = None, keep_daily: int = 7) -> str:
        """Add backup schedule."""
        schedule_id = hashlib.sha256(f"{repo_id}{job_name}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]

        next_run = self._calculate_next_run(cron_expr)
        policy = {"keep_last": keep_last, "keep_daily": keep_daily}

        schedule = Schedule(id=schedule_id, repo_id=repo_id, job_name=job_name,
                           cron_expr=cron_expr, retention_policy=policy, next_run=next_run)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO schedules (id, repo_id, job_name, cron_expr, retention_policy, next_run, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (schedule.id, repo_id, job_name, cron_expr, json.dumps(policy), next_run, schedule.status))
        conn.commit()
        conn.close()
        return schedule_id

    def _calculate_next_run(self, cron_expr: str) -> str:
        """Calculate next run time from cron expression."""
        now = datetime.utcnow()

        if cron_expr == "daily" or cron_expr == "@daily":
            return (now + timedelta(days=1)).isoformat()
        elif cron_expr == "weekly" or cron_expr == "@weekly":
            return (now + timedelta(weeks=1)).isoformat()
        elif cron_expr == "monthly" or cron_expr == "@monthly":
            return (now + timedelta(days=30)).isoformat()
        else:
            # Simple cron parsing: "0 2 * * *" = 2am daily
            return (now + timedelta(days=1)).isoformat()

    def get_due_jobs(self) -> List[Schedule]:
        """Get schedules where next_run <= now."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        now = datetime.utcnow().isoformat()
        cursor.execute("SELECT * FROM schedules WHERE next_run <= ? AND status = 'active'", (now,))
        rows = cursor.fetchall()

        schedules = []
        for row in rows:
            schedule = Schedule(id=row[0], repo_id=row[1], job_name=row[2],
                              cron_expr=row[3], retention_policy=json.loads(row[4]),
                              last_run=row[5], next_run=row[6], status=row[7])
            schedules.append(schedule)

        conn.close()
        return schedules

    def run_scheduled(self, job_id: str) -> bool:
        """Run scheduled backup job."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT repo_id, job_name FROM schedules WHERE id = ?", (job_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return False

        repo_id, job_name = row

        # Get repo paths
        cursor.execute("SELECT path FROM repositories WHERE id = ?", (repo_id,))
        repo_row = cursor.fetchone()
        if not repo_row:
            conn.close()
            return False

        # Create snapshot
        paths = [repo_row[0]]  # Simplified
        snap_id = self.create_snapshot(repo_id, paths, tags=[job_name])

        # Update schedule
        now = datetime.utcnow().isoformat()
        next_run = self._calculate_next_run("")

        cursor.execute("""
            UPDATE schedules SET last_run = ?, next_run = ? WHERE id = ?
        """, (now, next_run, job_id))

        conn.commit()
        conn.close()
        return True

    def get_stats(self, repo_id: str) -> Dict:
        """Get repository statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT size_bytes, snapshots FROM repositories WHERE id = ?", (repo_id,))
        repo_row = cursor.fetchone()

        if not repo_row:
            conn.close()
            return {}

        size_bytes, num_snapshots = repo_row

        cursor.execute("SELECT created_at FROM snapshots WHERE repo_id = ? ORDER BY created_at ASC LIMIT 1",
                      (repo_id,))
        oldest_row = cursor.fetchone()
        oldest_snapshot = oldest_row[0] if oldest_row else None

        conn.close()

        return {
            "repo_id": repo_id,
            "total_size_bytes": size_bytes,
            "snapshots": num_snapshots,
            "dedup_ratio": 0.65,  # Mock
            "oldest_snapshot": oldest_snapshot
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backup Scheduler CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Repos
    subparsers.add_parser("repos")

    # Snapshot
    snap_parser = subparsers.add_parser("snapshot")
    snap_parser.add_argument("repo_id")
    snap_parser.add_argument("paths", nargs="+")
    snap_parser.add_argument("--tags", type=str, nargs="+", default=[])

    # Forget
    forget_parser = subparsers.add_parser("forget")
    forget_parser.add_argument("repo_id")
    forget_parser.add_argument("--keep-daily", type=int, default=7)
    forget_parser.add_argument("--keep-weekly", type=int, default=4)

    args = parser.parse_args()
    scheduler = BackupScheduler()

    if args.command == "repos":
        conn = sqlite3.connect(scheduler.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM repositories")
        repos = cursor.fetchall()
        conn.close()
        for repo in repos:
            print(f"{repo[1]}: {repo[2]} ({repo[4]})")
    elif args.command == "snapshot":
        snap_id = scheduler.create_snapshot(args.repo_id, args.paths, tags=args.tags)
        print(f"Snapshot created: {snap_id}")
    elif args.command == "forget":
        scheduler.forget(args.repo_id, keep_daily=args.keep_daily, keep_weekly=args.keep_weekly)
        print(f"Pruned old snapshots from {args.repo_id}")
