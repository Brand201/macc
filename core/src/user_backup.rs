use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::{MaccError, Result};

/// An entry describing a single file that was backed up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserBackupEntry {
    /// Absolute path to the original file.
    pub original: PathBuf,
    /// Absolute path to the backup copy.
    pub backup: PathBuf,
}

/// Report describing the set of user-level files backed up under a timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserBackupReport {
    /// Timestamp used for this backup run.
    pub timestamp: String,
    /// Root directory containing the backups for the timestamp (i.e., ~/.macc/backups/<timestamp>).
    pub root: PathBuf,
    /// Entries that were copied.
    pub entries: Vec<UserBackupEntry>,
}

impl UserBackupReport {
    /// Returns true if at least one file was backed up.
    pub fn has_backups(&self) -> bool {
        !self.entries.is_empty()
    }
}

/// Manages timestamped backups for user-level files (typically under ~/.macc/backups).
pub struct UserBackupManager {
    root: PathBuf,
    home: PathBuf,
}

impl UserBackupManager {
    /// Creates a manager rooted at the current user's home directory.
    pub fn try_new() -> Result<Self> {
        let home = find_user_home().ok_or(MaccError::HomeDirNotFound)?;
        Self::with_home(home)
    }

    /// Creates a manager rooted at the provided home directory (useful for tests).
    pub fn with_home(home: PathBuf) -> Result<Self> {
        let root = home.join(".macc/backups");
        fs::create_dir_all(&root).map_err(|e| MaccError::Io {
            path: root.to_string_lossy().into(),
            action: "create user backup root".into(),
            source: e,
        })?;
        Ok(Self { root, home })
    }

    /// Returns the root directory under which timestamped backups are stored.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Creates a backup for `source_path` under `<root>/<timestamp>/...` and returns the entry if the
    /// source existed.
    pub fn backup_file<P: AsRef<Path>>(
        &self,
        timestamp: &str,
        source_path: P,
    ) -> Result<Option<UserBackupEntry>> {
        let source_path = source_path.as_ref();
        if !source_path.exists() {
            return Ok(None);
        }

        let metadata = source_path.metadata().map_err(|e| MaccError::Io {
            path: source_path.to_string_lossy().into(),
            action: "stat user file before backup".into(),
            source: e,
        })?;

        if !metadata.is_file() {
            // Currently only support backing up files.
            return Ok(None);
        }

        let relative = self.relative_path(source_path);
        let timestamp_root = self.root.join(timestamp);
        let backup_target = timestamp_root.join(relative);

        if let Some(parent) = backup_target.parent() {
            fs::create_dir_all(parent).map_err(|e| MaccError::Io {
                path: parent.to_string_lossy().into(),
                action: "create user backup parent".into(),
                source: e,
            })?;
        }

        fs::copy(source_path, &backup_target).map_err(|e| MaccError::Io {
            path: source_path.to_string_lossy().into(),
            action: format!("copy to {}", backup_target.display()),
            source: e,
        })?;

        fs::set_permissions(&backup_target, metadata.permissions()).map_err(|e| MaccError::Io {
            path: backup_target.to_string_lossy().into(),
            action: "set backup permissions".into(),
            source: e,
        })?;

        Ok(Some(UserBackupEntry {
            original: source_path.to_path_buf(),
            backup: backup_target,
        }))
    }

    /// Copies each source path into the timestamped backup set, returning the generated report.
    pub fn backup_files<I, P>(&self, timestamp: &str, sources: I) -> Result<UserBackupReport>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        let mut entries = Vec::new();
        for source in sources {
            if let Some(entry) = self.backup_file(timestamp, source)? {
                entries.push(entry);
            }
        }
        Ok(self.report(timestamp, entries))
    }

    /// Builds a report for the completed backup entries.
    pub fn report(&self, timestamp: &str, entries: Vec<UserBackupEntry>) -> UserBackupReport {
        UserBackupReport {
            timestamp: timestamp.to_string(),
            root: self.root.join(timestamp),
            entries,
        }
    }

    fn relative_path<P: AsRef<Path>>(&self, source_path: P) -> PathBuf {
        let source_path = source_path.as_ref();
        if source_path.is_absolute() {
            if let Ok(stripped) = source_path.strip_prefix(&self.home) {
                if !stripped.as_os_str().is_empty() {
                    return stripped.to_path_buf();
                }
            }
        }

        let mut components = PathBuf::new();
        for comp in source_path.components() {
            match comp {
                Component::Normal(segment) => components.push(segment),
                Component::RootDir
                | Component::CurDir
                | Component::ParentDir
                | Component::Prefix(_) => {}
            }
        }
        if components.as_os_str().is_empty() {
            components.push("root");
        }
        components
    }
}

pub fn find_user_home() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("HOME") {
        return Some(PathBuf::from(dir));
    }
    if let Some(dir) = std::env::var_os("USERPROFILE") {
        return Some(PathBuf::from(dir));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_home(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("user_backup_{}_{}", name, uuid_v4_like()));
        fs::create_dir_all(&path).expect("create temp home");
        path
    }

    fn uuid_v4_like() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{:x}", nanos)
    }

    #[test]
    fn backup_file_copies_content_and_permissions() -> Result<()> {
        let home = temp_home("backups");
        let manager = UserBackupManager::with_home(home.clone())?;
        let tool_id = format!("tool-{}", uuid_v4_like());
        let user_file = home.join(format!(".{}/settings.json", tool_id));
        fs::create_dir_all(user_file.parent().unwrap()).unwrap();
        fs::write(&user_file, b"hello user").unwrap();

        let entry = manager
            .backup_file("20260130-000000", &user_file)?
            .expect("backup entry expected");

        assert_eq!(entry.original, user_file);
        assert!(entry.backup.exists());
        assert_eq!(fs::read_to_string(entry.backup).unwrap(), "hello user");

        fs::remove_dir_all(&home).unwrap();
        Ok(())
    }

    #[test]
    fn backup_file_returns_none_when_missing() -> Result<()> {
        let home = temp_home("missing");
        let manager = UserBackupManager::with_home(home.clone())?;
        let missing = home.join("does/not/exist.txt");

        assert!(manager.backup_file("20260130-000001", &missing)?.is_none());

        fs::remove_dir_all(&home).unwrap();
        Ok(())
    }

    #[test]
    fn backup_files_reports_entries() -> Result<()> {
        let home = temp_home("many");
        let manager = UserBackupManager::with_home(home.clone())?;
        let file_a = home.join("file_a");
        let file_b = home.join("dir/file_b");
        fs::write(&file_a, b"a").unwrap();
        fs::create_dir_all(file_b.parent().unwrap()).unwrap();
        fs::write(&file_b, b"b").unwrap();

        let report =
            manager.backup_files("20260130-000003", vec![file_a.clone(), file_b.clone()])?;

        assert!(report.has_backups());
        assert_eq!(report.entries.len(), 2);
        assert_eq!(report.entries[0].original, file_a);
        assert_eq!(report.entries[1].original, file_b);

        fs::remove_dir_all(&home).unwrap();
        Ok(())
    }

    #[test]
    fn backup_path_sanitizes_absolute_outside_home() -> Result<()> {
        let home = temp_home("sanitize");
        let manager = UserBackupManager::with_home(home.clone())?;

        let other_dir = std::env::temp_dir().join(format!("outside_{}", uuid_v4_like()));
        fs::create_dir_all(&other_dir).unwrap();
        let external_file = other_dir.join("etc/passwd");
        fs::create_dir_all(external_file.parent().unwrap()).unwrap();
        fs::write(&external_file, b"root:x:0:0").unwrap();

        let entry = manager
            .backup_file("20260130-000002", &external_file)?
            .expect("backup entry expected");

        assert!(entry
            .backup
            .starts_with(manager.root.join("20260130-000002")));
        assert!(entry.backup.to_string_lossy().contains("etc/passwd"));

        fs::remove_dir_all(&home).unwrap();
        fs::remove_dir_all(&other_dir).unwrap();
        Ok(())
    }
}
