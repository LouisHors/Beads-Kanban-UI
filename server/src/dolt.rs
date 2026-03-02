//! Dolt database backend support for beads-kanban-ui.
//!
//! Provides connectivity to Dolt (MySQL-compatible version-controlled database)
//! for reading beads data when the backend is configured as "dolt".

use mysql::{Pool, prelude::Queryable};
use serde::Deserialize;
use std::path::Path;

/// Dolt connection configuration.
#[derive(Debug, Clone)]
pub struct DoltConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
}

impl Default for DoltConfig {
    fn default() -> Self {
        Self {
            host: std::env::var("BEADS_DOLT_SERVER_HOST")
                .unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("BEADS_DOLT_SERVER_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3306),
            user: std::env::var("BEADS_DOLT_SERVER_USER")
                .unwrap_or_else(|_| "root".to_string()),
            database: "beads".to_string(),
        }
    }
}

/// Backend type detection from config.
#[derive(Debug, Clone, PartialEq)]
pub enum BackendType {
    Jsonl,
    Dolt,
}

/// Detects the backend type from .beads/config.yaml.
///
/// Returns `BackendType::Dolt` if `backend: dolt` is set in the config,
/// otherwise returns `BackendType::Jsonl` for backward compatibility.
pub fn detect_backend(project_path: &Path) -> BackendType {
    let config_path = project_path.join(".beads").join("config.yaml");
    
    let config_contents = match std::fs::read_to_string(&config_path) {
        Ok(c) => c,
        Err(_) => return BackendType::Jsonl,
    };
    
    let yaml: serde_yaml::Value = match serde_yaml::from_str(&config_contents) {
        Ok(v) => v,
        Err(_) => return BackendType::Jsonl,
    };
    
    // Check for backend: dolt
    if let Some(backend) = yaml.get("backend").and_then(|v| v.as_str()) {
        if backend == "dolt" {
            return BackendType::Dolt;
        }
    }
    
    BackendType::Jsonl
}

/// Extracts database name from config or generates from repo name.
fn get_database_name(project_path: &Path, config: &serde_yaml::Value) -> String {
    // Try to get from config first
    if let Some(db_name) = config.get("database").and_then(|v| v.as_str()) {
        return db_name.to_string();
    }
    
    // Try dolt.database
    if let Some(dolt) = config.get("dolt") {
        if let Some(db_name) = dolt.get("database").and_then(|v| v.as_str()) {
            return db_name.to_string();
        }
    }
    
    // Fallback: derive from directory name
    // e.g., "Beads-Kanban-UI" -> "Beads_Kanban_UI"
    project_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.replace('-', "_"))
        .unwrap_or_else(|| "beads".to_string())
}

/// Creates a MySQL connection pool to Dolt server.
pub fn connect(config: &DoltConfig) -> Result<Pool, String> {
    let url = format!(
        "mysql://{}@{}:{}/{}",
        config.user, config.host, config.port, config.database
    );
    
    Pool::new(url.as_str())
        .map_err(|e| format!("Failed to connect to Dolt: {}", e))
}

/// A dependency row from Dolt.
#[derive(Debug, Clone, Deserialize)]
pub struct DoltDependency {
    pub issue_id: String,
    pub depends_on_id: String,
    #[serde(rename = "type")]
    pub dep_type: String,
}

/// A comment row from Dolt.
#[derive(Debug, Clone, Deserialize)]
pub struct DoltComment {
    pub id: i64,
    pub issue_id: String,
    pub author: String,
    pub text: String,
    pub created_at: String,
}

/// An issue row from Dolt (subset of columns).
#[derive(Debug, Clone, Deserialize)]
pub struct DoltIssue {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: Option<i32>,
    pub issue_type: Option<String>,
    pub owner: Option<String>,
    pub created_at: Option<String>,
    pub created_by: Option<String>,
    pub updated_at: Option<String>,
    pub closed_at: Option<String>,
    pub design: Option<String>,  // Maps to design_doc
}

/// Loads beads from Dolt database.
///
/// Queries the issues, dependencies, and comments tables and returns
/// structured data compatible with the JSONL format.
pub fn load_beads_from_dolt(
    project_path: &Path,
) -> Result<(Vec<DoltIssue>, Vec<DoltDependency>, Vec<DoltComment>), String> {
    // Read config for database name
    let config_path = project_path.join(".beads").join("config.yaml");
    let config_yaml: serde_yaml::Value = std::fs::read_to_string(&config_path)
        .ok()
        .and_then(|c| serde_yaml::from_str(&c).ok())
        .unwrap_or(serde_yaml::Value::Null);
    
    let db_name = get_database_name(project_path, &config_yaml);
    
    let mut config = DoltConfig::default();
    config.database = db_name;
    
    let pool = connect(&config)?;
    let mut conn = pool.get_conn()
        .map_err(|e| format!("Failed to get connection: {}", e))?;
    
    // Query issues
    let issues: Vec<DoltIssue> = conn
        .query_map(
            "SELECT id, title, description, status, priority, issue_type, owner, \
             created_at, created_by, updated_at, closed_at, design \
             FROM issues \
             WHERE (ephemeral = 0 OR ephemeral IS NULL) \
             ORDER BY updated_at DESC",
            |(id, title, description, status, priority, issue_type, owner,
              created_at, created_by, updated_at, closed_at, design)| {
                DoltIssue {
                    id, title, description, status, priority, issue_type, owner,
                    created_at, created_by, updated_at, closed_at, design,
                }
            },
        )
        .map_err(|e| format!("Failed to query issues: {}", e))?;
    
    // Query dependencies
    let dependencies: Vec<DoltDependency> = conn
        .query_map(
            "SELECT issue_id, depends_on_id, type \
             FROM dependencies \
             WHERE type IN ('parent-child', 'relates-to', 'blocks')",
            |(issue_id, depends_on_id, dep_type)| {
                DoltDependency {
                    issue_id, depends_on_id, dep_type,
                }
            },
        )
        .map_err(|e| format!("Failed to query dependencies: {}", e))?;
    
    // Query comments
    let comments: Vec<DoltComment> = conn
        .query_map(
            "SELECT id, issue_id, author, text, created_at \
             FROM comments \
             ORDER BY created_at ASC",
            |(id, issue_id, author, text, created_at)| {
                DoltComment { id, issue_id, author, text, created_at }
            },
        )
        .map_err(|e| format!("Failed to query comments: {}", e))?;
    
    Ok((issues, dependencies, comments))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_detect_backend_default() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        std::fs::create_dir_all(project.join(".beads")).unwrap();
        
        // No config -> Jsonl
        assert_eq!(detect_backend(project), BackendType::Jsonl);
    }
    
    #[test]
    fn test_detect_backend_dolt() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        std::fs::create_dir_all(project.join(".beads")).unwrap();
        std::fs::write(
            project.join(".beads").join("config.yaml"),
            "backend: dolt\n",
        ).unwrap();
        
        assert_eq!(detect_backend(project), BackendType::Dolt);
    }
    
    #[test]
    fn test_detect_backend_sqlite() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        std::fs::create_dir_all(project.join(".beads")).unwrap();
        std::fs::write(
            project.join(".beads").join("config.yaml"),
            "backend: sqlite\n",
        ).unwrap();
        
        assert_eq!(detect_backend(project), BackendType::Jsonl);
    }
    
    #[test]
    fn test_get_database_name_from_config() {
        let yaml: serde_yaml::Value = serde_yaml::from_str("database: my_db\n").unwrap();
        let name = get_database_name(Path::new("/some/path"), &yaml);
        assert_eq!(name, "my_db");
    }
    
    #[test]
    fn test_get_database_name_from_path() {
        let yaml = serde_yaml::Value::Null;
        let name = get_database_name(Path::new("/some/My-Project"), &yaml);
        assert_eq!(name, "My_Project");
    }
}
