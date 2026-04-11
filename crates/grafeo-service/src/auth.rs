//! Transport-agnostic authentication provider.
//!
//! Credential extraction is transport-specific (HTTP headers, gRPC metadata,
//! Bolt LOGON). This module only handles credential verification.

use serde::{Deserialize, Serialize};
#[cfg(feature = "auth")]
use subtle::ConstantTimeEq;

use grafeo_engine::auth::{Identity, Role};

/// Map a [`Role`] to the wire-format string used in JSON storage and API responses.
pub fn role_to_str(role: Role) -> &'static str {
    match role {
        Role::Admin => "admin",
        Role::ReadWrite => "read-write",
        Role::ReadOnly => "read-only",
    }
}

/// Parse a wire-format string into a [`Role`].
pub fn str_to_role(s: &str) -> Result<Role, String> {
    match s {
        "admin" => Ok(Role::Admin),
        "read-write" => Ok(Role::ReadWrite),
        "read-only" => Ok(Role::ReadOnly),
        _ => Err(format!("unknown role: {s}")),
    }
}

mod role_serde {
    use super::*;

    #[allow(clippy::trivially_copy_pass_by_ref)] // serde requires &T
    pub fn serialize<S: serde::Serializer>(role: &Role, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(role_to_str(*role))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Role, D::Error> {
        let s = String::deserialize(d)?;
        str_to_role(&s).map_err(serde::de::Error::custom)
    }
}

/// Permission scope for a token: role + database restrictions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TokenScope {
    /// Permission level: "admin", "read-write", or "read-only".
    #[serde(with = "role_serde")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "admin"))]
    pub role: Role,
    /// Databases this token can access. Empty vec = all databases.
    #[serde(default)]
    pub databases: Vec<String>,
}

impl Default for TokenScope {
    fn default() -> Self {
        Self {
            role: Role::Admin,
            databases: vec![],
        }
    }
}

/// On-disk record (hashed token, never the plaintext).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub id: String,
    pub name: String,
    pub token_hash: String,
    pub scope: TokenScope,
    pub created_at: String,
}

/// In-flight identity attached to a request after authentication.
#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub id: String,
    pub name: String,
    pub scope: TokenScope,
}

impl TokenInfo {
    /// Construct an engine [`Identity`] from this token's metadata.
    pub fn identity(&self) -> Identity {
        Identity::new(&self.name, [self.scope.role])
    }
}

/// Authentication provider supporting bearer tokens, token store, and HTTP Basic auth.
///
/// `check_bearer` checks the legacy single token first (from `--auth-token`),
/// then the token store. Returns `Option<TokenInfo>` with the token's scope.
/// Basic auth always returns admin scope.
#[cfg(feature = "auth")]
#[derive(Clone)]
pub struct AuthProvider {
    /// Legacy single bearer token (from --auth-token CLI flag).
    bearer_token: Option<String>,
    basic_user: Option<String>,
    basic_password: Option<String>,
    /// Token store for managed API keys (from token CRUD API).
    token_store: Option<std::sync::Arc<crate::token_store::TokenStore>>,
}

#[cfg(feature = "auth")]
impl AuthProvider {
    /// Creates an auth provider if any credentials are configured.
    /// Returns `None` if no authentication is set up.
    pub fn new(
        token: Option<String>,
        user: Option<String>,
        password: Option<String>,
    ) -> Option<Self> {
        if token.is_none() && user.is_none() {
            return None;
        }
        Some(Self {
            bearer_token: token,
            basic_user: user,
            basic_password: password,
            token_store: None,
        })
    }

    /// Creates a provider with a token store for managed API keys.
    pub fn with_token_store(
        token: Option<String>,
        user: Option<String>,
        password: Option<String>,
        store: std::sync::Arc<crate::token_store::TokenStore>,
    ) -> Option<Self> {
        // If we have a store, we always enable auth (even without a legacy token,
        // managed tokens will authenticate via the store).
        let has_credentials = token.is_some() || user.is_some();
        let has_store = true;
        if !has_credentials && !has_store {
            return None;
        }
        Some(Self {
            bearer_token: token,
            basic_user: user,
            basic_password: password,
            token_store: Some(store),
        })
    }

    /// Whether any authentication method is configured.
    pub fn is_enabled(&self) -> bool {
        self.bearer_token.is_some() || self.basic_user.is_some() || self.token_store.is_some()
    }

    /// Returns a reference to the token store, if configured.
    pub fn token_store(&self) -> Option<&crate::token_store::TokenStore> {
        self.token_store.as_deref()
    }

    /// Check a bearer token or API key. Returns token identity on success.
    ///
    /// Checks the legacy single token first (admin scope), then the token
    /// store. Returns `None` if no match.
    pub fn check_bearer(&self, token: &str) -> Option<TokenInfo> {
        // Check legacy single token (constant-time)
        let legacy_match = self
            .bearer_token
            .as_ref()
            .is_some_and(|expected| ct_eq(token.as_bytes(), expected.as_bytes()));

        if legacy_match {
            return Some(TokenInfo {
                id: "_root".to_string(),
                name: "root".to_string(),
                scope: TokenScope::default(), // admin, all databases
            });
        }

        // Check token store
        if let Some(store) = &self.token_store {
            let hash = crate::token_service::hash_token(token);
            if let Some(record) = store.find_by_hash(&hash) {
                return Some(TokenInfo {
                    id: record.id,
                    name: record.name,
                    scope: record.scope,
                });
            }
        }

        None
    }

    /// Check basic auth credentials. Returns true on match.
    /// Basic auth always grants admin scope (no per-database scoping).
    pub fn check_basic(&self, user: &str, password: &str) -> bool {
        match (&self.basic_user, &self.basic_password) {
            (Some(expected_user), Some(expected_pass)) => {
                ct_eq(user.as_bytes(), expected_user.as_bytes())
                    && ct_eq(password.as_bytes(), expected_pass.as_bytes())
            }
            _ => false,
        }
    }
}

/// Constant-time comparison of two byte slices.
#[cfg(feature = "auth")]
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.ct_eq(b).into()
}

#[cfg(all(test, feature = "auth"))]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Role serde round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn role_serde_roundtrip() {
        let scope = TokenScope {
            role: Role::ReadWrite,
            databases: vec!["mydb".to_string()],
        };
        let json = serde_json::to_string(&scope).unwrap();
        assert!(json.contains("\"read-write\""));
        let parsed: TokenScope = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.role, Role::ReadWrite);
    }

    #[test]
    fn role_serde_all_variants() {
        for (role, expected) in [
            (Role::Admin, "admin"),
            (Role::ReadWrite, "read-write"),
            (Role::ReadOnly, "read-only"),
        ] {
            assert_eq!(role_to_str(role), expected);
            assert_eq!(str_to_role(expected).unwrap(), role);
        }
    }

    #[test]
    fn str_to_role_unknown() {
        assert!(str_to_role("superuser").is_err());
    }

    // -----------------------------------------------------------------------
    // TokenInfo::identity
    // -----------------------------------------------------------------------

    #[test]
    fn token_info_identity() {
        let info = TokenInfo {
            id: "tok-1".to_string(),
            name: "my-service".to_string(),
            scope: TokenScope {
                role: Role::ReadOnly,
                databases: vec![],
            },
        };
        let id = info.identity();
        assert_eq!(id.user_id(), "my-service");
        assert!(id.can_read());
        assert!(!id.can_write());
    }

    // -----------------------------------------------------------------------
    // AuthProvider::new
    // -----------------------------------------------------------------------

    #[test]
    fn new_returns_none_when_no_credentials() {
        assert!(AuthProvider::new(None, None, None).is_none());
    }

    #[test]
    fn new_returns_some_with_bearer_token() {
        assert!(AuthProvider::new(Some("tok".into()), None, None).is_some());
    }

    #[test]
    fn new_returns_some_with_basic_credentials() {
        assert!(AuthProvider::new(None, Some("user".into()), Some("pass".into())).is_some());
    }

    #[test]
    fn new_returns_some_with_user_only() {
        assert!(AuthProvider::new(None, Some("user".into()), None).is_some());
    }

    // -----------------------------------------------------------------------
    // is_enabled
    // -----------------------------------------------------------------------

    #[test]
    fn is_enabled_with_bearer() {
        let p = AuthProvider::new(Some("tok".into()), None, None).unwrap();
        assert!(p.is_enabled());
    }

    #[test]
    fn is_enabled_with_basic() {
        let p = AuthProvider::new(None, Some("u".into()), Some("p".into())).unwrap();
        assert!(p.is_enabled());
    }

    // -----------------------------------------------------------------------
    // check_bearer
    // -----------------------------------------------------------------------

    #[test]
    fn check_bearer_exact_match() {
        let p = AuthProvider::new(Some("secret-token".into()), None, None).unwrap();
        assert!(p.check_bearer("secret-token").is_some());
    }

    #[test]
    fn check_bearer_returns_admin_scope() {
        let p = AuthProvider::new(Some("secret-token".into()), None, None).unwrap();
        let info = p.check_bearer("secret-token").unwrap();
        assert_eq!(info.scope.role, Role::Admin);
        assert!(info.scope.databases.is_empty());
    }

    #[test]
    fn check_bearer_mismatch() {
        let p = AuthProvider::new(Some("secret-token".into()), None, None).unwrap();
        assert!(p.check_bearer("wrong-token").is_none());
    }

    #[test]
    fn check_bearer_empty_token() {
        let p = AuthProvider::new(Some("secret-token".into()), None, None).unwrap();
        assert!(p.check_bearer("").is_none());
    }

    #[test]
    fn check_bearer_prefix_not_enough() {
        let p = AuthProvider::new(Some("secret-token".into()), None, None).unwrap();
        assert!(p.check_bearer("secret").is_none());
    }

    #[test]
    fn check_bearer_case_sensitive() {
        let p = AuthProvider::new(Some("Secret".into()), None, None).unwrap();
        assert!(p.check_bearer("secret").is_none());
    }

    #[test]
    fn check_bearer_no_token_configured() {
        let p = AuthProvider::new(None, Some("user".into()), Some("pass".into())).unwrap();
        assert!(p.check_bearer("anything").is_none());
    }

    // -----------------------------------------------------------------------
    // check_basic
    // -----------------------------------------------------------------------

    #[test]
    fn check_basic_exact_match() {
        let p = AuthProvider::new(None, Some("admin".into()), Some("pass123".into())).unwrap();
        assert!(p.check_basic("admin", "pass123"));
    }

    #[test]
    fn check_basic_wrong_password() {
        let p = AuthProvider::new(None, Some("admin".into()), Some("pass123".into())).unwrap();
        assert!(!p.check_basic("admin", "wrong"));
    }

    #[test]
    fn check_basic_wrong_user() {
        let p = AuthProvider::new(None, Some("admin".into()), Some("pass123".into())).unwrap();
        assert!(!p.check_basic("other", "pass123"));
    }

    #[test]
    fn check_basic_both_wrong() {
        let p = AuthProvider::new(None, Some("admin".into()), Some("pass123".into())).unwrap();
        assert!(!p.check_basic("other", "wrong"));
    }

    #[test]
    fn check_basic_empty_credentials() {
        let p = AuthProvider::new(None, Some("admin".into()), Some("pass123".into())).unwrap();
        assert!(!p.check_basic("", ""));
    }

    #[test]
    fn check_basic_no_password_configured() {
        let p = AuthProvider::new(None, Some("admin".into()), None).unwrap();
        assert!(!p.check_basic("admin", "anything"));
    }

    #[test]
    fn check_basic_no_basic_configured() {
        let p = AuthProvider::new(Some("token".into()), None, None).unwrap();
        assert!(!p.check_basic("admin", "pass"));
    }

    // -----------------------------------------------------------------------
    // ct_eq (constant-time comparison)
    // -----------------------------------------------------------------------

    #[test]
    fn ct_eq_identical() {
        assert!(ct_eq(b"hello", b"hello"));
    }

    #[test]
    fn ct_eq_different_content() {
        assert!(!ct_eq(b"hello", b"world"));
    }

    #[test]
    fn ct_eq_different_length() {
        assert!(!ct_eq(b"short", b"longer"));
    }

    #[test]
    fn ct_eq_empty() {
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn ct_eq_one_empty() {
        assert!(!ct_eq(b"", b"a"));
    }
}
