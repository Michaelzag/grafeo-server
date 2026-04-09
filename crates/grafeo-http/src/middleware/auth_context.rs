//! AuthContext extractor — pulls token identity from request extensions.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::error::ApiError;

/// Token identity extracted from request extensions.
///
/// `None` when auth is disabled (no restrictions).
/// `Some(info)` when a token was validated by the auth middleware.
pub struct AuthContext(pub Option<grafeo_service::auth::TokenInfo>);

impl AuthContext {
    /// Check that the token can access the given database. No-op when auth is off.
    pub fn check_db_access(&self, db_name: &str) -> Result<(), ApiError> {
        if let Some(ref info) = self.0 {
            if !info.scope.databases.is_empty()
                && !info.scope.databases.iter().any(|d| d == db_name)
            {
                return Err(ApiError::forbidden(format!(
                    "token not authorized for database '{db_name}'"
                )));
            }
        }
        Ok(())
    }

    /// Check that the token has admin role. No-op when auth is off.
    pub fn check_admin(&self) -> Result<(), ApiError> {
        if let Some(ref info) = self.0 {
            if info.scope.role != "admin" {
                return Err(ApiError::forbidden(
                    "admin access required".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Check that the token can write (admin or read-write). No-op when auth is off.
    pub fn check_write(&self) -> Result<(), ApiError> {
        if let Some(ref info) = self.0 {
            if info.scope.role == "read-only" {
                return Err(ApiError::forbidden(
                    "write access required".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl<S> FromRequestParts<S> for AuthContext
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let info = parts.extensions.get::<grafeo_service::auth::TokenInfo>().cloned();
        Ok(AuthContext(info))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grafeo_service::auth::{TokenInfo, TokenScope};

    fn admin_ctx() -> AuthContext {
        AuthContext(Some(TokenInfo {
            id: "t1".into(),
            name: "admin".into(),
            scope: TokenScope { role: "admin".into(), databases: vec![] },
        }))
    }

    fn scoped_ctx(dbs: Vec<String>) -> AuthContext {
        AuthContext(Some(TokenInfo {
            id: "t2".into(),
            name: "scoped".into(),
            scope: TokenScope { role: "read-write".into(), databases: dbs },
        }))
    }

    fn readonly_ctx() -> AuthContext {
        AuthContext(Some(TokenInfo {
            id: "t3".into(),
            name: "reader".into(),
            scope: TokenScope { role: "read-only".into(), databases: vec![] },
        }))
    }

    #[test]
    fn check_db_access_none_is_noop() {
        AuthContext(None).check_db_access("anything").unwrap();
    }

    #[test]
    fn check_db_access_admin_all_dbs() {
        admin_ctx().check_db_access("any-db").unwrap();
    }

    #[test]
    fn check_db_access_scoped_allowed() {
        scoped_ctx(vec!["mydb".into()]).check_db_access("mydb").unwrap();
    }

    #[test]
    fn check_db_access_scoped_denied() {
        scoped_ctx(vec!["mydb".into()]).check_db_access("other").unwrap_err();
    }

    #[test]
    fn check_admin_none_is_noop() {
        AuthContext(None).check_admin().unwrap();
    }

    #[test]
    fn check_admin_admin_ok() {
        admin_ctx().check_admin().unwrap();
    }

    #[test]
    fn check_admin_readonly_denied() {
        readonly_ctx().check_admin().unwrap_err();
    }

    #[test]
    fn check_write_none_is_noop() {
        AuthContext(None).check_write().unwrap();
    }

    #[test]
    fn check_write_admin_ok() {
        admin_ctx().check_write().unwrap();
    }

    #[test]
    fn check_write_readwrite_ok() {
        scoped_ctx(vec![]).check_write().unwrap();
    }

    #[test]
    fn check_write_readonly_denied() {
        readonly_ctx().check_write().unwrap_err();
    }
}
