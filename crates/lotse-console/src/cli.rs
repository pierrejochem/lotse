//! Validation and the `lotse-cli` command shown next to every write, so UI
//! and CLI always offer the same operations.

use crate::error::ApiError;
use crate::model::{CreateQueue, Isolation};

/// Checks a stream or queue name: 1–200 of `a-z 0-9 . -`, starting with a
/// letter or digit. Names starting with `__` are internal.
pub fn validate_name(name: &str) -> Result<(), ApiError> {
    let ok_chars = name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-');
    let ok_start = name.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    if name.is_empty() || name.len() > 200 || !ok_chars || !ok_start {
        return Err(ApiError::BadRequest(format!(
            "\"{name}\" is not a valid name. Use 1 to 200 lowercase letters, digits, dots and hyphens, starting with a letter or digit."
        )));
    }
    Ok(())
}

/// Checks a create-queue request.
pub fn validate_create_queue(req: &CreateQueue) -> Result<(), ApiError> {
    validate_name(&req.name)?;
    if !(1..=10).contains(&req.priority_levels) {
        return Err(ApiError::BadRequest("Priority levels must be between 1 and 10.".into()));
    }
    if let Some(dlq) = &req.dlq {
        validate_name(dlq)?;
        if dlq == &req.name {
            return Err(ApiError::BadRequest("A queue cannot be its own dead-letter queue.".into()));
        }
    }
    if req.max_deliveries == Some(0) {
        return Err(ApiError::BadRequest("Max deliveries must be at least 1.".into()));
    }
    if req.max_deliveries.is_some() && req.dlq.is_none() {
        return Err(ApiError::BadRequest("Max deliveries needs a dead-letter queue.".into()));
    }
    Ok(())
}

/// Formats a duration for CLI flags: the largest unit that divides it.
pub fn duration_flag(ms: u64) -> String {
    const UNITS: [(u64, &str); 4] = [(86_400_000, "d"), (3_600_000, "h"), (60_000, "m"), (1_000, "s")];
    for (size, unit) in UNITS {
        if ms >= size && ms % size == 0 {
            return format!("{}{unit}", ms / size);
        }
    }
    format!("{ms}ms")
}

/// `lotse-cli queue create …` for a request.
pub fn create_queue(req: &CreateQueue) -> String {
    let mut cmd = format!("lotse-cli queue create {}", req.name);
    if let Some(tenant) = &req.tenant {
        cmd += &format!(" --tenant {tenant}");
    }
    if req.priority_levels > 1 {
        cmd += &format!(" --priorities {}", req.priority_levels);
    }
    if req.single_active {
        cmd += " --single-active";
    }
    if let Some(ttl) = req.ttl_ms {
        cmd += &format!(" --ttl {}", duration_flag(ttl));
    }
    if let Some(dlq) = &req.dlq {
        cmd += &format!(" --dlq {dlq}");
    }
    if let Some(n) = req.max_deliveries {
        cmd += &format!(" --max-deliveries {n}");
    }
    if req.create_dlq && req.dlq.is_some() {
        cmd += " --create-dlq";
    }
    cmd
}

/// `lotse-cli queue purge …`.
pub fn purge_queue(name: &str) -> String {
    format!("lotse-cli queue purge {name}")
}

/// `lotse-cli queue requeue …`.
pub fn requeue(name: &str) -> String {
    format!("lotse-cli queue requeue {name}")
}

/// `lotse-cli leader move …`, or `unpin`.
pub fn move_leader(req: &crate::model::MoveLeader) -> String {
    match &req.node {
        Some(node) => format!("lotse-cli leader move {} --partition {} --node {node}", req.resource, req.partition),
        None => format!("lotse-cli leader unpin {} --partition {}", req.resource, req.partition),
    }
}

/// `lotse-cli read …` for a browser read.
pub fn read(stream: &str, partition: u32, offset: u64, count: u32, isolation: Isolation) -> String {
    let iso = match isolation {
        Isolation::Committed => "committed",
        Isolation::Uncommitted => "uncommitted",
    };
    format!("lotse-cli read {stream} --partition {partition} --offset {offset} --count {count} --isolation {iso}")
}

/// `lotse-cli user set-password …`.
pub fn set_user_password(name: &str) -> String {
    format!("lotse-cli user set-password {name}")
}

/// `lotse-cli user delete …`.
pub fn delete_user(name: &str) -> String {
    format!("lotse-cli user delete {name}")
}

/// `lotse-cli role bind …`, or `unbind`.
pub fn bind(b: &crate::model::BindingRow, add: bool) -> String {
    let mut cmd = format!(
        "lotse-cli role {} --{} {} --role {}",
        if add { "bind" } else { "unbind" },
        b.subject_kind,
        b.subject,
        b.role
    );
    if !b.tenant.is_empty() {
        cmd += &format!(" --tenant {}", b.tenant);
    }
    if !b.prefix.is_empty() {
        cmd += &format!(" --prefix {}", b.prefix);
    }
    cmd
}

/// `lotse-cli cert revoke …`.
pub fn revoke_certificate(req: &crate::model::RevokeCertificate) -> String {
    format!("lotse-cli cert revoke --issuer {} --serial {} --not-after-ms {}", req.issuer, req.serial, req.not_after_ms)
}

/// `lotse-cli session revoke …`.
pub fn revoke_sessions(subject: &str, group: bool) -> String {
    format!("lotse-cli session revoke {}{subject}", if group { "--group " } else { "" })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> CreateQueue {
        CreateQueue {
            name: "payments.refund".into(),
            tenant: Some("commerce".into()),
            priority_levels: 3,
            single_active: true,
            ttl_ms: Some(86_400_000),
            dlq: Some("payments.refund.dlq".into()),
            max_deliveries: Some(5),
            create_dlq: true,
        }
    }

    #[test]
    fn names() {
        for ok in ["orders", "payments.capture", "a", "9-lives"] {
            assert!(validate_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", "Orders", "__txn_state", ".hidden", "a b", "a/b", &"x".repeat(201)] {
            assert!(validate_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn create_queue_rules() {
        assert!(validate_create_queue(&req()).is_ok());
        assert!(validate_create_queue(&CreateQueue { priority_levels: 0, ..req() }).is_err());
        assert!(validate_create_queue(&CreateQueue { priority_levels: 11, ..req() }).is_err());
        assert!(validate_create_queue(&CreateQueue { dlq: Some("payments.refund".into()), ..req() }).is_err());
        assert!(validate_create_queue(&CreateQueue { dlq: None, ..req() }).is_err());
        assert!(validate_create_queue(&CreateQueue { max_deliveries: Some(0), ..req() }).is_err());
    }

    #[test]
    fn cli_matches_the_dialog() {
        // A day of TTL is written as 1d, the largest whole unit.
        assert_eq!(
            create_queue(&req()),
            "lotse-cli queue create payments.refund --tenant commerce --priorities 3 --single-active --ttl 1d \
             --dlq payments.refund.dlq --max-deliveries 5 --create-dlq"
        );
        let plain = CreateQueue {
            priority_levels: 1,
            single_active: false,
            ttl_ms: None,
            dlq: None,
            max_deliveries: None,
            create_dlq: false,
            ..req()
        };
        assert_eq!(create_queue(&plain), "lotse-cli queue create payments.refund --tenant commerce");
    }

    #[test]
    fn durations() {
        assert_eq!(duration_flag(86_400_000), "1d");
        assert_eq!(duration_flag(90_000_000), "25h");
        assert_eq!(duration_flag(90_000), "90s");
        assert_eq!(duration_flag(1_500), "1500ms");
    }
}
