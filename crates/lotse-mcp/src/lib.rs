//! The tools of `lotse mcp` (ADR 0134): what an AI agent may do with a
//! Lotse cluster over the Model Context Protocol, without the transports.
//!
//! Each tool is one or a few calls of a [`lotse_client::Client`], made as
//! the principal that client signed in as, so the cluster checks every
//! call exactly as it checks the same request over the native protocol:
//! `lotse mcp` holds no rights of its own. Nothing is kept between calls.
//! The crate is generic over the `lotse-rt` traits, so the tools run in the
//! simulation against real nodes; the `lotse` binary adds stdio and
//! Streamable HTTP around [`Tools`].
//!
//! Every outcome is a [`ToolResult`]: a refusal by the cluster or an
//! invalid argument comes back with `is_error` and a text that starts with
//! its code (`UNAUTHORIZED: ...`), never as a panic or a protocol error, so
//! the agent can read it.

use std::fmt;
use std::sync::Arc;

use lotse_client::{Client, ClientError};
use lotse_rt::{Clock, Net, Rng, Spawn};
use serde_json::{Map, Value, json};

mod change;
mod data;
mod destroy;
mod look;
mod tokens;

/// A tool as an MCP client sees it in `tools/list`.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolSpec {
    /// The tool's name.
    pub name: &'static str,
    /// What it does, for the agent.
    pub description: &'static str,
    /// A JSON Schema object for its arguments.
    pub input_schema: Value,
    /// It destroys data (`destructiveHint`).
    pub destructive: bool,
    /// It changes nothing (`readOnlyHint`).
    pub read_only: bool,
}

/// What a tool call gave.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResult {
    /// The result as JSON (`structuredContent`); on an error, `{"error":
    /// {"code", "message"}}`.
    pub structured: Value,
    /// A short text for the agent; on an error, `CODE: message`.
    pub text: String,
    /// Whether the call failed (`isError`).
    pub is_error: bool,
}

/// The tools, on one signed-in client.
pub struct Tools<R: Net + Clock + Spawn + Rng> {
    client: Arc<Client<R>>,
}

impl<R: Net + Clock + Spawn + Rng> fmt::Debug for Tools<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tools").finish_non_exhaustive()
    }
}

impl<R: Net + Clock + Spawn + Rng> Tools<R> {
    /// Tools that act through `client`, as the principal it signed in as.
    pub fn new(client: Arc<Client<R>>) -> Self {
        Tools { client }
    }

    /// Every tool, always in the same order: look, data, change, destroy,
    /// tokens.
    pub fn list() -> Vec<ToolSpec> {
        let mut specs = look::specs();
        specs.extend(data::specs());
        specs.extend(change::specs());
        specs.extend(destroy::specs());
        specs.extend(tokens::specs());
        specs
    }

    /// Runs the tool `name` with `args`, a JSON object (or null for none).
    pub async fn call(&self, name: &str, args: Value) -> ToolResult {
        let outcome = match Self::list().into_iter().find(|s| s.name == name) {
            None => Err(ToolError::invalid(format!("there is no tool named {name:?}"))),
            Some(spec) => match check_args(&spec, &args) {
                Err(e) => Err(e),
                Ok(map) => self.run(name, &Args(&map)).await,
            },
        };
        match outcome {
            Ok(Done { structured, text }) => ToolResult { structured, text, is_error: false },
            Err(e) => ToolResult {
                text: format!("{}: {}", e.code, e.message),
                structured: json!({"error": {"code": e.code, "message": e.message}}),
                is_error: true,
            },
        }
    }

    async fn run(&self, name: &str, a: &Args<'_>) -> Outcome {
        let c = &*self.client;
        match name {
            "cluster_overview" => look::cluster_overview(c).await,
            "list_resources" => look::list_resources(c, a).await,
            "describe_stream" => look::describe_stream(c, a).await,
            "describe_queue" => look::describe_queue(c, a).await,
            "describe_consumer_group" => look::describe_consumer_group(c, a).await,
            "node_metrics" => look::node_metrics(c, a).await,
            "list_sessions" => look::list_sessions(c).await,
            "describe_upgrade" => look::describe_upgrade(c).await,
            "read_stream" => data::read_stream(c, a).await,
            "produce" => data::produce(c, a).await,
            "publish" => data::publish(c, a).await,
            "list_offsets" => data::list_offsets(c, a).await,
            "create_stream" => change::create_stream(c, a).await,
            "create_queue" => change::create_queue(c, a).await,
            "declare_exchange" => change::declare_exchange(c, a).await,
            "bind" => change::bind(c, a, false).await,
            "unbind" => change::bind(c, a, true).await,
            "alter_configs" => change::alter_configs(c, a).await,
            "create_partitions" => change::create_partitions(c, a).await,
            "delete_resource" => destroy::delete_resource(c, a).await,
            "purge_queue" => destroy::purge_queue(c, a).await,
            "requeue_dead_letters" => destroy::requeue_dead_letters(c, a).await,
            "list_tokens" => tokens::list_tokens(c, a).await,
            other => Err(ToolError::invalid(format!("there is no tool named {other:?}"))),
        }
    }
}

/// A tool's successful outcome.
pub(crate) struct Done {
    structured: Value,
    text: String,
}

pub(crate) fn done(structured: Value, text: impl Into<String>) -> Outcome {
    Ok(Done { structured, text: text.into() })
}

pub(crate) type Outcome = Result<Done, ToolError>;

/// Why a tool call failed: a code (the broker's error name, or one of the
/// tools' own: `INVALID_ARGUMENT`, `CONFIRMATION_MISMATCH`) and a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolError {
    pub(crate) code: String,
    pub(crate) message: String,
}

impl ToolError {
    pub(crate) fn new(code: &str, message: impl Into<String>) -> Self {
        ToolError { code: code.into(), message: message.into() }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new("INVALID_ARGUMENT", message)
    }

    pub(crate) fn unknown(message: impl Into<String>) -> Self {
        Self::new("UNKNOWN_RESOURCE", message)
    }
}

impl From<ClientError> for ToolError {
    fn from(e: ClientError) -> Self {
        let code = match &e {
            ClientError::Broker { code, message } => {
                let message = message.clone().unwrap_or_else(|| format!("the broker refused the request ({code})"));
                return ToolError::new(code.name(), message);
            }
            ClientError::OffsetOutOfRange { .. } => "OFFSET_OUT_OF_RANGE",
            ClientError::Unknown(_) => "OUTCOME_UNKNOWN",
            ClientError::MustAbort(_) => "MUST_ABORT",
            ClientError::NotTransactional => "NOT_TRANSACTIONAL",
            ClientError::GaveUp(..) => "GAVE_UP",
            ClientError::UnknownStream(..) => "UNKNOWN_RESOURCE",
            ClientError::Unroutable { .. } => "UNROUTABLE",
            ClientError::NoAnswer(..) => "NO_ANSWER",
            ClientError::ResourceExists { .. } => "RESOURCE_EXISTS",
            ClientError::Authentication(_) => "AUTHENTICATION_FAILED",
            ClientError::NotAllWritten { .. } => "NOT_ALL_WRITTEN",
        };
        ToolError::new(code, e.to_string())
    }
}

/// Checks `args` against the tool's schema as far as names go: an object
/// (null for none), every required argument present, none it does not
/// know. Types are checked where each tool reads its arguments.
pub(crate) fn check_args(spec: &ToolSpec, args: &Value) -> Result<Map<String, Value>, ToolError> {
    let map = match args {
        Value::Object(m) => m.clone(),
        Value::Null => Map::new(),
        other => return Err(ToolError::invalid(format!("arguments must be a JSON object, not {other}"))),
    };
    let props = spec.input_schema["properties"].as_object().cloned().unwrap_or_default();
    if let Some(unknown) = map.keys().find(|k| !props.contains_key(*k)) {
        let known: Vec<&str> = props.keys().map(String::as_str).collect();
        return Err(ToolError::invalid(format!(
            "{} takes no argument {unknown:?}; it takes {}",
            spec.name,
            if known.is_empty() { "none".to_string() } else { known.join(", ") }
        )));
    }
    for r in spec.input_schema["required"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        if map.get(r).is_none_or(Value::is_null) {
            return Err(ToolError::invalid(format!("{} needs the argument {r:?}", spec.name)));
        }
    }
    Ok(map)
}

/// The arguments of one call, read with their types checked.
pub(crate) struct Args<'a>(&'a Map<String, Value>);

impl Args<'_> {
    fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key).filter(|v| !v.is_null())
    }

    pub(crate) fn str(&self, key: &str) -> Result<&str, ToolError> {
        self.opt_str(key)?.ok_or_else(|| ToolError::invalid(format!("{key:?} is required")))
    }

    pub(crate) fn opt_str(&self, key: &str) -> Result<Option<&str>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s)),
            Some(other) => Err(ToolError::invalid(format!("{key:?} must be a string, not {other}"))),
        }
    }

    pub(crate) fn opt_u64(&self, key: &str) -> Result<Option<u64>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(v) => v
                .as_u64()
                .map(Some)
                .ok_or_else(|| ToolError::invalid(format!("{key:?} must be a whole number of 0 or more, not {v}"))),
        }
    }

    pub(crate) fn opt_i64(&self, key: &str) -> Result<Option<i64>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(v) => v
                .as_i64()
                .map(Some)
                .ok_or_else(|| ToolError::invalid(format!("{key:?} must be a whole number, not {v}"))),
        }
    }

    /// A whole number in `min..=max`.
    pub(crate) fn opt_ranged(&self, key: &str, min: u64, max: u64) -> Result<Option<u64>, ToolError> {
        match self.opt_u64(key)? {
            Some(n) if n < min || n > max => {
                Err(ToolError::invalid(format!("{key:?} must be from {min} to {max}, not {n}")))
            }
            n => Ok(n),
        }
    }

    pub(crate) fn opt_u32(&self, key: &str) -> Result<Option<u32>, ToolError> {
        Ok(self.opt_ranged(key, 0, u64::from(u32::MAX))?.map(|n| n as u32))
    }

    pub(crate) fn opt_bool(&self, key: &str) -> Result<Option<bool>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(other) => Err(ToolError::invalid(format!("{key:?} must be true or false, not {other}"))),
        }
    }

    /// An object of string values, in key order.
    pub(crate) fn opt_strings(&self, key: &str) -> Result<Vec<(String, String)>, ToolError> {
        strings(key, self.get(key))
    }

    pub(crate) fn opt_str_list(&self, key: &str) -> Result<Vec<String>, ToolError> {
        match self.get(key) {
            None => Ok(Vec::new()),
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| v.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| ToolError::invalid(format!("{key:?} must be a list of strings"))),
            Some(other) => Err(ToolError::invalid(format!("{key:?} must be a list of strings, not {other}"))),
        }
    }

    pub(crate) fn opt_array(&self, key: &str) -> Result<Option<&Vec<Value>>, ToolError> {
        match self.get(key) {
            None => Ok(None),
            Some(Value::Array(items)) => Ok(Some(items)),
            Some(other) => Err(ToolError::invalid(format!("{key:?} must be a list, not {other}"))),
        }
    }
}

/// `value` as an object of string values, in key order; `None` is empty.
pub(crate) fn strings(key: &str, value: Option<&Value>) -> Result<Vec<(String, String)>, ToolError> {
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Object(m)) => m
            .iter()
            .map(|(k, v)| match v {
                Value::String(s) => Ok((k.clone(), s.clone())),
                other => Err(ToolError::invalid(format!("{key:?}.{k:?} must be a string, not {other}"))),
            })
            .collect(),
        Some(other) => Err(ToolError::invalid(format!("{key:?} must be an object of strings, not {other}"))),
    }
}

/// A tool's spec from its parts; the schema is an object of `props` that
/// takes nothing else.
pub(crate) fn spec(
    name: &'static str,
    description: &'static str,
    props: Value,
    required: &[&str],
    read_only: bool,
    destructive: bool,
) -> ToolSpec {
    ToolSpec {
        name,
        description,
        input_schema: json!({
            "type": "object",
            "properties": props,
            "required": required,
            "additionalProperties": false,
        }),
        destructive,
        read_only,
    }
}

/// The name of a resource kind, as the tools write it.
pub(crate) fn kind_name(kind: u8) -> &'static str {
    match kind {
        lotse_proto::kind::STREAM => "stream",
        lotse_proto::kind::QUEUE => "queue",
        lotse_proto::kind::EXCHANGE => "exchange",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests;
