//! Who may do what: roles bound to users and identity-provider groups, per
//! tenant, optionally limited to resources whose names start with a prefix.
//!
//! Pure and total: [`Grants::allows`] looks only at the bindings it is
//! given, so every node, the simulation and the console decide alike.

use std::collections::BTreeSet;

/// What a binding grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// Everything, in every tenant: users, tenants and bindings too.
    ClusterAdmin,
    /// Creating, changing, deleting and binding resources in the tenant,
    /// and everything a producer, consumer and viewer may.
    TenantAdmin,
    /// Writing to resources: produce, publish, transactions.
    Producer,
    /// Reading from resources: fetch, subscribe, acknowledge, consumer
    /// groups.
    Consumer,
    /// Seeing resources: metadata, settings, metrics.
    Viewer,
}

impl Role {
    /// Every role, in wire order.
    pub const ALL: [Role; 5] = [Role::ClusterAdmin, Role::TenantAdmin, Role::Producer, Role::Consumer, Role::Viewer];

    /// The role's code on the wire.
    pub fn code(self) -> u8 {
        self as u8
    }

    /// The role with wire code `c`.
    pub fn from_code(c: u8) -> Option<Role> {
        Role::ALL.get(usize::from(c)).copied()
    }

    /// The name used in documentation and the console.
    pub fn name(self) -> &'static str {
        match self {
            Role::ClusterAdmin => "cluster-admin",
            Role::TenantAdmin => "tenant-admin",
            Role::Producer => "producer",
            Role::Consumer => "consumer",
            Role::Viewer => "viewer",
        }
    }

    /// Whether the role permits `op` (in its scope).
    fn permits(self, op: Operation) -> bool {
        match self {
            Role::ClusterAdmin => true,
            Role::TenantAdmin => op != Operation::ClusterAdmin,
            Role::Producer => matches!(op, Operation::Write | Operation::Describe),
            Role::Consumer => matches!(op, Operation::Read | Operation::Describe),
            Role::Viewer => op == Operation::Describe,
        }
    }
}

/// Who a binding is for.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Subject {
    /// A principal by name.
    User(String),
    /// Every principal whose token lists this group.
    Group(String),
}

/// One grant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoleBinding {
    /// For whom.
    pub subject: Subject,
    /// What.
    pub role: Role,
    /// Where; ignored for [`Role::ClusterAdmin`], which holds everywhere.
    pub tenant: String,
    /// Only resources whose names start with this; empty for all of the
    /// tenant's. Ignored for [`Role::ClusterAdmin`].
    pub prefix: String,
}

/// What a request does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operation {
    /// See a resource.
    Describe,
    /// Read from it.
    Read,
    /// Write to it.
    Write,
    /// Create, change, delete or bind it.
    Admin,
    /// Users, tenants, bindings: cluster-wide administration.
    ClusterAdmin,
}

/// A principal as the bindings see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Who<'a> {
    /// The name.
    pub name: &'a str,
    /// Its groups at the identity provider.
    pub groups: &'a [String],
}

impl Who<'_> {
    fn is(&self, s: &Subject) -> bool {
        match s {
            Subject::User(u) => u == self.name,
            Subject::Group(g) => self.groups.iter().any(|x| x == g),
        }
    }
}

/// The bindings in force.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Grants {
    /// Every binding.
    pub bindings: BTreeSet<RoleBinding>,
}

impl Grants {
    /// Whether `who` may do `op` on the resource `name` of `tenant`. For a
    /// resource about to be created, `name` is the name it will have.
    pub fn allows(&self, who: &Who<'_>, op: Operation, tenant: &str, name: &str) -> bool {
        self.bindings.iter().filter(|b| who.is(&b.subject)).any(|b| {
            b.role.permits(op)
                && (b.role == Role::ClusterAdmin
                    || (op != Operation::ClusterAdmin && b.tenant == tenant && name.starts_with(&b.prefix)))
        })
    }

    /// Whether `who` may do cluster-wide administration.
    pub fn is_cluster_admin(&self, who: &Who<'_>) -> bool {
        self.allows(who, Operation::ClusterAdmin, "", "")
    }

    /// Whether `who` has any role in `tenant` (or everywhere): enough to see
    /// that the tenant exists.
    pub fn knows_tenant(&self, who: &Who<'_>, tenant: &str) -> bool {
        self.bindings.iter().any(|b| who.is(&b.subject) && (b.role == Role::ClusterAdmin || b.tenant == tenant))
    }

    /// The bindings that apply to `who`.
    pub fn of<'a>(&'a self, who: &'a Who<'_>) -> impl Iterator<Item = &'a RoleBinding> + 'a {
        self.bindings.iter().filter(move |b| who.is(&b.subject))
    }
}
