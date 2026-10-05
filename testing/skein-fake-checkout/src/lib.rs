//! Deterministic files, scripted commands and local git mechanics for domain
//! worlds (testing-strategy.md, section 4.3; fake-checkout.md, section 1).
//! Extracted from temper's dependency-free fake checkout at commit `246164f`
//! for a second service to share the same mechanics, with behavior preserved.
//!
//! Keeps byte-path nodes, registered roots, file versions and command scripts.
//! Never knows domain types, authorization policy, a service's remote, clocks,
//! delivery tokens or cancellation. Worlds translate their own requests, supply
//! scripts and own scheduling and terminal ledgers. No host IO is performed.
//!
//! Fixtures use `mkdir`, `write`, `link` and `special`; world adapters call
//! `load`, `scan`, `store`, `search`, `spawn` and `finish`. All answers are
//! synchronous. `finish` applies eligible changes each time it is called; it
//! is the world's responsibility to apply a process's effects at the right cut.
//! Root-relative operations follow relative links up to 40, refuse escapes and
//! make stores conditional on versions. This is a small model, without inode,
//! permissions or full POSIX semantics (fake-checkout.md, sections 2 and 3).
//!
//! Ordinary Rust test machinery (programming-model.md, section 10.2): ordered
//! maps and caller-owned fixture sizes, with no production heap bound. `git`
//! snapshots trees and records local objects through a caller's `Remote`;
//! remote policy and transport faults remain outside the kit (section 4).

/// Local working-tree and commit-graph operations, with the remote supplied
/// by the world (fake-checkout.md, section 4).
pub mod git;

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

/// Maximum followed symbolic links in one resolution.
const LINKS: u32 = 40;

/// In-memory byte-path nodes, roots, monotonically issued file versions and
/// command scripts. Fixtures and world adapters share this state; operations
/// are synchronous and never access the host (fake-checkout.md, sections 1–3).
/// Fixture size and the number of roots/writes are caller-owned.
#[derive(Debug, Default)]
pub struct Checkout {
    /// All nodes by global byte path.
    nodes: BTreeMap<Vec<u8>, Node>,
    /// Registered root names mapped to their global paths.
    roots: BTreeMap<u64, Vec<u8>>,
    /// Last issued regular-file version.
    versions: u64,
    /// Scripts selected by exact command bytes.
    programs: BTreeMap<Vec<u8>, Program>,
}

/// A world-supplied command script, cloned when spawned. Duration and output
/// are values for the world to schedule; no time elapses in this kit. Changes
/// use global fixture paths, with `None` deleting a subtree when `finish` is
/// applied (fake-checkout.md, section 3). All sizes are fixture-owned.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Program {
    /// Scripted elapsed time for the world; no clock is consulted (fake-checkout.md, section 3).
    pub duration: Duration,

    /// Complete scripted output; adapters own delivery and truncation (fake-checkout.md, section 3).
    pub output: Vec<u8>,

    /// Scripted final status, interpreted by the world (fake-checkout.md, section 3).
    pub exit: Exit,

    /// Ordered global-path writes or removals, filtered by `finish` against
    /// writable roots and git protection (fake-checkout.md, section 3).
    pub changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

/// Synchronous search result: retained matching lines and an exact count of
/// omitted matches under the requested bounds (fake-checkout.md, section 2).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Found {
    /// Path relative to the searched path, one-based line number and retained
    /// text, in path/line order; bounded by the requested hit and aggregate
    /// path/text byte budgets (fake-checkout.md, section 2).
    pub hits: Vec<(Vec<u8>, usize, Vec<u8>)>,

    /// Matches omitted after a hit/byte budget ran out; a retained text prefix
    /// is still one retained hit (fake-checkout.md, section 2).
    pub more: u64,
}

/// Synchronous search refusal, before any file mutation (fake-checkout.md, section 2).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Searched {
    /// Unsupported pattern with the fixed diagnostic (fake-checkout.md, section 2).
    Unreadable(
        /// Diagnostic bytes for the world’s error channel (fake-checkout.md, section 2).
        Vec<u8>,
    ),

    /// Underlying root/path/file refusal (fake-checkout.md, section 2).
    Failed(
        /// The exact file refusal (fake-checkout.md, section 2).
        Failure,
    ),
}

/// Final status supplied by a script; delivery and terminal uniqueness are
/// the world’s responsibility (fake-checkout.md, section 3).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Exit {
    /// Ordinary exit status (fake-checkout.md, section 3).
    Code(
        /// Status byte supplied by the fixture (fake-checkout.md, section 3).
        u8,
    ),

    /// Scripted signal termination (fake-checkout.md, section 3).
    Signal(
        /// Signal number supplied by the fixture (fake-checkout.md, section 3).
        u8,
    ),
}

/// Spawned script plus captured root paths and write permissions. It contains
/// no running host process or terminal ledger; the world owns its lifecycle
/// (fake-checkout.md, section 3).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Process {
    /// Script snapshot selected by `spawn`, exposed for world scheduling and
    /// completion translation (fake-checkout.md, section 3).
    pub program: Program,
    roots: Vec<(Vec<u8>, bool)>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum Node {
    Directory,
    File { content: Vec<u8>, version: u64 },
    Link { target: Vec<u8> },
    Special,
}

/// Immediate directory-entry kind, reported without following the entry’s
/// link (fake-checkout.md, section 2).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Kind {
    /// Regular file (fake-checkout.md, section 2).
    File,

    /// Directory (fake-checkout.md, section 2).
    Directory,

    /// Symbolic link, target not inspected (fake-checkout.md, section 2).
    Link,

    /// Opaque special node (fake-checkout.md, section 2).
    Special,
}

/// Synchronous file-operation refusal; refused operations do not change
/// files or issue versions (fake-checkout.md, section 2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Failure {
    /// Path or required ancestor is absent (fake-checkout.md, section 2).
    Missing,

    /// Target is not a regular file (fake-checkout.md, section 2).
    NotFile,

    /// Required directory is another node kind (fake-checkout.md, section 2).
    NotDirectory,

    /// Load target exceeds the requested content cap (fake-checkout.md, section 2).
    TooLarge {
        /// Actual content length in bytes (fake-checkout.md, section 2).
        size: u64,
    },

    /// Parent traversal or absolute link target escapes the registered root (fake-checkout.md, section 2).
    Escapes,

    /// Store encountered a symbolic link component (fake-checkout.md, section 2).
    Linked,

    /// Resolution required more than 40 links (fake-checkout.md, section 2).
    Loop,

    /// Store condition differs from the actual regular-file version (fake-checkout.md, section 2).
    Conflict {
        /// Current regular-file version, or `None` if absent (fake-checkout.md, section 2).
        now: Option<u64>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Resolve {
    Follow,
    Store,
}

/// Caller’s compare-and-store condition; failure reports the actual regular
/// file version without mutation (fake-checkout.md, section 2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Expect {
    /// Create only if the target is absent (fake-checkout.md, section 2).
    Absent,

    /// Replace only the specified regular-file version (fake-checkout.md, section 2).
    Is(
        /// Version previously observed by the caller (fake-checkout.md, section 2).
        u64,
    ),
}

/// Bounded sorted immediate entries and the exact count omitted. Each entry
/// is its byte name and kind; no recursive enumeration (fake-checkout.md, section 2).
pub type Listing = (Vec<(Vec<u8>, Kind)>, u64);

impl Checkout {
    /// Empty checkout with no roots, files or scripts; equivalent to `default`
    /// (fake-checkout.md, section 1).
    #[must_use]
    pub fn new() -> Checkout {
        Checkout::default()
    }

    /// Fixture registers `at` as a root after making its directories and returns
    /// a new numeric name. Roots are never closed; invalid fixture paths assert
    /// (fake-checkout.md, section 2).
    pub fn root(&mut self, at: &[u8]) -> u64 {
        self.mkdir(at);
        let root = u64::try_from(self.roots.len()).expect("few roots") + 1;
        self.roots.insert(root, at.to_vec());
        root
    }

    /// Pure borrowed lookup of a root previously returned by `root`; an unknown
    /// root is a client assertion (fake-checkout.md, section 2).
    #[must_use]
    pub fn root_path(&self, root: u64) -> &[u8] {
        self.roots.get(&root).expect("a root the checkout named")
    }

    /// Fixture replaces the script selected by these exact command bytes.
    /// Previously spawned processes retain their snapshots (fake-checkout.md, section 3).
    pub fn program(&mut self, command: &[u8], program: Program) {
        self.programs.insert(command.to_vec(), program);
    }

    /// World adapter resolves `cwd` beneath registered `root`, requiring a
    /// directory, then clones a script and captures the supplied roots’ paths and
    /// writability. `env` prints exactly the given entries; an unknown command
    /// returns a script exiting 127. No changes or time advancement occur.
    /// Unknown root names assert (fake-checkout.md, section 3).
    pub fn spawn(
        &self,
        root: u64,
        cwd: &[u8],
        command: &[u8],
        env: &[(Vec<u8>, Vec<u8>)],
        roots: &[(u64, bool)],
    ) -> Result<Process, Failure> {
        let (at, _) = self.resolve(root, cwd, Resolve::Follow)?;
        match self.nodes.get(&at) {
            None => return Err(Failure::Missing),
            Some(Node::Directory) => {}
            Some(Node::File { .. } | Node::Special) => return Err(Failure::NotDirectory),
            Some(Node::Link { .. }) => unreachable!("links are followed"),
        }
        let program = if command == b"env" {
            let mut output = Vec::new();
            for (name, value) in env {
                output.extend_from_slice(&[&name[..], b"=", value, b"\n"].concat());
            }
            Program { duration: Duration::from_millis(1), output, exit: Exit::Code(0), changes: Vec::new() }
        } else if let Some(program) = self.programs.get(command) {
            program.clone()
        } else {
            let output = [b"sh: ", command, b": not found\n"].concat();
            Program { duration: Duration::from_millis(1), output, exit: Exit::Code(127), changes: Vec::new() }
        };
        let roots = roots.iter().map(|(root, writable)| (self.root_path(*root).to_vec(), *writable)).collect();
        Ok(Process { program, roots })
    }

    /// World adapter applies script changes in order under the deepest captured
    /// writable root, excluding `.git` components in any ASCII case. Repeated
    /// calls reapply changes; scheduling, cancellation and exactly-once effects
    /// are world-owned. Invalid fixture node replacements assert
    /// (fake-checkout.md, section 3).
    pub fn finish(&mut self, process: &Process) {
        for (path, content) in &process.program.changes {
            let deepest = process
                .roots
                .iter()
                .filter(|(root, _)| {
                    root.is_empty()
                        || path.strip_prefix(root.as_slice()).is_some_and(|rest| rest.first() == Some(&b'/'))
                })
                .max_by_key(|(root, _)| root.len());
            let Some((_, true)) = deepest else {
                continue;
            };
            if in_git(path) {
                continue;
            }
            match content {
                Some(content) => drop(self.write(path, content)),
                None => self.remove(path),
            }
        }
    }

    /// World adapter searches literal byte matches in path/line order, skipping
    /// hidden descendants and following no descendant links. Optional glob is a
    /// name suffix with an optional leading `*`, not general glob syntax. At most
    /// the supplied hits and aggregate path/text bytes are retained, with the last
    /// text cut to fit. An unclosed `(` produces the fixed pattern diagnostic.
    /// No mutation; unknown roots assert (fake-checkout.md, section 2).
    pub fn search(
        &self,
        root: u64,
        path: &[u8],
        pattern: &[u8],
        glob: Option<&[u8]>,
        (hits, bytes): (usize, usize),
    ) -> Result<Found, Searched> {
        if pattern.contains(&b'(') && !pattern.contains(&b')') {
            return Err(Searched::Unreadable(b"rg: regex parse error: unclosed group\n".to_vec()));
        }
        let (at, _) = self.resolve(root, path, Resolve::Follow).map_err(Searched::Failed)?;
        let files: Vec<(&Vec<u8>, &Vec<u8>)> = match self.nodes.get(&at) {
            None => return Err(Searched::Failed(Failure::Missing)),
            Some(Node::File { content, .. }) => vec![(&at, content)],
            Some(Node::Special) => Vec::new(),
            Some(Node::Directory) => {
                let prefix = if at.is_empty() { Vec::new() } else { [&at[..], b"/"].concat() };
                let mut files = Vec::new();
                for (file, node) in self.nodes.range(prefix.clone()..) {
                    let Some(beneath) = file.strip_prefix(prefix.as_slice()) else { break };
                    let hidden = beneath.split(|byte| *byte == b'/').any(|name| name.first() == Some(&b'.'));
                    match node {
                        Node::File { content, .. } if !hidden => files.push((file, content)),
                        Node::File { .. } | Node::Directory | Node::Link { .. } | Node::Special => {}
                    }
                }
                files
            }
            Some(Node::Link { .. }) => unreachable!("links are followed"),
        };
        let mut found = Found { hits: Vec::new(), more: 0 };
        let mut left = bytes;
        for (file, content) in files {
            let name = file.rsplit(|byte| *byte == b'/').next().unwrap_or(file);
            if let Some(glob) = glob
                && !name.ends_with(glob.strip_prefix(b"*").unwrap_or(glob))
            {
                continue;
            }
            let beneath = file.strip_prefix(at.as_slice()).unwrap_or(file);
            let beneath = beneath.strip_prefix(b"/").unwrap_or(beneath).to_vec();
            for (index, line) in content.split(|byte| *byte == b'\n').enumerate() {
                if !line.windows(pattern.len().max(1)).any(|window| window == pattern) {
                    continue;
                }
                // A hit costs its path and its text.
                if found.hits.len() >= hits || left <= beneath.len() {
                    found.more += 1;
                    left = 0;
                    continue;
                }
                left -= beneath.len();
                let text = line[..line.len().min(left)].to_vec();
                left -= text.len();
                found.hits.push((beneath.clone(), index + 1, text));
            }
        }
        Ok(found)
    }

    // What anything else on the machine does: an outsider changing the
    // checkout, or a test setting it up.

    /// Fixture makes `at` and its ancestors directories. A conflicting node
    /// asserts; fixture paths are global bytes without a leading slash
    /// (fake-checkout.md, section 2).
    pub fn mkdir(&mut self, at: &[u8]) {
        for end in boundaries(at) {
            match self.nodes.get(&at[..end]) {
                None => drop(self.nodes.insert(at[..end].to_vec(), Node::Directory)),
                Some(Node::Directory) => {}
                Some(node) => panic!("{:?} is a {node:?}, not a directory", String::from_utf8_lossy(&at[..end])),
            }
        }
    }

    /// Fixture makes/replaces a regular file and creates missing parents, issuing
    /// a fresh version even for identical bytes. Replacing other node kinds
    /// asserts; no content bound is imposed (fake-checkout.md, section 2).
    pub fn write(&mut self, at: &[u8], content: &[u8]) -> u64 {
        self.parents(at);
        let version = self.next_version();
        let node = Node::File { content: content.to_vec(), version };
        match self.nodes.insert(at.to_vec(), node) {
            None | Some(Node::File { .. }) => version,
            Some(node) => panic!("{:?} was a {node:?}", String::from_utf8_lossy(at)),
        }
    }

    /// Fixture installs a symbolic link, creating missing parents and replacing
    /// any existing node at that path (fake-checkout.md, section 2).
    pub fn link(&mut self, at: &[u8], target: &[u8]) {
        self.parents(at);
        self.nodes.insert(at.to_vec(), Node::Link { target: target.to_vec() });
    }

    /// Fixture installs an opaque special node, creating missing parents and
    /// replacing any existing node at that path (fake-checkout.md, section 2).
    pub fn special(&mut self, at: &[u8]) {
        self.parents(at);
        self.nodes.insert(at.to_vec(), Node::Special);
    }

    /// Fixture removes the exact global path and its subtree, including local
    /// git objects. Missing paths do nothing; root registrations remain
    /// (fake-checkout.md, section 2).
    pub fn remove(&mut self, at: &[u8]) {
        let beneath = [at, b"/"].concat();
        self.nodes.retain(|path, _| path != at && !path.starts_with(&beneath));
    }

    /// Pure exact-global-path existence query; does not follow links
    /// (fake-checkout.md, section 2).
    #[must_use]
    pub fn exists(&self, at: &[u8]) -> bool {
        self.nodes.contains_key(at)
    }

    /// Pure borrowed regular-file bytes at the exact global path, or `None` for
    /// missing/nonregular nodes; does not follow links (fake-checkout.md, section 2).
    #[must_use]
    pub fn content(&self, at: &[u8]) -> Option<&[u8]> {
        match self.nodes.get(at)? {
            Node::File { content, .. } => Some(content),
            Node::Directory | Node::Link { .. } | Node::Special => None,
        }
    }

    /// Pure regular-file version at the exact global path, or `None` for missing
    /// or nonregular nodes; does not follow links (fake-checkout.md, section 2).
    #[must_use]
    pub fn version(&self, at: &[u8]) -> Option<u64> {
        match self.nodes.get(at)? {
            Node::File { version, .. } => Some(*version),
            Node::Directory | Node::Link { .. } | Node::Special => None,
        }
    }

    /// Pure snapshot of every regular file, including git metadata, in global
    /// path order. Borrows bytes; does not follow links (fake-checkout.md, section 2).
    #[must_use]
    pub fn files(&self) -> BTreeMap<&[u8], &[u8]> {
        let mut files = BTreeMap::new();
        for (path, node) in &self.nodes {
            match node {
                Node::File { content, .. } => drop(files.insert(path.as_slice(), content.as_slice())),
                Node::Directory | Node::Link { .. } | Node::Special => {}
            }
        }
        files
    }

    /// Pure owned snapshot of regular files strictly beneath `at`, keyed by
    /// relative path, including git metadata. Does not follow links or require
    /// `at` to exist (fake-checkout.md, section 2).
    #[must_use]
    pub fn tree(&self, at: &[u8]) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let beneath = [at, b"/"].concat();
        let mut tree = BTreeMap::new();
        for (path, node) in self.nodes.range(beneath.clone()..) {
            let Some(name) = path.strip_prefix(beneath.as_slice()) else {
                break;
            };
            match node {
                Node::File { content, .. } => drop(tree.insert(name.to_vec(), content.clone())),
                Node::Directory | Node::Link { .. } | Node::Special => {}
            }
        }
        tree
    }

    /// Fixture replaces descendants of `at` with the supplied regular-file tree,
    /// preserving git nodes at all depths and their ancestors. Supplied entries
    /// are written with fresh versions; callers supply valid relative paths
    /// (fake-checkout.md, section 2).
    pub fn replace_tree(&mut self, at: &[u8], tree: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let beneath = [at, b"/"].concat();
        let kept: Vec<Vec<u8>> = self
            .nodes
            .keys()
            .filter_map(|path| path.strip_prefix(beneath.as_slice()))
            .filter(|name| in_git(name))
            .map(<[u8]>::to_vec)
            .collect();
        self.nodes.retain(|path, _| match path.strip_prefix(beneath.as_slice()) {
            Some(name) => {
                in_git(name) || kept.iter().any(|git| git.starts_with(name) && git.get(name.len()) == Some(&b'/'))
            }
            None => true,
        });
        for (path, content) in tree {
            self.write(&[at, b"/", path].concat(), content);
        }
    }

    // What io does for the domain, beneath a root.

    /// World adapter follows relative links beneath a registered root and returns
    /// owned content plus version only when content length is at most `max`.
    /// No mutation; unknown roots assert (fake-checkout.md, section 2).
    pub fn load(&self, root: u64, path: &[u8], max: u64) -> Result<(Vec<u8>, u64), Failure> {
        let (at, _) = self.resolve(root, path, Resolve::Follow)?;
        match self.nodes.get(&at) {
            None => Err(Failure::Missing),
            Some(Node::File { content, version }) => {
                let size = u64::try_from(content.len()).expect("a small file");
                if size > max {
                    return Err(Failure::TooLarge { size });
                }
                Ok((content.clone(), *version))
            }
            Some(Node::Directory | Node::Special) => Err(Failure::NotFile),
            Some(Node::Link { .. }) => unreachable!("links are followed"),
        }
    }

    /// World adapter resolves a directory beneath a registered root and returns
    /// at most `max` immediate entries in byte-name order, plus the omitted count.
    /// Entry links are classified, not followed; unknown roots assert
    /// (fake-checkout.md, section 2).
    pub fn scan(&self, root: u64, path: &[u8], max: usize) -> Result<Listing, Failure> {
        let (at, _) = self.resolve(root, path, Resolve::Follow)?;
        match self.nodes.get(&at) {
            None => return Err(Failure::Missing),
            Some(Node::Directory) => {}
            Some(Node::File { .. } | Node::Special) => return Err(Failure::NotDirectory),
            Some(Node::Link { .. }) => unreachable!("links are followed"),
        }
        let prefix = if at.is_empty() { Vec::new() } else { [&at[..], b"/"].concat() };
        let mut entries = Vec::new();
        let mut more = 0;
        for (path, node) in self.nodes.range(prefix.clone()..) {
            let Some(name) = path.strip_prefix(prefix.as_slice()) else { break };
            if name.is_empty() || name.contains(&b'/') {
                continue;
            }
            if entries.len() < max {
                entries.push((name.to_vec(), kind(node)));
            } else {
                more += 1;
            }
        }
        Ok((entries, more))
    }

    /// World adapter conditionally writes beneath a registered root, following
    /// no link component. Only after the version condition succeeds are missing
    /// parents created and a fresh version issued. Refusal leaves all nodes and
    /// versions unchanged; unknown roots assert (fake-checkout.md, section 2).
    pub fn store(&mut self, root: u64, path: &[u8], content: &[u8], expect: Expect) -> Result<u64, Failure> {
        let (at, missing) = self.resolve(root, path, Resolve::Store)?;
        let now = match self.nodes.get(&at) {
            None => None,
            Some(Node::File { version, .. }) => Some(*version),
            Some(Node::Directory | Node::Special) => return Err(Failure::NotFile),
            Some(Node::Link { .. }) => unreachable!("a store refuses links"),
        };
        let expected = match expect {
            Expect::Absent => None,
            Expect::Is(version) => Some(version),
        };
        if now != expected {
            return Err(Failure::Conflict { now });
        }
        for directory in missing {
            self.nodes.insert(directory, Node::Directory);
        }
        let version = self.next_version();
        self.nodes.insert(at, Node::File { content: content.to_vec(), version });
        Ok(version)
    }

    /// Pure resolution, collecting missing store parents without installing them.
    fn resolve(&self, root: u64, path: &[u8], resolve: Resolve) -> Result<(Vec<u8>, Vec<Vec<u8>>), Failure> {
        let base = self.roots.get(&root).expect("a root the checkout named");
        let mut names: Vec<Vec<u8>> = Vec::new();
        let mut todo: VecDeque<Vec<u8>> = parts(path).into();
        let mut missing = Vec::new();
        let mut links = 0;
        while let Some(name) = todo.pop_front() {
            match name.as_slice() {
                b"" | b"." => continue,
                b".." => {
                    names.pop().ok_or(Failure::Escapes)?;
                    continue;
                }
                _ => {}
            }
            let last = todo.is_empty();
            let at = absolute(base, &names, &name);
            match self.nodes.get(&at) {
                None => {
                    // What is missing at the end is what the operation is
                    // about; on the way, it is a directory to make.
                    if !last && !missing.contains(&at) {
                        match resolve {
                            Resolve::Follow => return Err(Failure::Missing),
                            Resolve::Store => missing.push(at),
                        }
                    }
                }
                Some(Node::Directory) => {}
                Some(Node::File { .. } | Node::Special) => {
                    if !last {
                        return Err(Failure::NotDirectory);
                    }
                }
                Some(Node::Link { target }) => {
                    match resolve {
                        Resolve::Follow => {}
                        Resolve::Store => return Err(Failure::Linked),
                    }
                    links += 1;
                    if links > LINKS {
                        return Err(Failure::Loop);
                    }
                    if target.first() == Some(&b'/') {
                        return Err(Failure::Escapes);
                    }
                    for part in parts(target).into_iter().rev() {
                        todo.push_front(part);
                    }
                    continue;
                }
            }
            names.push(name);
        }
        Ok((absolute(base, &names, b""), missing))
    }

    /// Make ancestors of a fixture node.
    fn parents(&mut self, at: &[u8]) {
        if let Some(slash) = at.iter().rposition(|byte| *byte == b'/') {
            self.mkdir(&at[..slash]);
        }
    }

    fn next_version(&mut self) -> u64 {
        self.versions += 1;
        self.versions
    }
}

fn kind(node: &Node) -> Kind {
    match node {
        Node::Directory => Kind::Directory,
        Node::File { .. } => Kind::File,
        Node::Link { .. } => Kind::Link,
        Node::Special => Kind::Special,
    }
}

/// Pure byte-path predicate for a `.git` component in any ASCII case;
/// used by scripted writes and local git snapshots (fake-checkout.md, sections 2–4).
#[must_use]
pub fn in_git(path: &[u8]) -> bool {
    path.split(|byte| *byte == b'/').any(|name| name.eq_ignore_ascii_case(b".git"))
}

fn parts(path: &[u8]) -> Vec<Vec<u8>> {
    path.split(|byte| *byte == b'/').map(<[u8]>::to_vec).collect()
}

fn boundaries(at: &[u8]) -> Vec<usize> {
    let mut ends: Vec<usize> = at.iter().enumerate().filter(|(_, byte)| **byte == b'/').map(|(end, _)| end).collect();
    ends.push(at.len());
    ends
}

fn absolute(base: &[u8], names: &[Vec<u8>], name: &[u8]) -> Vec<u8> {
    let mut parts: Vec<&[u8]> = Vec::new();
    for part in std::iter::once(base).chain(names.iter().map(Vec::as_slice)).chain(std::iter::once(name)) {
        if !part.is_empty() {
            parts.push(part);
        }
    }
    parts.join(&b'/')
}
