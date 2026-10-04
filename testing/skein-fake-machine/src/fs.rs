//! The filesystem: nodes, the names directories hold, the handles to what
//! is open, and paths resolved beneath a root. Its vocabulary is its own;
//! the face translates (testing-strategy.md, 4).

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, VecDeque};
use alloc::format;
use alloc::vec::Vec;

/// The longest name a directory holds (`NAME_MAX`).
const LONGEST_NAME: usize = 255;

/// A path this long or longer is refused (`PATH_MAX`, its NUL included).
const LONGEST_PATH: usize = 4096;

/// The most symbolic links one resolution follows (Linux's `MAXSYMLINKS`).
const MOST_LINKS: u32 = 40;

/// The largest file the fake disk holds: a write past it finds no space.
const LARGEST_FILE: usize = 64 << 20;

/// The owner's permission bits, which are all the machine checks: the
/// process is the owner of everything beneath its roots.
const READ: u32 = 0o4;
const WRITE: u32 = 0o2;
const SEARCH: u32 = 0o1;

/// What a new file and a new directory are made with, less no umask: the
/// owner's bits of `0o666` and `0o777`.
const NEW_FILE: u32 = READ | WRITE;
const NEW_DIRECTORY: u32 = READ | WRITE | SEARCH;

/// The machine's name for something it opened: a handle, issued once.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Opened(u64);

impl Opened {
    #[must_use]
    pub const fn new(raw: u64) -> Opened {
        Opened(raw)
    }

    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// How something is opened.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum How {
    /// An existing file or directory, to read: a final symbolic link is
    /// followed.
    Read,
    /// An existing directory, to list or to open beneath.
    Directory,
    /// A new, empty file, to write: its name must be free.
    Create,
}

/// What a name names.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Is {
    File,
    Directory,
    Link,
}

/// One entry a `list` hands back: what it is, and its name.
pub type Listed = (Is, Box<[u8]>);

/// What `stat` finds of something open.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Facts {
    pub is: Is,
    /// A file's length; 0 for a directory.
    pub size: u64,
}

/// What the machine refuses, as Linux would beneath a root.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Refusal {
    NotFound,
    Exists,
    NotADirectory,
    IsADirectory,
    NotEmpty,
    Permission,
    /// The fake disk holds no file that large.
    NoSpace,
    /// A loop of symbolic links, or too many in one resolution.
    Loop,
    NameTooLong,
    /// The path leads out of its root.
    Escape,
    /// A directory renamed beneath itself.
    Beneath,
}

/// What a scenario lays beneath a new root: a path relative to it, whose
/// directories come earlier in the list, what it is, and its mode, of which
/// the owner's bits count.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Item {
    pub path: Vec<u8>,
    pub made: Made,
    pub mode: u32,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Made {
    File(Vec<u8>),
    Directory,
    /// A symbolic link, to the path it holds.
    Link(Vec<u8>),
}

impl Item {
    /// A file holding `bytes`, mode `0o644`.
    #[must_use]
    pub fn file(path: &[u8], bytes: &[u8]) -> Item {
        Item { path: path.to_vec(), made: Made::File(bytes.to_vec()), mode: 0o644 }
    }

    /// A directory, mode `0o755`.
    #[must_use]
    pub fn directory(path: &[u8]) -> Item {
        Item { path: path.to_vec(), made: Made::Directory, mode: 0o755 }
    }

    /// A symbolic link to `target`.
    #[must_use]
    pub fn link(path: &[u8], target: &[u8]) -> Item {
        Item { path: path.to_vec(), made: Made::Link(target.to_vec()), mode: 0o777 }
    }

    /// The same, with `mode`.
    #[must_use]
    pub fn mode(self, mode: u32) -> Item {
        Item { mode, ..self }
    }
}

type NodeId = u64;

#[derive(Debug)]
pub(crate) struct Node {
    body: Body,
    /// The owner's permission bits.
    mode: u32,
    /// The directory holding a directory: what `..` leads to.
    parent: Option<NodeId>,
    /// Whether a directory names it. Unnamed, it lives on while open.
    named: bool,
}

#[derive(Debug)]
enum Body {
    File(Vec<u8>),
    Directory { entries: BTreeMap<Box<[u8]>, NodeId>, removed: bool },
    Link(Box<[u8]>),
}

/// What a handle has open, and where a listing of it stopped.
#[derive(Debug)]
struct Handle {
    node: NodeId,
    how: How,
    /// The last name a `list` of this handle handed back.
    listed: Option<Box<[u8]>>,
}

/// Where a path led: the directory its last name is in, that name and what
/// it names; or, for a path that ends at a directory without a name of its
/// own (`.`, `..`), no name and that directory.
#[derive(Debug)]
struct Found {
    parent: NodeId,
    name: Option<Box<[u8]>>,
    node: Option<NodeId>,
    /// The path ended in `/`: what it names must be a directory.
    slash: bool,
}

/// The fake filesystem. See the crate documentation.
#[derive(Debug)]
pub struct Machine {
    pub(crate) nodes: BTreeMap<NodeId, Node>,
    next_node: NodeId,
    handles: BTreeMap<u64, Handle>,
    next_handle: u64,
    /// The directory every laid root hangs from, beyond reach: `..` from a
    /// root is an escape before it is a step up.
    top: NodeId,
    roots: u32,
}

impl Default for Machine {
    fn default() -> Machine {
        Machine::new()
    }
}

impl Machine {
    #[must_use]
    pub fn new() -> Machine {
        let top = Node {
            body: Body::Directory { entries: BTreeMap::new(), removed: false },
            mode: NEW_DIRECTORY,
            parent: None,
            named: true,
        };
        Machine {
            nodes: BTreeMap::from([(0, top)]),
            next_node: 1,
            handles: BTreeMap::new(),
            next_handle: 1,
            top: 0,
            roots: 0,
        }
    }

    /// A new root, laid out as `items` say, opened as a directory: what the
    /// shell opens at startup.
    pub fn lay(&mut self, items: &[Item]) -> Opened {
        let name = format!("root-{}", self.roots).into_bytes().into_boxed_slice();
        self.roots = self.roots.checked_add(1).expect("fewer than 2^32 roots");
        let root = self.make(self.top, name, Body::Directory { entries: BTreeMap::new(), removed: false }, 0o755);
        for item in items {
            let mut parent = root;
            let mut names: Vec<&[u8]> = item.path.split(|&byte| byte == b'/').collect();
            let last = names.pop().expect("an item's path names it");
            for name in names {
                parent = self.lookup(parent, name).expect("an item's directories are laid before it");
            }
            let body = match &item.made {
                Made::File(bytes) => Body::File(bytes.clone()),
                Made::Directory => Body::Directory { entries: BTreeMap::new(), removed: false },
                Made::Link(target) => Body::Link(target.clone().into_boxed_slice()),
            };
            self.make(parent, Box::from(last), body, item.mode);
        }
        self.issue(root, How::Directory)
    }

    /// How many handles are open.
    #[must_use]
    pub fn open_handles(&self) -> usize {
        self.handles.len()
    }

    /// Opens `path`, resolved beneath the directory `root` has open, as
    /// `how` says.
    pub fn open(&mut self, root: Opened, path: &[u8], how: How) -> Result<Opened, Refusal> {
        let root = self.handle(root).node;
        let node = match how {
            How::Read | How::Directory => {
                let found = self.resolve(root, path, false)?;
                let node = found.node.ok_or(Refusal::NotFound)?;
                if (found.slash || how == How::Directory) && !self.is_directory(node) {
                    return Err(Refusal::NotADirectory);
                }
                if !self.may(node, READ) {
                    return Err(Refusal::Permission);
                }
                node
            }
            How::Create => {
                let found = self.resolve(root, path, true)?;
                // A path that ends at a directory of no name of its own
                // (`.`, `a/..`) is taken: O_EXCL answers before O_CREAT.
                let Some(name) = found.name else {
                    return Err(Refusal::Exists);
                };
                if found.node.is_some() {
                    return Err(Refusal::Exists);
                }
                if !self.may(found.parent, WRITE) {
                    return Err(Refusal::Permission);
                }
                self.make(found.parent, name, Body::File(Vec::new()), NEW_FILE << 6_u32)
            }
        };
        Ok(self.issue(node, how))
    }

    /// The bytes of the file `file` has open from `at`, `len` at most:
    /// fewer at its end, none past it.
    pub fn read(&self, file: Opened, at: u64, len: u32) -> Result<Vec<u8>, Refusal> {
        let bytes = match &self.node(self.handle(file).node).body {
            Body::File(bytes) => bytes,
            Body::Directory { .. } => return Err(Refusal::IsADirectory),
            Body::Link(_) => fail("a handle is never to a symbolic link"),
        };
        let start = usize::try_from(at).unwrap_or(usize::MAX).min(bytes.len());
        let end = start.saturating_add(usize::try_from(len).expect("a u32 fits a usize")).min(bytes.len());
        Ok(bytes.get(start..end).expect("within the file").to_vec())
    }

    /// Writes all of `bytes` to the file `file` has open, at `at`, zeros
    /// filling any gap past its end; or none, when the file would grow past
    /// the largest the fake disk holds.
    pub fn write(&mut self, file: Opened, at: u64, bytes: &[u8]) -> Result<(), Refusal> {
        let node = self.handle(file).node;
        let Body::File(contents) = &mut self.node_mut(node).body else {
            fail("only a file is opened to write");
        };
        let start = usize::try_from(at).unwrap_or(usize::MAX);
        let end = start.saturating_add(bytes.len());
        if end > LARGEST_FILE {
            return Err(Refusal::NoSpace);
        }
        if contents.len() < end {
            contents.resize(end, 0);
        }
        contents.get_mut(start..end).expect("resized to hold it").copy_from_slice(bytes);
        Ok(())
    }

    /// What `file` has open: its kind and size.
    #[must_use]
    pub fn stat(&self, file: Opened) -> Facts {
        match &self.node(self.handle(file).node).body {
            Body::File(bytes) => Facts { is: Is::File, size: u64::try_from(bytes.len()).expect("a usize fits a u64") },
            Body::Directory { .. } => Facts { is: Is::Directory, size: 0 },
            Body::Link(_) => fail("a handle is never to a symbolic link"),
        }
    }

    /// Moves the entry `from` of the directory `from_dir` has open to the
    /// name `to` in `to_dir`'s, replacing what `to` named, in the order
    /// Linux checks (`renameat2`).
    pub fn rename(&mut self, from_dir: Opened, from: &[u8], to_dir: Opened, to: &[u8]) -> Result<(), Refusal> {
        let (from_dir, to_dir) = (self.handle(from_dir).node, self.handle(to_dir).node);
        // Both directories are walked to (`filename_parentat`), then the
        // source is looked up, then the target.
        self.searchable(from_dir)?;
        self.searchable(to_dir)?;
        let source = self.look_up(from_dir, from)?.ok_or(Refusal::NotFound)?;
        let target = self.look_up(to_dir, to)?;
        if self.is_directory(source) && self.within(to_dir, source) {
            return Err(Refusal::Beneath);
        }
        if let Some(target) = target
            && self.is_directory(target)
            && self.within(from_dir, target)
        {
            return Err(Refusal::NotEmpty);
        }
        if target == Some(source) {
            return Ok(());
        }
        if !self.may(from_dir, WRITE) || !self.may(to_dir, WRITE) {
            return Err(Refusal::Permission);
        }
        if let Some(target) = target {
            match (self.is_directory(source), self.is_directory(target)) {
                (true, false) => return Err(Refusal::NotADirectory),
                (false, true) => return Err(Refusal::IsADirectory),
                (true, true) | (false, false) => {}
            }
        }
        if self.is_directory(source) && from_dir != to_dir && !self.may(source, WRITE) {
            return Err(Refusal::Permission);
        }
        if let Some(target) = target
            && self.has_entries(target)
        {
            return Err(Refusal::NotEmpty);
        }
        if let Some(target) = target {
            self.unname(to_dir, to, target);
        }
        self.entries_mut(from_dir).remove(from);
        self.entries_mut(to_dir).insert(Box::from(to), source);
        if self.is_directory(source) {
            self.node_mut(source).parent = Some(to_dir);
        }
        Ok(())
    }

    /// Removes the entry `name` of the directory `dir` has open: an empty
    /// directory with `directory`, anything else without (`unlinkat`).
    pub fn remove(&mut self, dir: Opened, name: &[u8], directory: bool) -> Result<(), Refusal> {
        let dir = self.handle(dir).node;
        self.searchable(dir)?;
        let node = self.look_up(dir, name)?.ok_or(Refusal::NotFound)?;
        if !self.may(dir, WRITE) {
            return Err(Refusal::Permission);
        }
        match (directory, self.is_directory(node)) {
            (true, false) => return Err(Refusal::NotADirectory),
            (false, true) => return Err(Refusal::IsADirectory),
            (true, true) | (false, false) => {}
        }
        if self.has_entries(node) {
            return Err(Refusal::NotEmpty);
        }
        self.unname(dir, name, node);
        Ok(())
    }

    /// Makes the directory `name` in the one `dir` has open (`mkdirat`).
    pub fn make_directory(&mut self, dir: Opened, name: &[u8]) -> Result<(), Refusal> {
        let dir = self.handle(dir).node;
        self.searchable(dir)?;
        if self.look_up(dir, name)?.is_some() {
            return Err(Refusal::Exists);
        }
        if !self.may(dir, WRITE) {
            return Err(Refusal::Permission);
        }
        let body = Body::Directory { entries: BTreeMap::new(), removed: false };
        self.make(dir, Box::from(name), body, NEW_DIRECTORY << 6_u32);
        Ok(())
    }

    /// The next entries of the directory `dir` has open, in the order of
    /// their names, after the last a `list` of this handle handed back: at
    /// most `most`, their names `room` bytes together at most.
    pub fn list(&mut self, dir: Opened, most: u32, room: u32) -> Result<Vec<Listed>, Refusal> {
        assert!(usize::try_from(room).is_ok_and(|room| room >= LONGEST_NAME), "a list has room for any name");
        let handle = self.handle(dir);
        let (node, after) = (handle.node, handle.listed.clone());
        let entries = match &self.node(node).body {
            Body::Directory { removed: true, .. } => return Err(Refusal::NotFound),
            Body::Directory { entries, removed: false } => entries,
            Body::File(_) => return Err(Refusal::NotADirectory),
            Body::Link(_) => fail("a handle is never to a symbolic link"),
        };
        let mut listed = Vec::new();
        let mut left = usize::try_from(room).expect("a u32 fits a usize");
        let most = usize::try_from(most).expect("a u32 fits a usize");
        for (name, &child) in entries {
            if after.as_ref().is_some_and(|after| name <= after) {
                continue;
            }
            if listed.len() >= most || name.len() > left {
                break;
            }
            left = left.checked_sub(name.len()).expect("checked to fit");
            listed.push((self.is(child), name.clone()));
        }
        if let Some((_, last)) = listed.last() {
            let last = last.clone();
            self.handles.get_mut(&dir.raw()).expect("an open handle").listed = Some(last);
        }
        Ok(listed)
    }

    /// Closes `file`: the handle is forgotten, and what it had open with it,
    /// if nothing names it any more.
    pub fn close(&mut self, file: Opened) {
        let handle = self.handles.remove(&file.raw()).expect("a handle the machine issued, and open");
        self.release(handle.node);
    }

    /// How `handle` was opened.
    #[must_use]
    pub fn how(&self, handle: Opened) -> How {
        self.handle(handle).how
    }
}

// Resolution, beneath a root.
impl Machine {
    /// Where `path` leads beneath the directory `root`, as `openat2` with
    /// `RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS` resolves it: each
    /// directory on the way searched, `..` never above `root`, no absolute
    /// path or link, links followed, and no more than 40 of them. To
    /// `create` follows no final link, and refuses a final `/` before it
    /// looks the last name up, as `O_CREAT` does.
    fn resolve(&self, root: NodeId, path: &[u8], create: bool) -> Result<Found, Refusal> {
        if path.is_empty() {
            return Err(Refusal::NotFound);
        }
        if path.len() >= LONGEST_PATH {
            return Err(Refusal::NameTooLong);
        }
        if path.first() == Some(&b'/') {
            return Err(Refusal::Escape);
        }
        if !self.is_directory(root) {
            return Err(Refusal::NotADirectory);
        }
        let mut slash = path.last() == Some(&b'/');
        let mut todo = components(path);
        let mut dir = root;
        let mut links = 0_u32;
        while let Some(name) = todo.pop_front() {
            let last = todo.is_empty();
            if !self.may(dir, SEARCH) {
                return Err(Refusal::Permission);
            }
            if &*name == b"." {
                continue;
            }
            if &*name == b".." {
                if dir == root {
                    return Err(Refusal::Escape);
                }
                dir = self.node(dir).parent.expect("a directory beneath a root has a parent");
                continue;
            }
            if last && create && slash {
                return Err(Refusal::IsADirectory);
            }
            // A removed directory holds no name, however long.
            if self.removed(dir) {
                return Err(Refusal::NotFound);
            }
            if name.len() > LONGEST_NAME {
                return Err(Refusal::NameTooLong);
            }
            let Some(node) = self.lookup(dir, &name) else {
                return if last {
                    Ok(Found { parent: dir, name: Some(name), node: None, slash })
                } else {
                    Err(Refusal::NotFound)
                };
            };
            match &self.node(node).body {
                Body::Link(target) if !last || !create => {
                    links = links.checked_add(1).expect("fewer than 2^32 links");
                    if links > MOST_LINKS {
                        return Err(Refusal::Loop);
                    }
                    if target.is_empty() {
                        return Err(Refusal::NotFound);
                    }
                    if target.first() == Some(&b'/') {
                        return Err(Refusal::Escape);
                    }
                    if last && target.last() == Some(&b'/') {
                        slash = true;
                    }
                    let mut expanded = components(target);
                    expanded.extend(todo);
                    todo = expanded;
                }
                Body::Directory { .. } if !last => dir = node,
                Body::File(_) | Body::Link(_) if !last => return Err(Refusal::NotADirectory),
                Body::File(_) | Body::Link(_) | Body::Directory { .. } => {
                    return Ok(Found { parent: dir, name: Some(name), node: Some(node), slash });
                }
            }
        }
        Ok(Found { parent: dir, name: None, node: Some(dir), slash })
    }

    /// Whether a name may be looked up in `dir`: a directory, searched.
    fn searchable(&self, dir: NodeId) -> Result<(), Refusal> {
        if !self.is_directory(dir) {
            return Err(Refusal::NotADirectory);
        }
        if !self.may(dir, SEARCH) {
            return Err(Refusal::Permission);
        }
        Ok(())
    }

    /// What `name` names in the searchable `dir`: nothing in a removed
    /// directory, however long the name, and no name longer than a name may
    /// be.
    fn look_up(&self, dir: NodeId, name: &[u8]) -> Result<Option<NodeId>, Refusal> {
        if self.removed(dir) {
            return Err(Refusal::NotFound);
        }
        if name.len() > LONGEST_NAME {
            return Err(Refusal::NameTooLong);
        }
        Ok(self.lookup(dir, name))
    }

    /// Whether `node` is `dir` or lies beneath it.
    fn within(&self, node: NodeId, dir: NodeId) -> bool {
        let mut at = Some(node);
        while let Some(here) = at {
            if here == dir {
                return true;
            }
            at = self.node(here).parent;
        }
        false
    }
}

// Nodes and handles.
impl Machine {
    fn make(&mut self, parent: NodeId, name: Box<[u8]>, body: Body, mode: u32) -> NodeId {
        let id = self.next_node;
        self.next_node = id.checked_add(1).expect("fewer than 2^64 nodes");
        let parent_of = match body {
            Body::Directory { .. } => Some(parent),
            Body::File(_) | Body::Link(_) => None,
        };
        let owner = (mode >> 6_u32) & 0o7;
        self.nodes.insert(id, Node { body, mode: owner, parent: parent_of, named: true });
        let previous = self.entries_mut(parent).insert(name, id);
        assert!(previous.is_none(), "a name is made only where none is");
        id
    }

    fn issue(&mut self, node: NodeId, how: How) -> Opened {
        let handle = self.next_handle;
        self.next_handle = handle.checked_add(1).expect("fewer than 2^64 handles");
        self.handles.insert(handle, Handle { node, how, listed: None });
        Opened(handle)
    }

    /// Takes `name`, naming `node`, out of `dir`: a directory is then removed
    /// for good, and `node` goes once nothing has it open.
    fn unname(&mut self, dir: NodeId, name: &[u8], node: NodeId) {
        self.entries_mut(dir).remove(name);
        let unnamed = self.node_mut(node);
        unnamed.named = false;
        if let Body::Directory { removed, .. } = &mut unnamed.body {
            *removed = true;
        }
        self.release(node);
    }

    /// Forgets `node` if no name and no handle has it.
    fn release(&mut self, node: NodeId) {
        if self.node(node).named {
            return;
        }
        for handle in self.handles.values() {
            if handle.node == node {
                return;
            }
        }
        self.nodes.remove(&node);
    }

    fn handle(&self, handle: Opened) -> &Handle {
        match self.handles.get(&handle.raw()) {
            Some(handle) => handle,
            None => fail(&format!("{handle:?}: a handle the machine never issued, or closed")),
        }
    }

    fn node(&self, node: NodeId) -> &Node {
        self.nodes.get(&node).expect("a node the machine holds")
    }

    fn node_mut(&mut self, node: NodeId) -> &mut Node {
        self.nodes.get_mut(&node).expect("a node the machine holds")
    }

    fn entries_mut(&mut self, dir: NodeId) -> &mut BTreeMap<Box<[u8]>, NodeId> {
        match &mut self.node_mut(dir).body {
            Body::Directory { entries, .. } => entries,
            Body::File(_) | Body::Link(_) => fail("only a directory holds names"),
        }
    }

    fn lookup(&self, dir: NodeId, name: &[u8]) -> Option<NodeId> {
        match &self.node(dir).body {
            Body::Directory { entries, .. } => entries.get(name).copied(),
            Body::File(_) | Body::Link(_) => None,
        }
    }

    fn is(&self, node: NodeId) -> Is {
        match self.node(node).body {
            Body::File(_) => Is::File,
            Body::Directory { .. } => Is::Directory,
            Body::Link(_) => Is::Link,
        }
    }

    fn is_directory(&self, node: NodeId) -> bool {
        self.is(node) == Is::Directory
    }

    fn removed(&self, node: NodeId) -> bool {
        match self.node(node).body {
            Body::Directory { removed, .. } => removed,
            Body::File(_) | Body::Link(_) => false,
        }
    }

    fn has_entries(&self, node: NodeId) -> bool {
        match &self.node(node).body {
            Body::Directory { entries, .. } => !entries.is_empty(),
            Body::File(_) | Body::Link(_) => false,
        }
    }

    /// Whether the owner may do all of `bits` to `node`.
    fn may(&self, node: NodeId, bits: u32) -> bool {
        self.node(node).mode & bits == bits
    }
}

/// The names of a path, empty ones (`a//b`, a final `/`) left out.
fn components(path: &[u8]) -> VecDeque<Box<[u8]>> {
    let mut names = VecDeque::new();
    for name in path.split(|&byte| byte == b'/') {
        if !name.is_empty() {
            names.push_back(Box::from(name));
        }
    }
    names
}

/// Fails the world on a bug of the machine's client, the simulator.
#[expect(clippy::panic, reason = "the machine checks its client, as a fake does (testing-strategy.md, 4)")]
fn fail(what: &str) -> ! {
    panic!("skein-fake-machine: {what}");
}
