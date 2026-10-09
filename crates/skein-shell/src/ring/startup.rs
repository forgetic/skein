//! Startup's synchronous bounded regular-file reads (shell.md, 6.1).
//! Every descriptor stays here and is closed before returning bytes or a
//! refusal. No service or ring state is kept.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;

use skein_io::kernel::Error;

use crate::startup::Unread;

use super::{close, last_errno};

pub(crate) fn read_file(path: &Path, max: u32) -> Result<Box<[u8]>, Unread> {
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| Unread::Kernel(Error::InvalidArgument))?;
    // SAFETY: the NUL-terminated path remains live across open.
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOCTTY | libc::O_CLOEXEC) };
    if fd < 0 {
        let errno = last_errno();
        return if errno == libc::ENOENT { Err(Unread::Absent) } else { Err(Unread::Kernel(kernel_error(errno))) };
    }
    let result = read_opened(fd, max);
    close(fd);
    result
}

fn read_opened(fd: i32, max: u32) -> Result<Box<[u8]>, Unread> {
    // SAFETY: all-zero stat storage is valid; fstat initializes this live
    // exclusive local during the call and retains no pointer.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: stat is exclusive initialized storage that lives across fstat.
    if unsafe { libc::fstat(fd, ptr::from_mut(&mut stat)) } < 0 {
        return Err(Unread::Kernel(kernel_error(last_errno())));
    }
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(Unread::NotAFile);
    }
    if stat.st_size > i64::from(max) {
        return Err(Unread::TooLarge { max });
    }
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8 * 1_024];
    loop {
        // SAFETY: the kernel writes only within the live exclusive buffer;
        // the descriptor is ours until the outer function closes it.
        let read = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
        if read < 0 {
            let errno = last_errno();
            if errno == libc::EINTR {
                continue;
            }
            return Err(Unread::Kernel(kernel_error(errno)));
        }
        if read == 0 {
            return Ok(bytes.into_boxed_slice());
        }
        let read = usize::try_from(read).expect("a positive read fits usize");
        let length = bytes.len().checked_add(read).ok_or(Unread::TooLarge { max })?;
        if u64::try_from(length).expect("a length fits u64") > u64::from(max) {
            return Err(Unread::TooLarge { max });
        }
        bytes.extend_from_slice(buffer.get(..read).expect("the kernel read within its buffer"));
    }
}

fn kernel_error(errno: i32) -> Error {
    match errno {
        libc::EACCES | libc::EPERM => Error::Permission,
        libc::ENOTDIR => Error::NotADirectory,
        libc::ENAMETOOLONG => Error::NameTooLong,
        libc::EMFILE | libc::ENFILE => Error::TooManyOpenFiles,
        libc::EINVAL => Error::InvalidArgument,
        libc::ENOBUFS | libc::ENOMEM => Error::NoBufferSpace,
        other => Error::Other(other),
    }
}
