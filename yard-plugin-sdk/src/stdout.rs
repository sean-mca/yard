//! Stdout capture and protocol writer for fd-level stdout redirection.
//!
//! The plugin protocol uses stdout (fd 1) as the response channel between
//! the plugin binary and the yard host process. To prevent handler code
//! from accidentally writing to the protocol channel (e.g. via
//! `println!()`), [`ProtocolWriter::capture`] saves a private copy of
//! fd 1, then redirects fd 1 to stderr via `dup2`. After capture, any
//! `println!()` in handler code writes to stderr, while protocol messages
//! are written to the saved private fd via [`ProtocolWriter::write_line`].

use std::fs::File;
use std::io::Write;

/// Wraps a private file descriptor for writing protocol messages.
///
/// Created by [`capture`](ProtocolWriter::capture), which saves a copy
/// of stdout (fd 1) before redirecting it to stderr. Protocol messages
/// are written to this saved fd, not to the redirected stdout.
pub(crate) struct ProtocolWriter {
    inner: File,
}

impl ProtocolWriter {
    /// Capture stdout by saving fd 1 to a private fd, then redirect
    /// fd 1 to stderr (fd 2) via `dup2`.
    ///
    /// After this call:
    /// - `println!()` and `io::stdout()` write to **stderr**
    /// - [`write_line`](ProtocolWriter::write_line) writes to the
    ///   original stdout (the protocol channel)
    ///
    /// # Errors
    ///
    /// Returns an error if `libc::dup` or `libc::dup2` fails.
    #[allow(unsafe_code)]
    pub(crate) fn capture() -> anyhow::Result<Self> {
        // SAFETY: libc::dup duplicates STDOUT_FILENO to a new fd.
        // The returned fd is valid if >= 0. No memory or aliasing
        // concerns -- this is a POSIX syscall on integer file
        // descriptors.
        let private_fd = unsafe { libc::dup(libc::STDOUT_FILENO) };
        if private_fd < 0 {
            return Err(anyhow::anyhow!(
                "failed to dup stdout (fd 1): errno {}",
                std::io::Error::last_os_error()
            ));
        }

        // SAFETY: libc::dup2 redirects STDOUT_FILENO to point at
        // STDERR_FILENO's underlying file description. After this
        // call, writes to fd 1 go to stderr. The private_fd from
        // the dup() above still points to the original stdout.
        let result = unsafe { libc::dup2(libc::STDERR_FILENO, libc::STDOUT_FILENO) };
        if result < 0 {
            // Close the private fd to avoid leaking it (T-67-03).
            //
            // SAFETY: private_fd is a valid open fd returned by the
            // successful dup() call above; closing it is safe.
            unsafe {
                libc::close(private_fd);
            }
            return Err(anyhow::anyhow!(
                "failed to dup2 stderr -> stdout: errno {}",
                std::io::Error::last_os_error()
            ));
        }

        // SAFETY: private_fd is a valid open file descriptor returned
        // by libc::dup. File::from_raw_fd takes ownership and will
        // close it on drop.
        let file = unsafe { std::os::unix::io::FromRawFd::from_raw_fd(private_fd) };

        Ok(Self { inner: file })
    }

    /// Write a single JSON line to the protocol channel and flush.
    ///
    /// # Errors
    ///
    /// Returns an error if the write or flush fails.
    pub(crate) fn write_line(&mut self, json: &str) -> anyhow::Result<()> {
        writeln!(self.inner, "{json}")?;
        self.inner.flush()?;
        Ok(())
    }
}
