//! Local image links require a session on the frontend's host. A displayed cwd alone is not
//! evidence of that: remote sessions can use identical paths on a different machine.
//! Replayed messages do not carry per-message host provenance, so only live replies receive
//! this capability. Their resolved destinations remain stable across scrollback and reflow.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::PoisonError;
use url::Url;

pub(crate) fn enabled_for_session(
    remote_app_server: bool,
    execution_host: crate::app::WindowsSandboxHost,
) -> bool {
    !remote_app_server
        && execution_host == crate::app::WindowsSandboxHost::Local
        && !crate::clipboard_copy::is_ssh_session()
}

/// Resolves explicit Markdown targets once per message, independently of terminal reflow.
#[derive(Debug)]
pub(crate) struct LocalImageLinks {
    cwd: PathBuf,
    destinations: Mutex<HashMap<String, Option<Url>>>,
}

impl LocalImageLinks {
    /// Callers must establish that the session uses the frontend's filesystem.
    pub(crate) fn new(cwd: &Path) -> Self {
        Self {
            cwd: cwd.to_owned(),
            destinations: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn resolve(&self, destination: &str) -> Option<Url> {
        self.destinations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(destination.to_owned())
            .or_insert_with(|| resolve_image(&self.cwd, destination))
            .clone()
    }
}

fn is_image(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" | "tif" | "tiff" | "avif" | "ico"
            )
        })
}

fn resolve_image(cwd: &Path, destination: &str) -> Option<Url> {
    if destination.len() > 8 * 1024 || destination.chars().any(char::is_control) {
        return None;
    }
    let path = if destination.starts_with("file:") {
        let url = Url::parse(destination).ok()?;
        if url.host_str().is_some_and(|host| host != "localhost")
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        url.to_file_path().ok()?
    } else {
        let decoded = urlencoding::decode(destination).ok()?;
        let path = Path::new(decoded.as_ref());
        if !path.is_absolute() && Url::parse(&decoded).is_ok() {
            return None;
        }
        path.to_owned()
    };
    let text = path.to_str()?;
    if text.starts_with(['~', '\\'])
        || text.starts_with("//")
        || text.chars().any(char::is_control)
        || !is_image(&path)
        || path.components().any(|component| matches!(component, std::path::Component::Normal(name) if name.to_string_lossy().contains(':')))
    {
        return None;
    }
    // Canonicalize before exporting a file URI so a symlink cannot target a different file type.
    let path = cwd.join(path).canonicalize().ok()?;
    if !is_image(&path) || !path.is_file() {
        return None;
    }
    // Desktop openers may use content-based MIME detection instead of the filename extension.
    let mut header = Vec::new();
    std::fs::File::open(&path)
        .ok()?
        .take(/*limit*/ 32)
        .read_to_end(&mut header)
        .ok()?;
    if !matches!(
        image::guess_format(&header).ok()?,
        image::ImageFormat::Png
            | image::ImageFormat::Jpeg
            | image::ImageFormat::Gif
            | image::ImageFormat::WebP
            | image::ImageFormat::Bmp
            | image::ImageFormat::Tiff
            | image::ImageFormat::Avif
            | image::ImageFormat::Ico
    ) {
        return None;
    }
    Url::from_file_path(path).ok()
}

/// Image links use the associated viewer; web links and generated HTML retain browser behavior.
pub(crate) fn open_link(destination: &str) -> std::io::Result<()> {
    let url = Url::parse(destination).map_err(std::io::Error::other)?;
    if url.scheme() == "file" && url.to_file_path().ok().is_some_and(|path| is_image(&path)) {
        let image = resolve_image(Path::new(""), destination)
            .ok_or_else(|| std::io::Error::other("image is no longer available"))?;
        open_image(&image)
    } else {
        webbrowser::open(destination)
    }
}

#[cfg(target_os = "macos")]
fn open_image(url: &Url) -> std::io::Result<()> {
    use objc2_app_kit::NSWorkspace;
    use objc2_foundation::NSString;
    use objc2_foundation::NSURL;

    let url = NSURL::URLWithString(&NSString::from_str(url.as_str()))
        .ok_or_else(|| std::io::Error::other("invalid image URL"))?;
    if NSWorkspace::sharedWorkspace().openURL(&url) {
        Ok(())
    } else {
        Err(std::io::Error::other("could not open the image viewer"))
    }
}

#[cfg(windows)]
fn open_image(url: &Url) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::UI::Shell::ShellExecuteW;
    use windows_sys::Win32::UI::WindowsAndMessaging::SW_SHOWNORMAL;

    let path = url
        .to_file_path()
        .map_err(|()| std::io::Error::other("invalid image URL"))?;
    let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    // The path is a NUL-terminated argument, never interpolated into a shell command.
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            std::ptr::null(),
            path.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            SW_SHOWNORMAL,
        )
    } as isize;
    if result > 32 {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "could not open the image viewer ({result})"
        )))
    }
}

#[cfg(not(any(target_os = "macos", windows)))]
fn open_image(url: &Url) -> std::io::Result<()> {
    // A complete URI is one argument, including spaces and non-ASCII filenames.
    let mut child = std::process::Command::new("xdg-open")
        .arg(url.as_str())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    std::thread::spawn(move || match child.wait() {
        Ok(status) if status.success() => {}
        Ok(status) => tracing::warn!(%status, "image opener failed"),
        Err(error) => tracing::warn!(%error, "could not reap image opener"),
    });
    Ok(())
}

#[cfg(test)]
mod tests;
