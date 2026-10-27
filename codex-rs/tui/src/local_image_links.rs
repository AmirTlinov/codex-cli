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
    destinations: Mutex<HashMap<String, Option<ValidatedImage>>>,
}

impl LocalImageLinks {
    /// Callers must establish that the session uses the frontend's filesystem.
    pub(crate) fn new(cwd: &Path) -> Self {
        Self {
            cwd: cwd.to_owned(),
            destinations: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn resolve(&self, destination: &str) -> Option<ValidatedImage> {
        self.destinations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(destination.to_owned())
            .or_insert_with(|| {
                let path = parse_markdown_target(&self.cwd, destination)?;
                ValidatedImage::from_path(&path)
            })
            .clone()
    }
}

/// An existing local raster image with a canonical, validated file URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedImage(Url);

impl ValidatedImage {
    /// Paths are literal filenames; only Markdown targets decode URL escapes.
    pub(crate) fn from_path(path: &Path) -> Option<Self> {
        if !path.is_absolute() || !is_supported_image_path(path) {
            return None;
        }
        // Validate the resolved path too: symlinks can change the type or point to a network share.
        let path = path.canonicalize().ok()?;
        let url = Url::from_file_path(&path).ok()?;
        if url.host_str().is_some() || !is_supported_image_path(&path) || !path.is_file() {
            return None;
        }
        // Desktop openers may use content-based MIME detection instead of the extension.
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
        Some(Self(url))
    }

    pub(crate) fn url(&self) -> &Url {
        &self.0
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

fn is_supported_image_path(path: &Path) -> bool {
    is_image(path)
        && path
            .to_str()
            .is_some_and(|text| !text.starts_with("//") && !text.chars().any(char::is_control))
        && path.components().all(|component| match component {
            std::path::Component::Prefix(prefix) => matches!(
                prefix.kind(),
                std::path::Prefix::Disk(_) | std::path::Prefix::VerbatimDisk(_)
            ),
            std::path::Component::Normal(name) => {
                !cfg!(windows) || !name.to_string_lossy().contains(':')
            }
            _ => true,
        })
}

fn is_local_file_url(url: &Url) -> bool {
    url.scheme() == "file"
        && url.host_str().is_none_or(|host| host == "localhost")
        && url.query().is_none()
        && url.fragment().is_none()
}

fn parse_markdown_target(cwd: &Path, destination: &str) -> Option<PathBuf> {
    if destination.len() > 8 * 1024 || destination.chars().any(char::is_control) {
        return None;
    }
    let path = if destination.starts_with("file:") {
        let url = Url::parse(destination).ok()?;
        if !is_local_file_url(&url) {
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
    if text.starts_with('~')
        || text.starts_with("//")
        || text.starts_with('\\') && !path.is_absolute()
    {
        return None;
    }
    Some(cwd.join(path))
}

/// Deliver opener failures to the owning surface without blocking terminal input.
pub(crate) fn open_link_in_background(
    destination: String,
    on_error: impl FnOnce(String) + Send + 'static,
) -> std::io::Result<()> {
    // Some desktop helpers wait for the viewer to close. A detached thread also keeps that
    // wait out of Tokio runtime shutdown when the user exits the CLI first.
    std::thread::Builder::new()
        .name("codex-link-opener".into())
        .spawn(move || {
            if let Err(error) = open_link(&destination) {
                on_error(format!("Could not open link: {error}"));
            }
        })
        .map(drop)
}

/// Image links use the associated viewer; web links and generated HTML retain browser behavior.
fn open_link(destination: &str) -> std::io::Result<()> {
    let url = Url::parse(destination).map_err(std::io::Error::other)?;
    if let Ok(path) = url.to_file_path()
        && is_image(&path)
    {
        let image = is_local_file_url(&url)
            .then(|| ValidatedImage::from_path(&path))
            .flatten()
            .ok_or_else(|| std::io::Error::other("image is no longer available"))?;
        open_image(image.url())
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
    run_image_opener(std::process::Command::new("xdg-open").arg(url.as_str()))
}

#[cfg(any(all(test, unix), not(any(target_os = "macos", windows))))]
fn run_image_opener(command: &mut std::process::Command) -> std::io::Result<()> {
    let status = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "image viewer exited with {status}"
        )))
    }
}

#[cfg(test)]
mod tests;
