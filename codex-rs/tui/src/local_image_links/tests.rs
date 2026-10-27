use super::*;
use crate::history_cell::AgentMarkdownCell;
use crate::history_cell::HistoryCell;
use crate::history_cell::HistoryRenderMode;
use crate::streaming::controller::StreamController;
use crate::terminal_hyperlinks::HyperlinkLine;
use crate::terminal_hyperlinks::decorate_spans;
use pretty_assertions::assert_eq;

fn image_in(directory: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let path = directory.join(name);
    std::fs::create_dir_all(path.parent().unwrap())?;
    image::RgbImage::new(/*width*/ 1, /*height*/ 1)
        .save_with_format(&path, image::ImageFormat::Png)?;
    Ok(path.canonicalize()?)
}

#[test]
fn image_paths_round_trip_spaces_unicode_and_reserved_characters() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let name = "My Pictures/кадр 01 #100%.PNG";
    let path = image_in(directory.path(), name)?;
    let expected = Url::from_file_path(&path).unwrap();
    let links = LocalImageLinks::new(directory.path());
    for destination in [
        path.to_str().unwrap().to_owned(),
        name.to_owned(),
        format!("./{name}"),
        urlencoding::encode(name).into_owned(),
        expected.to_string(),
    ] {
        assert_eq!(
            links.resolve(&destination),
            Some(expected.clone()),
            "{destination}"
        );
    }
    assert!(expected.as_str().contains("%20"));
    assert!(expected.as_str().contains("%23"));
    assert!(expected.as_str().contains("%25"));
    assert_eq!(expected.to_file_path().unwrap(), path);
    Ok(())
}

#[test]
fn unsafe_missing_and_non_image_targets_stay_unlinked() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    image_in(directory.path(), "script.exe")?;
    image_in(directory.path(), "page.svg")?;
    image_in(directory.path(), "image.png")?;
    std::fs::write(
        directory.path().join("renamed.png"),
        b"#!/bin/sh\necho not-an-image",
    )?;
    std::fs::create_dir(directory.path().join("directory.png"))?;
    let links = LocalImageLinks::new(directory.path());
    for target in [
        "missing.png",
        "script.exe",
        "page.svg",
        "directory.png",
        "renamed.png",
        "https://example.com/image.png",
        "javascript:image.png",
        "data:image/png,abc",
        "file://other-host/share/image.png",
        "//other-host/share/image.png",
        "\\\\other-host\\share\\image.png",
        "image.png#L1",
        "image.png:1",
        "image.png\x1b]8;;https://example.com\x07",
        "image%00.png",
        "image%0a.png",
    ] {
        assert!(links.resolve(target).is_none(), "{target:?}");
    }
    let url = Url::from_file_path(directory.path().join("image.png")).unwrap();
    assert!(links.resolve(&format!("{url}#fragment")).is_none());
    assert!(links.resolve(&format!("{url}?query")).is_none());
    Ok(())
}

#[cfg(unix)]
#[test]
fn image_named_symlink_cannot_open_another_file_type() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let script = image_in(directory.path(), "script.sh")?;
    std::os::unix::fs::symlink(script, directory.path().join("image.png"))?;
    assert!(
        LocalImageLinks::new(directory.path())
            .resolve("image.png")
            .is_none()
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn image_symlinks_cannot_bypass_path_validation() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    for (name, target) in [
        ("newline.png", "line\nbreak.png"),
        ("colon.png", "file:stream.png"),
    ] {
        let target = image_in(directory.path(), target)?;
        std::os::unix::fs::symlink(target, directory.path().join(name))?;
        assert!(
            LocalImageLinks::new(directory.path())
                .resolve(name)
                .is_none()
        );
    }
    Ok(())
}

fn assert_image_links(lines: &[HyperlinkLine], expected: &Url) {
    let linked_rows = lines
        .iter()
        .filter(|line| !line.hyperlinks.is_empty())
        .collect::<Vec<_>>();
    assert!(linked_rows.len() > 1, "fixture must wrap: {lines:?}");
    for line in linked_rows {
        for link in &line.hyperlinks {
            assert_eq!(
                link.terminal_destination().as_deref(),
                Some(expected.as_str())
            );
        }
        let decorated = decorate_spans(line)
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(
            decorated.contains(&format!("\x1b]8;;{expected}\x07")),
            "{decorated:?}"
        );
    }
}

#[test]
fn explicit_image_links_survive_reflow_and_keep_copy_source() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = image_in(directory.path(), "My Pictures/кадр 01.png")?;
    let expected = Url::from_file_path(path).unwrap();
    for markdown in [
        "[A **long** image label with `styled text`](<My Pictures/кадр 01.png>)".to_owned(),
        format!("[My Pictures/кадр 01.png]({expected})"),
        "> [My Pictures/кадр 01.png][image]\n\n[image]: <My Pictures/кадр 01.png>".to_owned(),
        "| Preview |\n| --- |\n| [My Pictures/кадр 01.png](<My Pictures/кадр 01.png>) |".to_owned(),
    ] {
        let cell = AgentMarkdownCell::new(markdown.clone(), directory.path())
            .with_local_image_links(/*enabled*/ true);
        for width in [18, 12, 18] {
            assert_image_links(&cell.display_hyperlink_lines(width), &expected);
        }
        assert_eq!(cell.copy_source(), Some(markdown.as_str()));
        let remote = AgentMarkdownCell::new(markdown, directory.path());
        assert!(
            remote
                .display_hyperlink_lines(/*width*/ 12)
                .iter()
                .all(|line| line.hyperlinks.is_empty())
        );
    }
    Ok(())
}

#[test]
fn streamed_image_links_keep_destinations_through_completion() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = image_in(directory.path(), "My Pictures/кадр 01.png")?;
    let expected = Url::from_file_path(path).unwrap();
    let source = "[My Pictures/кадр 01.png](<My Pictures/кадр 01.png>)";
    let mut stream = StreamController::new(Some(12), directory.path(), HistoryRenderMode::Rich)
        .with_local_image_links(/*enabled*/ true);
    for character in source.chars() {
        stream.push(&character.to_string());
    }
    assert_image_links(&stream.current_tail_lines(), &expected);
    stream.push("\n");
    let (cell, completed) = stream.finalize();
    assert_image_links(
        &cell.unwrap().display_hyperlink_lines(/*width*/ 14),
        &expected,
    );
    assert_eq!(completed.as_deref(), Some(format!("{source}\n").as_str()));
    Ok(())
}

#[test]
fn deleted_image_reports_an_error_without_launching() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = image_in(directory.path(), "image.png")?;
    let url = LocalImageLinks::new(directory.path())
        .resolve("image.png")
        .unwrap();
    std::fs::remove_file(path)?;
    assert!(open_link(url.as_str()).is_err());
    Ok(())
}

#[test]
fn only_a_known_local_execution_host_can_enable_image_links() {
    use crate::app::WindowsSandboxHost;

    for host in [
        WindowsSandboxHost::Unknown,
        WindowsSandboxHost::Remote,
        WindowsSandboxHost::Mixed,
    ] {
        assert!(!enabled_for_session(/*remote_app_server*/ false, host));
    }
    assert!(!enabled_for_session(
        /*remote_app_server*/ true,
        WindowsSandboxHost::Local
    ));
    assert_eq!(
        enabled_for_session(/*remote_app_server*/ false, WindowsSandboxHost::Local),
        !crate::clipboard_copy::is_ssh_session()
    );
}

#[test]
fn persisted_history_does_not_infer_image_origin_from_cwd() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    image_in(directory.path(), "My Pictures/кадр 01.png")?;
    let cwd = codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path(directory.path())?;
    let item = codex_app_server_protocol::ThreadItemPayload::AgentMessage {
        id: "image-message".to_owned(),
        text: "[My Pictures/кадр 01.png](<My Pictures/кадр 01.png>)".to_owned(),
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    };
    let cells = crate::thread_transcript::thread_items_to_transcript_cells(
        /*thread_id*/ None,
        &cwd,
        [item],
        crate::thread_transcript::RawReasoningVisibility::Hidden,
        /*config*/ None,
    );
    assert!(
        cells[0]
            .display_hyperlink_lines(/*width*/ 14)
            .iter()
            .all(|line| line.hyperlinks.is_empty())
    );
    Ok(())
}
