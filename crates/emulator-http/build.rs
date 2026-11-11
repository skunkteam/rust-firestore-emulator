use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    println!("cargo:rerun-if-changed=../../ui");

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let ui_dir = manifest_dir.join("../../ui");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR not set"));
    let dest = out_dir.join("ui_assets.rs");

    if !ui_dir.exists() {
        fs::write(
            &dest,
            "pub(super) const UI_ASSETS: &[(&str, &str, &[u8])] = &[];\n",
        )
        .expect("failed to write empty ui_assets.rs");
        return;
    }

    let mut entries = Vec::new();
    collect_ui_assets(&ui_dir, &ui_dir, &mut entries);
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = String::from("pub(super) const UI_ASSETS: &[(&str, &str, &[u8])] = &[\n");
    for (rel_path, content_type, abs_path) in entries {
        let abs_str = abs_path
            .to_str()
            .expect("UI asset path must be valid UTF-8");
        out.push_str(&format!(
            "    ({rel_path:?}, {content_type:?}, include_bytes!({abs_str:?})),\n"
        ));
    }
    out.push_str("];\n");

    fs::write(&dest, out).expect("failed to write ui_assets.rs");
}

fn collect_ui_assets(root: &Path, dir: &Path, entries: &mut Vec<(String, &'static str, PathBuf)>) {
    println!("cargo:rerun-if-changed={}", dir.display());
    let mut dir_entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", dir.display()))
        .map(|res| res.expect("failed to read directory entry"))
        .collect();
    dir_entries.sort_by_key(|e| e.file_name());

    for entry in dir_entries {
        let path = entry.path();
        if path.is_dir() {
            collect_ui_assets(root, &path, entries);
            continue;
        }
        println!("cargo:rerun-if-changed={}", path.display());
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        if file_name_str.ends_with(".d.ts") || file_name_str.ends_with(".md") {
            continue;
        }
        let Some(content_type) = mime_for_path(&path) else {
            continue;
        };
        let rel = path
            .strip_prefix(root)
            .expect("asset must reside under ui root");
        let rel_forward = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");
        let canonical = fs::canonicalize(&path).unwrap_or(path);
        entries.push((rel_forward, content_type, canonical));
    }
}

fn mime_for_path(path: &Path) -> Option<&'static str> {
    match path.extension()?.to_str()? {
        "html" => Some("text/html; charset=utf-8"),
        "js" => Some("text/javascript; charset=utf-8"),
        "css" => Some("text/css; charset=utf-8"),
        "svg" => Some("image/svg+xml"),
        _ => None,
    }
}
