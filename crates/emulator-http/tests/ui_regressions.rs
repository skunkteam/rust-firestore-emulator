#![allow(unused_crate_dependencies)]
#![cfg(feature = "ui")]

use std::{fs, path::Path};

fn extract_css_rule_block<'a>(css: &'a str, selector: &str) -> &'a str {
    let needle = format!("\n{selector} {{");
    let start = if css.starts_with(&format!("{selector} {{")) {
        0
    } else {
        css.find(&needle)
            .map(|idx| idx + 1)
            .unwrap_or_else(|| panic!("missing CSS rule block for `{selector}`"))
    };
    let rest = &css[start..];
    let end = rest
        .find('}')
        .unwrap_or_else(|| panic!("unterminated CSS rule block for `{selector}`"));
    &rest[..=end]
}

fn slice_brace_block(src: &str, start_idx: usize, label: &str) -> String {
    let tail = &src[start_idx..];
    let open_rel = tail
        .find('{')
        .unwrap_or_else(|| panic!("{label} must open a brace block"));
    let mut depth = 0usize;
    for (rel, ch) in tail[open_rel..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return tail[..=open_rel + rel].to_owned();
                }
            }
            _ => {}
        }
    }
    panic!("unterminated brace block for {label}");
}

#[test]
fn bug_web_ui_8_miller_column_header_overflow_css() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css =
        fs::read_to_string(repo_root.join("ui/styles.css")).expect("failed to read ui/styles.css");

    assert!(
        css.contains(".miller-col") && css.contains("overflow: hidden;"),
        "expected .miller-col to enforce overflow: hidden"
    );
    assert!(
        css.contains(".col-header > span")
            && css.contains("min-width: 0;")
            && css.contains("text-overflow: ellipsis;")
            && css.contains("white-space: nowrap;"),
        "expected .col-header > span truncation rules"
    );
    assert!(
        css.contains(".col-header-actions") && css.contains("flex-shrink: 0;"),
        "expected .col-header-actions to enforce flex-shrink: 0"
    );
}

#[test]
fn regression_a11y_miller_columns_forms_and_icon_labels() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css =
        fs::read_to_string(repo_root.join("ui/styles.css")).expect("failed to read ui/styles.css");

    // Preserve bug_web_ui_8 CSS invariants and verify button reset + ellipsis + :focus-visible
    // rules against the exact selector blocks.
    let miller_col_rule = extract_css_rule_block(&css, ".miller-col");
    assert!(miller_col_rule.contains("overflow: hidden;"));
    let col_header_span_rule = extract_css_rule_block(&css, ".col-header > span");
    assert!(col_header_span_rule.contains("min-width: 0;"));
    let col_item_rule = extract_css_rule_block(&css, ".col-item");
    assert!(col_item_rule.contains("border: none;"));
    assert!(col_item_rule.contains("background: transparent;"));
    assert!(col_item_rule.contains("width: 100%;"));
    assert!(col_item_rule.contains("min-width: 0;"));
    assert!(col_item_rule.contains("text-align: left;"));
    let item_label_rule = extract_css_rule_block(&css, ".item-label");
    assert!(item_label_rule.contains("min-width: 0;"));
    assert!(item_label_rule.contains("overflow: hidden;"));
    assert!(item_label_rule.contains("text-overflow: ellipsis;"));
    assert!(css.contains(".col-item:focus-visible"));
}

#[test]
fn regression_contention_wait_elapsed_css() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css =
        fs::read_to_string(repo_root.join("ui/styles.css")).expect("failed to read ui/styles.css");
    assert!(
        css.contains(".wait-elapsed {"),
        "expected .wait-elapsed CSS rule"
    );
    assert!(
        !css.contains(".wait-elapsed:empty::before"),
        "CSS must not own .wait-elapsed text via :empty::before"
    );
    assert!(
        !css.contains("attr(data-wait-label)"),
        "CSS must not duplicate data-wait-label text via content: attr(...)"
    );
}

#[test]
fn regression_editor_narrow_query_overflow_and_combobox_wrap_css() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let css = fs::read_to_string(repo_root.join("ui/styles.css")).expect("read ui/styles.css");

    // Verify `@media (max-width: 900px)` sets `.query-results-pane { overflow: visible; }`
    // strictly AFTER the base `.query-results-pane` rule so it wins the CSS cascade.
    let base_results_idx = css
        .find(".query-results-pane {")
        .expect("base .query-results-pane rule must exist");
    let media_900_idx = css
        .find("@media (max-width: 900px)")
        .expect("@media (max-width: 900px) block must exist");
    assert!(
        media_900_idx > base_results_idx,
        "@media (max-width: 900px) must appear after base .query-results-pane rule to override \
         overflow: hidden"
    );
    let media_block = slice_brace_block(&css, media_900_idx, "@media (max-width: 900px)");
    let nested_workspace_idx = media_block
        .find(".query-workspace {")
        .expect("@media (max-width: 900px) must include .query-workspace");
    let nested_workspace_block = slice_brace_block(
        &media_block,
        nested_workspace_idx,
        "@media (max-width: 900px) .query-workspace",
    );
    assert!(
        nested_workspace_block.contains("grid-template-columns: minmax(0, 1fr);"),
        "@media (max-width: 900px) .query-workspace must set minmax(0, 1fr), \
         got:\n{nested_workspace_block}"
    );
    let nested_results_idx = media_block
        .find(".query-results-pane {")
        .expect("@media (max-width: 900px) must include .query-results-pane");
    let nested_results_block = slice_brace_block(
        &media_block,
        nested_results_idx,
        "@media (max-width: 900px) .query-results-pane",
    );
    assert!(
        nested_results_block.contains("overflow: visible;")
            && nested_results_block.contains("min-width: 0;"),
        "@media (max-width: 900px) .query-results-pane must set min-width: 0 and overflow: \
         visible, got:\n{nested_results_block}"
    );

    let combobox_wrap_idx = css
        .find(".query-row > .combobox-wrap")
        .expect(".query-row > .combobox-wrap rule must exist");
    let combobox_wrap_block =
        slice_brace_block(&css, combobox_wrap_idx, ".query-row > .combobox-wrap");
    assert!(
        combobox_wrap_block.contains(".query-agg-row > .combobox-wrap"),
        "expected .query-agg-row > .combobox-wrap to share rule block, got:\n{combobox_wrap_block}"
    );
    assert!(
        combobox_wrap_block.contains("flex: 1 1 120px;")
            && combobox_wrap_block.contains("min-width: 120px;"),
        "expected .query-row > .combobox-wrap and .query-agg-row > .combobox-wrap flex sizing, \
         got:\n{combobox_wrap_block}"
    );
}

#[test]
fn regression_dark_theme_css_tokens_and_wcag_contrast() {
    use std::collections::{BTreeMap, BTreeSet};

    fn parse_css_tokens(block: &str) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for line in block.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("--")
                && let Some((name, val)) = rest.split_once(':')
            {
                let key = format!("--{}", name.trim());
                let value = val.trim().trim_end_matches(';').trim().to_owned();
                map.insert(key, value);
            }
        }
        map
    }

    fn extract_braced_body(src: &str, open_brace_idx: usize) -> Option<(usize, &str)> {
        let bytes = src.as_bytes();
        if bytes.get(open_brace_idx) != Some(&b'{') {
            return None;
        }
        let mut depth = 0usize;
        for (offset, &b) in bytes[open_brace_idx..].iter().enumerate() {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        let end = open_brace_idx + offset;
                        return Some((end + 1, &src[open_brace_idx + 1..end]));
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn parse_hex_rgb(hex: &str) -> Option<(u8, u8, u8)> {
        let digits = hex.strip_prefix('#')?;
        if digits.len() != 6 {
            return None;
        }
        let r = u8::from_str_radix(&digits[0..2], 16).ok()?;
        let g = u8::from_str_radix(&digits[2..4], 16).ok()?;
        let b = u8::from_str_radix(&digits[4..6], 16).ok()?;
        Some((r, g, b))
    }

    fn relative_luminance(rgb: (u8, u8, u8)) -> f64 {
        fn channel(c: u8) -> f64 {
            let s = f64::from(c) / 255.0;
            if s <= 0.03928 {
                s / 12.92
            } else {
                ((s + 0.055) / 1.055).powf(2.4)
            }
        }
        0.2126 * channel(rgb.0) + 0.7152 * channel(rgb.1) + 0.0722 * channel(rgb.2)
    }

    fn contrast_ratio(fg: &str, bg: &str) -> f64 {
        let fg_rgb = parse_hex_rgb(fg).unwrap_or_else(|| panic!("invalid hex fg: {fg}"));
        let bg_rgb = parse_hex_rgb(bg).unwrap_or_else(|| panic!("invalid hex bg: {bg}"));
        let l1 = relative_luminance(fg_rgb);
        let l2 = relative_luminance(bg_rgb);
        let (hi, lo) = if l1 >= l2 { (l1, l2) } else { (l2, l1) };
        (hi + 0.05) / (lo + 0.05)
    }

    fn scan_stray_color_literals(
        src_path: &str,
        content: &str,
        is_css: bool,
        out: &mut Vec<String>,
    ) {
        let mut in_block_comment = false;
        let mut in_html_comment = false;
        for (idx, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if in_block_comment {
                if trimmed.contains("*/") {
                    in_block_comment = false;
                }
                continue;
            }
            if in_html_comment {
                if trimmed.contains("-->") {
                    in_html_comment = false;
                }
                continue;
            }
            if trimmed.starts_with("/*") {
                if !trimmed.contains("*/") {
                    in_block_comment = true;
                }
                continue;
            }
            if trimmed.starts_with("<!--") {
                if !trimmed.contains("-->") {
                    in_html_comment = true;
                }
                continue;
            }
            if trimmed.starts_with('*') || trimmed.starts_with("//") {
                continue;
            }

            // Check for `#RGB` / `#RRGGBB` / `#RRGGBBAA` hex color literals (excluding CSS ID
            // selectors like `#topbar`, URL fragments like `https://.../#deadbeef`, and JS
            // template strings like `#${id}`).
            let bytes = trimmed.as_bytes();
            for (i, &b) in bytes.iter().enumerate() {
                if b == b'#' {
                    let prev_ok = i == 0
                        || matches!(
                            bytes[i - 1],
                            b' ' | b'\t' | b':' | b',' | b'(' | b'"' | b'\'' | b'`' | b'='
                        );
                    let mut j = i + 1;
                    while j < bytes.len()
                        && (bytes[j].is_ascii_alphanumeric()
                            || bytes[j] == b'_'
                            || bytes[j] == b'$')
                    {
                        j += 1;
                    }
                    let token = &trimmed[i + 1..j];
                    let is_hex_len = matches!(token.len(), 3 | 4 | 6 | 8);
                    let prefix = &trimmed[..i];
                    let in_style_context = if is_css {
                        prefix.contains(':')
                    } else {
                        prefix.contains(':') || prefix.contains('=')
                    };
                    if prev_ok
                        && in_style_context
                        && is_hex_len
                        && token.bytes().all(|c| c.is_ascii_hexdigit())
                    {
                        out.push(format!("{src_path}:{}: {trimmed}", idx + 1));
                    }
                }
            }

            for func in ["rgb(", "rgba(", "hsl(", "hsla("] {
                let mut search_from = 0usize;
                while let Some(rel) = trimmed[search_from..].find(func) {
                    let pos = search_from + rel;
                    let word_boundary = pos == 0
                        || !(bytes[pos - 1].is_ascii_alphanumeric() || bytes[pos - 1] == b'_');
                    if word_boundary {
                        out.push(format!("{src_path}:{}: {trimmed}", idx + 1));
                        break;
                    }
                    search_from = pos + func.len();
                }
            }
        }
    }

    const UI_SOURCE_FILES: &[&str] = &[
        "ui/index.html",
        "ui/styles.css",
        "ui/app.js",
        "ui/components/combobox.js",
        "ui/components/editor.js",
        "ui/components/explorer.js",
        "ui/components/modals.js",
        "ui/components/query.js",
        "ui/components/query-model.js",
        "ui/components/query-suggestions.js",
        "ui/components/contention.js",
    ];

    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let index_html =
        fs::read_to_string(repo_root.join("ui/index.html")).expect("read ui/index.html");
    let head_start = index_html.find("<head>").expect("ui/index.html <head>");
    let head_end = index_html[head_start..]
        .find("</head>")
        .expect("ui/index.html </head>");
    let head_html = &index_html[head_start..head_start + head_end];
    assert!(
        head_html.contains(r#"<meta name="color-scheme" content="light dark" />"#),
        "ui/index.html <head> must declare `<meta name=\"color-scheme\" content=\"light dark\" \
         />` to prevent dark-mode FOUC"
    );

    let css = fs::read_to_string(repo_root.join("ui/styles.css")).expect("read ui/styles.css");

    // 1. Locate top-level `:root { ... }` and `@media (prefers-color-scheme: dark) { :root { ... }
    //    }`, plus semantic rule wiring for `.sse-dot.error` and semantic toasts.
    let light_root_start = css
        .find(":root {")
        .expect("top-level :root block must exist");
    let light_brace = light_root_start + ":root ".len();
    let (light_root_end, light_root_body) =
        extract_braced_body(&css, light_brace).expect("top-level :root must be balanced");
    assert!(
        light_root_body.contains("color-scheme: light dark;"),
        ":root must declare `color-scheme: light dark;`"
    );

    let dark_media_start = css
        .find("@media (prefers-color-scheme: dark)")
        .expect("@media (prefers-color-scheme: dark) block must exist");
    let dark_media_brace = dark_media_start
        + css[dark_media_start..]
            .find('{')
            .expect("@media dark opening brace");
    let (dark_media_end, dark_media_body) =
        extract_braced_body(&css, dark_media_brace).expect("@media dark must be balanced");
    let dark_root_rel = dark_media_body
        .find(":root {")
        .expect("@media (prefers-color-scheme: dark) must contain a :root block");
    let (_, dark_root_body) = extract_braced_body(dark_media_body, dark_root_rel + ":root ".len())
        .expect("dark :root must be balanced");

    let sse_error_start = css
        .find(".sse-dot.error {")
        .expect(".sse-dot.error rule must exist");
    let (_, sse_error_body) = extract_braced_body(&css, sse_error_start + ".sse-dot.error ".len())
        .expect(".sse-dot.error rule must be balanced");
    assert!(
        sse_error_body.contains("background: var(--danger-fg);"),
        ".sse-dot.error must use var(--danger-fg) for dark-mode non-text contrast"
    );

    for (selector, token) in [
        (".toast.error", "--toast-error-border"),
        (".toast.success", "--toast-success-border"),
    ] {
        let rule_sig = format!("{selector} {{");
        let start = css
            .find(&rule_sig)
            .unwrap_or_else(|| panic!("{selector} rule must exist"));
        let (_, body) = extract_braced_body(&css, start + selector.len() + 1)
            .unwrap_or_else(|| panic!("{selector} rule must be balanced"));
        assert!(
            body.contains(&format!("border-color: var({token});")),
            "{selector} must override `border-color: var({token});` so semantic toasts do not \
             inherit --toast-border"
        );
    }

    // 2. Verify zero stray hardcoded hex or rgb/rgba/hsl/hsla color literals outside `:root` token
    //    blocks across CSS, HTML, and Web UI JS modules.
    let mut non_token_css = String::with_capacity(css.len());
    let mut ranges = [
        (light_root_start, light_root_end),
        (dark_media_start, dark_media_end),
    ];
    ranges.sort_by_key(|r| r.0);
    let mut cursor = 0usize;
    for (start, end) in ranges {
        non_token_css.push_str(&css[cursor..start]);
        cursor = end;
    }
    non_token_css.push_str(&css[cursor..]);

    let mut stray_literals = Vec::new();
    for &src_path in UI_SOURCE_FILES {
        if src_path == "ui/styles.css" {
            scan_stray_color_literals(src_path, &non_token_css, true, &mut stray_literals);
        } else {
            let text = fs::read_to_string(repo_root.join(src_path))
                .unwrap_or_else(|e| panic!("failed reading {src_path}: {e}"));
            scan_stray_color_literals(src_path, &text, false, &mut stray_literals);
        }
    }
    assert!(
        stray_literals.is_empty(),
        "found hardcoded color literals outside :root token blocks:\n{}",
        stray_literals.join("\n")
    );

    // 3. Every color/shadow token in light `:root` must have an explicit dark `:root` entry (and
    //    vice versa), and every `var(--...)` referenced across UI files must be defined in `:root`.
    let light_tokens = parse_css_tokens(light_root_body);
    let dark_tokens = parse_css_tokens(dark_root_body);

    for (name, val) in &light_tokens {
        let is_theme_token = val.starts_with('#')
            || val.contains("rgba(")
            || val.contains("rgb(")
            || name.contains("border")
            || name.contains("bg")
            || name.contains("text")
            || name.contains("shadow");
        if is_theme_token && name != "--radius-sm" && name != "--radius-md" && name != "--radius-lg"
        {
            assert!(
                dark_tokens.contains_key(name),
                "theme token {name} (light={val}) is missing from @media (prefers-color-scheme: \
                 dark) :root"
            );
        }
    }
    for (name, val) in &dark_tokens {
        assert!(
            light_tokens.contains_key(name),
            "dark theme token {name} (dark={val}) is missing from top-level :root"
        );
    }

    let mut referenced_vars = BTreeSet::new();
    for &src_path in UI_SOURCE_FILES {
        let text = fs::read_to_string(repo_root.join(src_path))
            .unwrap_or_else(|e| panic!("failed reading {src_path}: {e}"));
        let mut rest = text.as_str();
        while let Some(pos) = rest.find("var(--") {
            let after = &rest[pos + "var(".len()..];
            if let Some(end) = after.find([')', ',', ' ']) {
                referenced_vars.insert(after[..end].trim().to_owned());
            }
            rest = after;
        }
    }
    for var_name in &referenced_vars {
        assert!(
            light_tokens.contains_key(var_name),
            "referenced CSS custom property {var_name} is not defined in :root"
        );
    }

    // 4. Verify WCAG AA (>= 4.5:1) text contrast and WCAG 1.4.11 (>= 3.0:1) non-text UI indicator
    //    contrast for all dark-mode semantic token pairs.
    let dark_pairs = [
        ("--text-main", "--bg-app"),
        ("--text-main", "--bg-surface"),
        ("--text-main", "--bg-surface-alt"),
        ("--text-main", "--bg-card-subtle"),
        ("--text-main", "--bg-subtle"),
        ("--text-main", "--bg-hover"),
        ("--text-main", "--input-error-bg"),
        ("--text-muted", "--bg-app"),
        ("--text-muted", "--bg-surface"),
        ("--text-muted", "--bg-surface-alt"),
        ("--text-muted", "--bg-subtle"),
        ("--text-muted", "--bg-card-subtle"),
        ("--text-faint", "--bg-surface"),
        ("--text-faint", "--bg-surface-alt"),
        ("--text-faint", "--bg-subtle"),
        ("--text-faint", "--bg-card-subtle"),
        ("--text-on-solid", "--primary"),
        ("--text-on-solid", "--primary-hover"),
        ("--text-on-solid", "--accent"),
        ("--text-on-solid", "--accent-hover"),
        ("--text-on-solid", "--danger"),
        ("--text-on-solid", "--danger-hover"),
        ("--primary-link", "--bg-surface"),
        ("--primary-link", "--bg-selected"),
        ("--primary-link", "--bg-card-subtle"),
        ("--danger-fg", "--bg-surface"),
        ("--danger-fg", "--bg-card-subtle"),
        ("--danger-text", "--bg-surface"),
        ("--warning", "--bg-surface"),
        ("--warning", "--bg-card-subtle"),
        ("--primary-text", "--primary-soft"),
        ("--accent-text", "--accent-soft"),
        ("--danger-text", "--danger-soft"),
        ("--warning-text", "--warning-soft"),
        ("--toast-text", "--toast-bg"),
        ("--toast-text", "--toast-error-bg"),
        ("--toast-text", "--toast-success-bg"),
        ("--badge-int-text", "--badge-int-bg"),
        ("--badge-double-text", "--badge-double-bg"),
        ("--badge-string-text", "--badge-string-bg"),
        ("--badge-bool-text", "--badge-bool-bg"),
        ("--badge-null-text", "--badge-null-bg"),
        ("--badge-ts-text", "--badge-ts-bg"),
        ("--badge-ref-text", "--badge-ref-bg"),
        ("--badge-bytes-text", "--badge-bytes-bg"),
        ("--badge-geo-text", "--badge-geo-bg"),
        ("--badge-struct-text", "--badge-struct-bg"),
        ("--badge-vector-text", "--badge-vector-bg"),
    ];

    for (fg_tok, bg_tok) in dark_pairs {
        let fg = dark_tokens
            .get(fg_tok)
            .unwrap_or_else(|| panic!("missing dark token {fg_tok}"));
        let bg = dark_tokens
            .get(bg_tok)
            .unwrap_or_else(|| panic!("missing dark token {bg_tok}"));
        let ratio = contrast_ratio(fg, bg);
        assert!(
            ratio >= 4.5,
            "dark theme contrast for ({fg_tok}={fg} on {bg_tok}={bg}) is {ratio:.2}:1, below WCAG \
             AA 4.5:1"
        );
    }

    let dark_non_text_pairs = [
        ("--success", "--bg-subtle"),
        ("--warning", "--bg-subtle"),
        ("--danger-fg", "--bg-subtle"),
        ("--sse-dot-idle", "--bg-subtle"),
    ];

    for (fg_tok, bg_tok) in dark_non_text_pairs {
        let fg = dark_tokens
            .get(fg_tok)
            .unwrap_or_else(|| panic!("missing dark token {fg_tok}"));
        let bg = dark_tokens
            .get(bg_tok)
            .unwrap_or_else(|| panic!("missing dark token {bg_tok}"));
        let ratio = contrast_ratio(fg, bg);
        assert!(
            ratio >= 3.0,
            "dark theme non-text contrast for ({fg_tok}={fg} on {bg_tok}={bg}) is {ratio:.2}:1, \
             below WCAG 1.4.11 3.0:1"
        );
    }
}
