use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use reqwest::Client;
use scraper::{Html, Selector, ElementRef};
use url::Url;
use pdf_extract::extract_text;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;
use log::{info, error, warn};
use std::time::{Duration, Instant};
use actix_cors::Cors;
use dotenv::dotenv;

mod supercrawler;
use crate::supercrawler::{
    AppState as SuperCrawlerState,
    background_crawler as super_background_crawler,
    super_crawl
};

#[derive(Serialize, Deserialize)]
struct CrawlRequest {
    domains: Vec<String>,
    max_depth: Option<usize>,
}

#[derive(Serialize)]
struct CrawlResponse {
    message: String,
    logs: Vec<String>,
    mdx_files: Vec<(String, String)>,
}

struct CrawlerState {
    client: Client,
    semaphore: Arc<Semaphore>,
    logs: Arc<tokio::sync::Mutex<Vec<String>>>,
    bg_tx: mpsc::Sender<(String, usize, usize, String)>,
}

/// Converts an HTML element's inline content to Markdown syntax, handling nesting.
fn element_to_markdown(element: ElementRef) -> String {
    let mut markdown = String::new();
    for node in element.children() {
        if let Some(text) = node.value().as_text() {
            markdown.push_str(&text.to_string());
        } else if let Some(child_elem) = node.value().as_element() {
            if let Some(child_ref) = ElementRef::wrap(node) {
                match child_elem.name() {
                    "a" => {
                        let href = child_elem.attr("href").unwrap_or("#");
                        let link_text = element_to_markdown(child_ref).trim().to_string();
                        if !link_text.is_empty() {
                            markdown.push_str(&format!("[{}]({})", link_text, href));
                        }
                    }
                    "strong" | "b" => {
                        let inner = element_to_markdown(child_ref);
                        if !inner.trim().is_empty() {
                            markdown.push_str(&format!("**{}**", inner.trim()));
                        }
                    }
                    "em" | "i" => {
                        let inner = element_to_markdown(child_ref);
                        if !inner.trim().is_empty() {
                            markdown.push_str(&format!("*{}*", inner.trim()));
                        }
                    }
                    "code" => {
                        let inner_text = child_ref.text().collect::<String>().trim().to_string();
                        if !inner_text.is_empty() {
                            markdown.push_str(&format!("`{}`", inner_text));
                        }
                    }
                    "span" => {
                        markdown.push_str(&element_to_markdown(child_ref));
                    }
                    "br" => {
                        markdown.push_str("\n");
                    }
                    "p" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "ul" | "ol" | "li" | "pre" | "div" | "img" | "table" | "thead" | "tbody" | "tr" | "th" | "td" | "blockquote" => {
                    }
                    _ => {
                        markdown.push_str(&element_to_markdown(child_ref));
                    }
                }
            }
        }
    }
    markdown.trim().to_string()
}

/// Processes an HTML element and converts it to MDX, preserving document structure.
fn process_element(element: ElementRef) -> String {
    let tag = element.value().name();
    match tag {
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
            let level = tag[1..].parse::<usize>().unwrap_or(1);
            let text = element_to_markdown(element);
            if text.is_empty() { String::new() } else { format!("{} {}\n\n", "#".repeat(level), text) }
        }
        "p" => {
            let text = element_to_markdown(element);
            if text.is_empty() { String::new() } else { format!("{}\n\n", text) }
        }
        "ul" => {
            let mut list_mdx = String::new();
            for li in element.select(&Selector::parse("li").unwrap()) {
                let li_text = element_to_markdown(li);
                if !li_text.is_empty() {
                    let indent = "";
                    list_mdx.push_str(&format!("{}{} {}\n", indent, "*", li_text));
                }
            }
            if list_mdx.is_empty() { String::new() } else { format!("{}\n", list_mdx) }
        }
        "ol" => {
            let mut list_mdx = String::new();
            let mut item_count = 1;
            for li in element.select(&Selector::parse("li").unwrap()) {
                let li_text = element_to_markdown(li);
                if !li_text.is_empty() {
                    let indent = "";
                    list_mdx.push_str(&format!("{}{}. {}\n", indent, item_count, li_text));
                    item_count += 1;
                }
            }
            if list_mdx.is_empty() { String::new() } else { format!("{}\n", list_mdx) }
        }
        "pre" => {
            if let Some(code) = element.select(&Selector::parse("code").unwrap()).next() {
                let class_attr = code.value().attr("class").unwrap_or("");
                let language = {
                    let re = Regex::new(r"(?:lang|language)-(\w+)").unwrap();
                    if let Some(caps) = re.captures(class_attr) {
                        caps.get(1).map_or("", |m| m.as_str()).to_lowercase()
                    } else if class_attr.contains("jsx") || class_attr.contains("javascript") {
                        "jsx".to_string()
                    } else if class_attr.contains("typescript") || class_attr.contains("ts") {
                        "tsx".to_string()
                    } else if class_attr.contains("bash") || class_attr.contains("shell") || class_attr.contains("sh") {
                        "bash".to_string()
                    } else if class_attr.contains("python") || class_attr.contains("py") {
                        "python".to_string()
                    } else if class_attr.contains("rust") || class_attr.contains("rs") {
                        "rust".to_string()
                    } else if class_attr.contains("html") {
                        "html".to_string()
                    } else if class_attr.contains("css") {
                        "css".to_string()
                    } else {
                        "".to_string()
                    }
                };

                let code_text = code.text().collect::<String>();
                let trimmed_code = code_text.trim();

                if trimmed_code.is_empty() {
                    String::new()
                } else {
                    let lang_hint = if language.is_empty() &&
                        (trimmed_code.contains("import React") || trimmed_code.contains("from 'react'") || trimmed_code.contains("next/")) {
                        "jsx"
                    } else {
                        &language
                    };
                    format!("```{}\n{}\n```\n\n", lang_hint, trimmed_code)
                }
            } else {
                let pre_text = element.text().collect::<String>();
                let trimmed_pre = pre_text.trim();
                if trimmed_pre.is_empty() { String::new() } else { format!("```\n{}\n```\n\n", trimmed_pre) }
            }
        }
        "img" => {
            let src = element.value().attr("src").unwrap_or("");
            let alt = element.value().attr("alt").unwrap_or("");
            if src.is_empty() || src == "/" {
                String::new()
            } else {
                format!("![{}]({})\n\n", alt, src)
            }
        }
        "a" => {
            let href = element.value().attr("href").unwrap_or("#");
            let text = element_to_markdown(element);
            if text.is_empty() || href == "#" {
                String::new()
            } else {
                format!("[{}]({})\n\n", text, href)
            }
        }
        "blockquote" => {
            let inner_content = element.children()
                .filter_map(ElementRef::wrap)
                .map(|e| process_element(e))
                .collect::<String>();

            let trimmed_inner = inner_content.trim();
            if trimmed_inner.is_empty() {
                String::new()
            } else {
                let quoted_lines = trimmed_inner.lines()
                    .map(|line| format!("> {}", line))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{}\n\n", quoted_lines)
            }
        }
        "table" => {
            let mut table_mdx = String::new();
            let mut headers = Vec::new();
            let mut rows = Vec::new();

            if let Some(thead) = element.select(&Selector::parse("thead").unwrap()).next() {
                if let Some(tr) = thead.select(&Selector::parse("tr").unwrap()).next() {
                    for th in tr.select(&Selector::parse("th, td").unwrap()) {
                        headers.push(element_to_markdown(th));
                    }
                }
            } else if let Some(tr) = element.select(&Selector::parse("tr").unwrap()).next() {
                let is_likely_header = tr.select(&Selector::parse("th").unwrap()).next().is_some();
                if is_likely_header {
                    for th in tr.select(&Selector::parse("th, td").unwrap()) {
                        headers.push(element_to_markdown(th));
                    }
                }
            }

            let body_selector = Selector::parse("tbody").unwrap();
            let row_selector = Selector::parse("tr").unwrap();
            let cell_selector = Selector::parse("td, th").unwrap();

            let rows_container = element.select(&body_selector).next().unwrap_or(element);

            let mut skip_first_row = !headers.is_empty() && element.select(&Selector::parse("thead").unwrap()).next().is_none();

            for tr in rows_container.select(&row_selector) {
                if skip_first_row {
                    skip_first_row = false;
                    continue;
                }
                let mut row = Vec::new();
                for cell in tr.select(&cell_selector) {
                    row.push(element_to_markdown(cell));
                }
                if !row.is_empty() {
                    rows.push(row);
                }
            }

            if !headers.is_empty() && !rows.is_empty() {
                table_mdx.push_str(&format!("| {} |\n", headers.join(" | ")));
                table_mdx.push_str(&format!("| {} |\n", headers.iter().map(|_| "---").collect::<Vec<_>>().join(" | ")));
                for row in rows {
                    let padded_row: Vec<String> = headers.iter().enumerate().map(|(i, _)| {
                        row.get(i).cloned().unwrap_or_default()
                    }).collect();
                    table_mdx.push_str(&format!("| {} |\n", padded_row.join(" | ")));
                }
                table_mdx.push_str("\n");
            } else if !rows.is_empty() {
                for row in rows {
                    table_mdx.push_str(&format!("| {} |\n", row.join(" | ")));
                }
                table_mdx.push_str("\n");
            }

            table_mdx
        }
        "div" | "section" | "article" | "main" | "header" | "aside" => {
            let mut mdx = String::new();
            for child in element.children() {
                if let Some(child_elem) = ElementRef::wrap(child) {
                    mdx.push_str(&process_element(child_elem));
                } else if let Some(text) = child.value().as_text() {
                    let trimmed_text = text.trim();
                    if !trimmed_text.is_empty() {
                        mdx.push_str(trimmed_text);
                        mdx.push_str("\n\n");
                    }
                }
            }
            mdx
        }
        "nav" | "footer" | "script" | "style" | "noscript" | "iframe" | "form" | "button" | "input" | "textarea" | "select" | "option" | "label" | "svg" | "canvas" | "audio" | "video" => {
            String::new()
        }
        _ => {
            let mut mdx = String::new();
            for child in element.children() {
                if let Some(child_elem) = ElementRef::wrap(child) {
                    mdx.push_str(&process_element(child_elem));
                } else if let Some(text) = child.value().as_text() {
                    let trimmed_text = text.trim();
                    if !trimmed_text.is_empty() {
                        mdx.push_str(trimmed_text);
                        mdx.push_str("\n\n");
                    }
                }
            }
            mdx
        }
    }
}

/// Converts raw content (HTML or plain text) to structured MDX.
fn clean_to_mdx(content: &str, source_url: &str) -> String {
    let re_noise = Regex::new(r"(?i)<script.*?</script>|<style.*?</style>|<noscript.*?</noscript>|<!--.*?-->").unwrap();
    let clean = re_noise.replace_all(content, "").to_string();
    let doc = Html::parse_document(&clean);

    let mut mdx = String::new();

    let title_selector = Selector::parse("title").unwrap();
    let meta_desc_selector = Selector::parse("meta[name='description'], meta[property='og:description'], meta[property='twitter:description']").unwrap();
    let meta_keywords_selector = Selector::parse("meta[name='keywords'], meta[property='article:tag']").unwrap();
    let meta_author_selector = Selector::parse("meta[name='author'], meta[property='article:author']").unwrap();
    let meta_date_selector = Selector::parse("meta[property='article:published_time'], meta[property='og:updated_time'], meta[itemprop='datePublished']").unwrap();
    let canonical_selector = Selector::parse("link[rel='canonical']").unwrap();
    let body_selector = Selector::parse("body").unwrap();
    let content_selectors = [
        "main", "article", "[role='main']",
        ".content", "#content", ".main-content", "#main-content",
        ".post", ".entry", ".article-body", ".page-content",
    ].join(", ");
    let main_content_selector = Selector::parse(&content_selectors).unwrap();
    let heading_selector = Selector::parse("h1, h2, h3, h4, h5, h6").unwrap();
    let p_selector = Selector::parse("p").unwrap();
    let related_links_selector = Selector::parse("a[rel='related'], .related a, .see-also a, .related-posts a, #related-links a").unwrap();

    let title = doc.select(&title_selector)
        .next()
        .map(|t| t.text().collect::<String>().trim().to_string())
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| "Untitled".to_string());

    let description = doc.select(&meta_desc_selector)
        .next()
        .and_then(|m| m.value().attr("content"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            doc.select(&body_selector).next()
                .and_then(|body| body.select(&p_selector).next())
                .map(|p| p.text().collect::<String>())
                .map(|s| {
                    let trimmed = s.trim();
                    if trimmed.len() > 250 {
                        format!("{}...", &trimmed[..250])
                    } else {
                        trimmed.to_string()
                    }
                })
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "No description available".to_string());

    let keywords = doc.select(&meta_keywords_selector)
        .next()
        .and_then(|m| m.value().attr("content"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "No keywords available".to_string());

    let author = doc.select(&meta_author_selector)
        .next()
        .and_then(|m| m.value().attr("content"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let published_date = doc.select(&meta_date_selector)
        .next()
        .and_then(|m| m.value().attr("content"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let canonical_url = doc.select(&canonical_selector)
        .next()
        .and_then(|link| link.value().attr("href"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    mdx.push_str("---\n");
    mdx.push_str(&format!("title: \"{}\"\n", title.replace('"', "\\\"")));
    mdx.push_str(&format!("description: \"{}\"\n", description.replace('"', "\\\"")));
    mdx.push_str(&format!("keywords: \"{}\"\n", keywords.replace('"', "\\\"")));
    mdx.push_str(&format!("sourceUrl: \"{}\"\n", source_url.replace('"', "\\\"")));

    if let Some(author_str) = author {
        mdx.push_str(&format!("author: \"{}\"\n", author_str.replace('"', "\\\"")));
    }
    if let Some(date_str) = published_date {
        mdx.push_str(&format!("date: \"{}\"\n", date_str.replace('"', "\\\"")));
    }
    if let Some(canonical_str) = canonical_url {
        mdx.push_str(&format!("canonicalUrl: \"{}\"\n", canonical_str.replace('"', "\\\"")));
    } else {
        mdx.push_str(&format!("canonicalUrl: \"{}\"\n", source_url.replace('"', "\\\"")));
    }
    mdx.push_str("---\n\n");

    let content_scope = doc.select(&main_content_selector).next()
        .or_else(|| doc.select(&body_selector).next());

    if let Some(scope) = content_scope {
        if scope.select(&heading_selector).count() > 3 {
            mdx.push_str("## Table of Contents\n\n");
            mdx.push_str("<!-- toc -->\n\n");
        }
    }

    if let Some(body) = doc.select(&body_selector).next() {
        let main_content_node = doc.select(&main_content_selector).next().unwrap_or(body);

        for child in main_content_node.children() {
            if let Some(element) = ElementRef::wrap(child) {
                mdx.push_str(&process_element(element));
            } else if let Some(text) = child.value().as_text() {
                let trimmed_text = text.trim();
                if !trimmed_text.is_empty() && trimmed_text.len() > 10 {
                    mdx.push_str(trimmed_text);
                    mdx.push_str("\n\n");
                }
            }
        }

        let related_links: Vec<_> = doc.select(&related_links_selector).collect();
        if !related_links.is_empty() {
            let mut related_mdx = String::new();
            related_mdx.push_str("\n## Related Resources\n\n");
            let mut link_count = 0;
            for link in related_links {
                if let Some(href) = link.value().attr("href") {
                    let absolute_href = match Url::parse(source_url).ok().and_then(|base| base.join(href).ok()) {
                        Some(abs_url) => abs_url.to_string(),
                        None => href.to_string(),
                    };

                    let text = element_to_markdown(link);
                    if !text.is_empty() && !absolute_href.is_empty() && absolute_href != "#" {
                        related_mdx.push_str(&format!("- [{}]({})\n", text, absolute_href));
                        link_count += 1;
                    }
                }
            }
            if link_count > 0 {
                mdx.push_str(&related_mdx);
            }
        }

    } else {
        warn!("Could not find body element for URL: {}", source_url);
        mdx.push_str("Error: Could not parse body content.\n");
    }

    let re_blank = Regex::new(r"\n{3,}").unwrap();
    let final_mdx = re_blank.replace_all(&mdx, "\n\n").trim().to_string();

    final_mdx
}

/// Saves MDX content to a file based on the URL. Returns the filename.
fn save_mdx(url_str: &str, mdx: &str) -> Option<String> {
    let filename = url_to_filename(url_str);
    let dir = Path::new(&filename).parent()?;

    if let Err(e) = fs::create_dir_all(dir) {
        error!("Failed to create directory {}: {}", dir.display(), e);
        return None;
    }
    match File::create(&filename) {
        Ok(mut file) => {
            if let Err(e) = file.write_all(mdx.as_bytes()) {
                error!("Failed to write MDX to {}: {}", filename, e);
                None
            } else {
                info!("Successfully saved MDX file: {}", filename);
                Some(filename)
            }
        }
        Err(e) => {
            error!("Failed to create file {}: {}", filename, e);
            None
        }
    }
}

/// Generates a filename from a URL for storing MDX files.
fn url_to_filename(url: &str) -> String {
    let parsed_url = match Url::parse(url) {
        Ok(p) => p,
        Err(_) => {
            warn!("Failed to parse URL for filename generation: {}", url);
            let safe_url = url.chars().filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_').collect::<String>();
            return format!("./output/invalid_url/{}.mdx", safe_url);
        }
    };

    let domain = parsed_url.domain().unwrap_or("unknown_domain");
    let path_part = format!("{}{}", parsed_url.path(), parsed_url.query().map(|q| format!("_{}", q)).unwrap_or_default());
    let sanitized_path = path_part
        .chars()
        .map(|c| match c {
            '/' | '?' | '&' | '=' | ':' | '%' | '#' => '_',
            _ => c,
        })
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect::<String>();

    let max_len = 100;
    let truncated_path = if sanitized_path.len() > max_len {
        &sanitized_path[..max_len]
    } else {
        &sanitized_path
    };

    let final_path_part = if truncated_path.is_empty() || truncated_path == "_" {
        "index"
    } else {
        truncated_path.trim_start_matches('_').trim_end_matches('_')
    };

    format!("./output/{}/{}.mdx", domain, final_path_part)
}

/// Extracts URLs from an HTML page for further crawling.
async fn fetch_and_extract_urls(client: &Client, url: &str) -> Vec<String> {
    if url.ends_with(".pdf") {
        return Vec::new();
    }

    match client.get(url).send().await {
        Ok(resp) => {
            let content_type = resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            if !content_type.contains("text/html") {
                info!("Skipping URL extraction for non-HTML content: {} ({})", url, content_type);
                return Vec::new();
            }

            match resp.text().await {
                Ok(text) => {
                    let base_url = match Url::parse(url) {
                        Ok(b) => b,
                        Err(_) => return Vec::new(),
                    };
                    let doc = Html::parse_document(&text);
                    let selector = Selector::parse("a[href]").unwrap();
                    let mut urls = Vec::new();
                    for element in doc.select(&selector) {
                        if let Some(href) = element.value().attr("href") {
                            let trimmed_href = href.trim();
                            if trimmed_href.is_empty() || trimmed_href.starts_with('#') || trimmed_href.starts_with("javascript:") || trimmed_href.starts_with("mailto:") {
                                continue;
                            }

                            match base_url.join(trimmed_href) {
                                Ok(mut abs_url) => {
                                    abs_url.set_fragment(None);
                                    urls.push(abs_url.to_string());
                                }
                                Err(e) => {
                                    warn!("Failed to join URL '{}' with base '{}': {}", href, base_url, e);
                                }
                            }
                        }
                    }
                    urls
                }
                Err(e) => {
                    error!("Failed to read text from {}: {}", url, e);
                    Vec::new()
                }
            }
        }
        Err(e) => {
            error!("Failed to fetch for URL extraction {}: {}", url, e);
            Vec::new()
        }
    }
}

/// Fetches HTML content from a URL.
async fn fetch_html(client: &Client, url: &str) -> Option<String> {
    client.get(url).send().await.ok()?.text().await.ok()
}

/// Fetches and extracts text from a PDF file.
async fn fetch_pdf(client: &Client, url: &str) -> Option<String> {
    let resp = client.get(url).send().await.ok()?;
    let bytes = resp.bytes().await.ok()?;
    let mut temp_file = NamedTempFile::new().ok()?;
    temp_file.write_all(&bytes).ok()?;
    extract_text(temp_file.path()).ok()
}

/// Processes a single URL: fetches, cleans, saves MDX. Returns Option<filename> and Vec<extracted_urls>.
async fn process_url(
    client: &Client,
    semaphore: &Arc<Semaphore>,
    url: String,
    logs: &Arc<tokio::sync::Mutex<Vec<String>>>,
) -> (Option<String>, Vec<String>) {
    let _permit = match semaphore.acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            error!("Failed to acquire semaphore permit for {}", url);
            return (None, Vec::new());
        }
    };
    info!("Processing URL: {}", url);
    logs.lock().await.push(format!("Processing: {}", url));

    let resp = match client.get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            error!("Request failed for {}: {}", url, e);
            logs.lock().await.push(format!("Request failed for {}: {}", url, e));
            return (None, Vec::new());
        }
    };

    if !resp.status().is_success() {
        error!("Request failed for {} with status: {}", url, resp.status());
        logs.lock().await.push(format!("Request failed for {} with status: {}", url, resp.status()));
        return (None, Vec::new());
    }

    let content_type = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let mut mdx_content: Option<String> = None;
    let mut extracted_urls: Vec<String> = Vec::new();

    if content_type.contains("application/pdf") {
        info!("Fetching PDF content from {}", url);
        if let Some(pdf_text) = fetch_pdf(client, &url).await {
            let pdf_mdx = format!(
                "---\ntitle: \"PDF Document: {}\"\ndescription: \"Extracted text from PDF.\"\nsourceUrl: \"{}\"\n---\n\n{}",
                url.split('/').last().unwrap_or("document.pdf"), url, pdf_text
            );
            mdx_content = Some(pdf_mdx);
        } else {
            error!("Failed to extract text from PDF: {}", url);
            logs.lock().await.push(format!("Failed to extract text from PDF: {}", url));
        }
    } else if content_type.contains("text/html") {
        info!("Fetching HTML content from {}", url);
        if let Some(html_content) = resp.text().await.ok() {
            mdx_content = Some(clean_to_mdx(&html_content, &url));
            extracted_urls = fetch_and_extract_urls(client, &url).await;
        } else {
            error!("Failed to read HTML text from response: {}", url);
            logs.lock().await.push(format!("Failed to read HTML text from response: {}", url));
        }
    } else {
        info!("Skipping unsupported content type '{}' for URL: {}", content_type, url);
        logs.lock().await.push(format!("Skipping unsupported content type '{}' for URL: {}", content_type, url));
    }

    let saved_filename = if let Some(mdx) = mdx_content {
        if !mdx.is_empty() {
            save_mdx(&url, &mdx)
        } else {
            warn!("Generated MDX was empty for URL: {}", url);
            logs.lock().await.push(format!("Generated MDX was empty for URL: {}", url));
            None
        }
    } else {
        None
    };

    if saved_filename.is_some() {
        info!("Successfully processed and saved: {}", url);
        logs.lock().await.push(format!("Successfully processed: {}", url));
    }

    (saved_filename, extracted_urls)
}

/// Handles the crawl request: manages the crawl queue, waits for completion, collects results.
async fn start_crawl(req: web::Json<CrawlRequest>, state: web::Data<CrawlerState>) -> impl Responder {
    let initial_domains = req.domains.clone();
    let max_depth = req.max_depth.unwrap_or(5).min(5);
    let start_time = Instant::now();

    info!("Starting crawl for {} domains, max_depth: {}", initial_domains.len(), max_depth);
    let logs = state.logs.clone();
    logs.lock().await.push(format!(
        "Received crawl request for {} domains with max_depth: {}",
        initial_domains.len(), max_depth
    ));

    let mut visited_urls = HashSet::<String>::new();
    let mut urls_to_process = VecDeque::<(String, usize)>::new();
    let mut mdx_results = Vec::<(String, String)>::new();
    let mut processed_count = 0;
    let target_domains = initial_domains.iter()
        .filter_map(|d| Url::parse(d).ok()?.domain().map(|s| s.to_string()))
        .collect::<HashSet<String>>();

    for domain_url in initial_domains {
        if let Ok(parsed_url) = Url::parse(&domain_url) {
            let url_string = parsed_url.to_string();
            if visited_urls.insert(url_string.clone()) {
                urls_to_process.push_back((url_string, 0));
                info!("Queueing initial URL: {}", domain_url);
            }
        } else {
            warn!("Ignoring invalid initial URL: {}", domain_url);
            logs.lock().await.push(format!("Ignoring invalid initial URL: {}", domain_url));
        }
    }

    let mut active_tasks = Vec::new();

    while !urls_to_process.is_empty() || !active_tasks.is_empty() {
        while let Some((url, depth)) = urls_to_process.pop_front() {
            if depth > max_depth {
                info!("Skipping URL due to max depth: {}", url);
                continue;
            }

            processed_count += 1;
            let client = state.client.clone();
            let semaphore = state.semaphore.clone();
            let logs_clone = logs.clone();

            active_tasks.push(tokio::spawn(async move {
                let (filename_option, extracted_urls) = process_url(
                    &client,
                    &semaphore,
                    url.clone(),
                    &logs_clone
                ).await;
                (url, depth, filename_option, extracted_urls)
            }));
        }

        if !active_tasks.is_empty() {
            let (result, _index, remaining_tasks) = futures::future::select_all(active_tasks).await;
            active_tasks = remaining_tasks;

            match result {
                Ok((original_url, current_depth, filename_option, extracted_urls)) => {
                    if let Some(filename) = filename_option {
                        match fs::read_to_string(&filename) {
                            Ok(content) => mdx_results.push((original_url.clone(), content)),
                            Err(e) => error!("Failed to read saved MDX file {}: {}", filename, e),
                        }
                    }

                    if current_depth < max_depth {
                        logs.lock().await.push(format!("Found {} URLs at {} (depth {}/{})", extracted_urls.len(), original_url, current_depth, max_depth));
                        for next_url_str in &extracted_urls {
                            if let Ok(next_url) = Url::parse(next_url_str) {
                                if let Some(domain) = next_url.domain() {
                                    if target_domains.contains(domain) {
                                        let next_url_string = next_url.to_string();
                                        if visited_urls.insert(next_url_string.clone()) {
                                            urls_to_process.push_back((next_url_string, current_depth + 1));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("A crawl task failed: {:?}", e);
                    logs.lock().await.push(format!("A crawl task failed: {:?}", e));
                }
            }
        }
    }

    let elapsed = start_time.elapsed();
    let final_message = format!(
        "Completed crawl. Processed {} URLs for {} initial domains in {:.2} seconds. Found {} MDX files.",
        processed_count,
        req.domains.len(),
        elapsed.as_secs_f64(),
        mdx_results.len()
    );
    info!("{}", final_message);
    logs.lock().await.push(final_message.clone());

    let response_logs = logs.lock().await.clone();
    HttpResponse::Ok().json(CrawlResponse {
        message: final_message,
        logs: response_logs,
        mdx_files: mdx_results,
    })
}

/// Serves an MDX file based on domain and path.
async fn get_mdx(path: web::Path<(String, String)>, _state: web::Data<CrawlerState>) -> impl Responder {
    let (domain, path_param) = path.into_inner();
    let filename = format!("./output/{}/{}.mdx", domain, path_param);

    match fs::read_to_string(&filename) {
        Ok(content) => HttpResponse::Ok()
            .content_type("text/markdown; charset=utf-8")
            .body(content),
        Err(e) => {
            warn!("MDX file not found for get_mdx: {} ({})", filename, e);
            HttpResponse::NotFound().body(format!("MDX not found for {}/{}", domain, path_param))
        },
    }
}

/// Main entry point, sets up the server and background crawler task.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let host = "127.0.0.1";
    let port = 8080;

    info!("Initializing Crawler API (SuperCrawler parts separate)");

    if let Ok(api_key) = std::env::var("FIRECRAWL_API_KEY") {
        if api_key.len() >= 10 {
            let masked_key = format!("{}...{}", &api_key[0..5], &api_key[api_key.len()-5..]);
            info!("Found FIRECRAWL_API_KEY in environment: {}", masked_key);
        } else if !api_key.is_empty() {
            info!("Found FIRECRAWL_API_KEY in environment (short key)");
        } else {
            info!("FIRECRAWL_API_KEY is set but empty");
        }
    } else {
        info!("FIRECRAWL_API_KEY not found in environment");
    }

    let max_concurrency = 1000;
    let client = Client::builder()
        .pool_max_idle_per_host(50)
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(15))
        .pool_idle_timeout(Duration::from_secs(90))
        .user_agent("SuperCrawler/1.0")
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .expect("Failed to build reqwest client");

    let semaphore = Arc::new(Semaphore::new(max_concurrency));

    let (bg_tx, mut bg_rx) = mpsc::channel::<(String, usize, usize, String)>(100_000);
    let logs = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let bg_client = client.clone();
    let bg_semaphore = semaphore.clone();
    let bg_logs = logs.clone();
    tokio::spawn(async move {
        let mut visited = HashSet::<String>::new();
        info!("Background worker task started.");
        while let Some((url, depth, max_depth, _domain)) = bg_rx.recv().await {
            if !visited.insert(url.clone()) {
                continue;
            }
            info!("Background task processing: {} (Depth {}/{})", url, depth, max_depth);

            let (_filename_option, _extracted_urls) = process_url(
                &bg_client,
                &bg_semaphore,
                url,
                &bg_logs,
            ).await;
        }
        info!("Background worker task finished.");
    });

    let (super_tx, super_rx) = mpsc::channel::<(String, usize, usize, String)>(100_000);
    let super_client = client.clone();
    let super_semaphore = semaphore.clone();
    let super_logs = logs.clone();
    tokio::spawn(super_background_crawler(
        super_client,
        super_semaphore,
        super_tx.clone(),
        super_rx,
        super_logs
    ));

    let regular_state = web::Data::new(CrawlerState {
        client: client.clone(),
        semaphore: semaphore.clone(),
        logs: logs.clone(),
        bg_tx: bg_tx,
    });

    let super_state = web::Data::new(SuperCrawlerState {
        client,
        semaphore,
        tx: super_tx,
        logs,
    });

    info!("Starting server at http://{}:{}", host, port);
    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .app_data(regular_state.clone())
            .app_data(super_state.clone())
            .route("/crawl", web::post().to(start_crawl))
            .route("/supercrawler", web::post().to(super_crawl))
            .route("/mdx/{domain}/{path:.*}", web::get().to(get_mdx))
    })
    .bind((host, port))?
    .run()
    .await
}