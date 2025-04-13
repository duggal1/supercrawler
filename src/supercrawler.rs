use actix_web::{web, App, HttpServer, HttpResponse, Responder, rt::spawn};
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
use log::{info, error, warn, debug};
use std::time::{Duration, Instant};
use futures_util::{future::select_all, stream::StreamExt};
use std::env;
use serde_json::json;
use tokio::time::sleep;
use actix_web_lab::sse::{self, Sse, Event, Data};
use std::convert::Infallible;
use tokio_stream::wrappers::ReceiverStream;
use std::io::Error as IoError;

pub struct AppState {
    pub client: Client,
    pub semaphore: Arc<Semaphore>,
}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SuperCrawlerRequest {
    query: String,
    max_depth: Option<usize>,
    max_urls: Option<u32>,
    time_limit: Option<u32>,
    crawl_depth: Option<usize>,
    firecrawl_api_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ResearchSource {
    url: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ResearchData {
    #[serde(default)]
    sources: Vec<ResearchSource>,
    #[serde(default)]
    finalAnalysis: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DeepResearchResponse {
    success: bool,
    data: Option<ResearchData>,
    status: Option<String>,
    id: Option<String>,
    error: Option<String>,
    message: Option<String>,
}

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
                        // Block-level handled by process_element
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
                    if !trimmed_text.is_empty() && trimmed_text.len() > 10 {
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
                    if !trimmed_text.is_empty() && trimmed_text.len() > 10 {
                        mdx.push_str(trimmed_text);
                        mdx.push_str("\n\n");
                    }
                }
            }
            mdx
        }
    }
}

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

async fn fetch_html(client: &Client, url: &str) -> Option<String> {
    client.get(url).send().await.ok()?.text().await.ok()
}

async fn fetch_pdf(client: &Client, url: &str) -> Option<String> {
    let resp = client.get(url).send().await.ok()?;
    let bytes = resp.bytes().await.ok()?;
    let mut temp_file = NamedTempFile::new().ok()?;
    temp_file.write_all(&bytes).ok()?;
    extract_text(temp_file.path()).ok()
}

/// Helper to send SSE log messages
async fn sse_log(tx: &mpsc::Sender<Result<Event, Infallible>>, level: &str, message: String) {
    info!("[{}] {}", level, message); // Keep server-side logging
    let event = Event::Data(Data::new(format!("[{}] {}", level, message)).event("log"));
    if tx.send(Ok(event)).await.is_err() {
        // Log error if client disconnected, but don't crash the task
        warn!("Failed to send log event to client (disconnected?)");
    }
}

async fn sse_log_info(tx: &mpsc::Sender<Result<Event, Infallible>>, message: String) {
    sse_log(tx, "INFO", message).await;
}

async fn sse_log_warn(tx: &mpsc::Sender<Result<Event, Infallible>>, message: String) {
    sse_log(tx, "WARN", message).await;
}

async fn sse_log_error(tx: &mpsc::Sender<Result<Event, Infallible>>, message: String) {
    sse_log(tx, "ERROR", message).await;
}

async fn process_url(
    client: &Client,
    semaphore: &Arc<Semaphore>,
    url: String,
    tx: &mpsc::Sender<Result<Event, Infallible>>, // Added SSE sender
) -> (Option<String>, Vec<String>) {
    let _permit = match semaphore.acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            let err_msg = format!("Failed to acquire semaphore permit for {}", url);
            sse_log_error(tx, err_msg).await;
            return (None, Vec::new());
        }
    };
    sse_log_info(tx, format!("Processing URL: {}", url)).await;

    let max_retries = 3;
    let mut attempt = 0;
    let base_delay_ms = 500;
    let resp = loop {
        attempt += 1;
        match client.get(&url).send().await {
            Ok(resp) => break Ok(resp),
            Err(e) => {
                let is_connect_error = e.is_connect();
                let err_msg = format!("Request failed for {} (Attempt {}/{}): {}", url, attempt, max_retries, e);

                if is_connect_error && attempt < max_retries {
                    let delay_ms = base_delay_ms * 2_u64.pow(attempt as u32 - 1);
                    let retry_msg = format!("{} - Retrying in {}ms...", err_msg, delay_ms);
                    sse_log_warn(tx, retry_msg).await;
                    sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    sse_log_error(tx, err_msg).await;
                    break Err(e);
                }
            }
        }
    };

    let resp = match resp {
        Ok(r) => r,
        Err(_) => return (None, Vec::new()), // Error already logged
    };

    if !resp.status().is_success() {
        let err_msg = format!("Request failed for {} with status: {}", url, resp.status());
        sse_log_error(tx, err_msg).await;
        return (None, Vec::new());
    }

    let content_type = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let mut mdx_content: Option<String> = None;
    let mut extracted_urls: Vec<String> = Vec::new();
    let mut content_was_generated = false; // Flag to track if we attempted generation

    if content_type.contains("application/pdf") {
        sse_log_info(tx, format!("Fetching PDF content from {}", url)).await;
        if let Some(pdf_text) = fetch_pdf(client, &url).await {
            let pdf_mdx = format!(
                "---\ntitle: \"PDF Document: {}\"\ndescription: \"Extracted text from PDF.\"\nsourceUrl: \"{}\"\n---\n\n{}",
                 url.split('/').last().unwrap_or("document.pdf"), url, pdf_text
            );
            mdx_content = Some(pdf_mdx);
            content_was_generated = true; // Mark that we generated content
        } else {
            let err_msg = format!("Failed to extract text from PDF: {}", url);
            sse_log_error(tx, err_msg).await;
        }
    } else if content_type.contains("text/html") {
        sse_log_info(tx, format!("Fetching HTML content from {}", url)).await;
        match resp.text_with_charset("utf-8").await {
            Ok(html_content) => {
                 mdx_content = Some(clean_to_mdx(&html_content, &url));
                 content_was_generated = true; // Mark that we generated content
                 let base_url_res = Url::parse(&url);
                 if let Ok(base_url) = base_url_res {
                    let doc = Html::parse_document(&html_content);
                    let selector = Selector::parse("a[href]").unwrap();
                    for element in doc.select(&selector) {
                        if let Some(href) = element.value().attr("href") {
                            let trimmed_href = href.trim();
                            if trimmed_href.is_empty() || trimmed_href.starts_with('#') || trimmed_href.starts_with("javascript:") || trimmed_href.starts_with("mailto:") {
                                continue;
                            }
                            match base_url.join(trimmed_href) {
                                Ok(mut abs_url) => {
                                    abs_url.set_fragment(None);
                                    extracted_urls.push(abs_url.to_string());
                                }
                                Err(e) => {
                                     let warn_msg = format!("Failed to join URL '{}' with base '{}': {}", href, base_url, e);
                                     sse_log_warn(tx, warn_msg).await;
                                }
                            }
                        }
                    }
                 } else {
                     sse_log_warn(tx, format!("Could not parse base URL for extraction: {}", url)).await;
                 }
            }
            Err(e) => {
                 let err_msg = format!("Failed to read HTML text from response for {}: {}", url, e);
                 sse_log_error(tx, err_msg).await;
            }
        }
    } else {
        let skip_msg = format!("Skipping unsupported content type '{}' for URL: {}", content_type, url);
        sse_log_info(tx, skip_msg).await;
    }

    // --- Saving Logic ---
    let mut saved_filename: Option<String> = None;

    if let Some(mdx) = mdx_content { // Consume mdx_content if it's Some
        if !mdx.trim().is_empty() {
            saved_filename = save_mdx(&url, &mdx); // save_mdx logs its own file saving errors
        } else {
             let warn_msg = format!("Generated MDX was empty for URL: {}", url);
             sse_log_warn(tx, warn_msg).await;
             // saved_filename remains None
        }
    } // mdx_content is dropped here if it was Some

    // --- Logging Save Status ---
    if saved_filename.is_some() {
        let success_msg = format!("Successfully processed and saved: {}", url);
        sse_log_info(tx, success_msg).await;
    } else if content_was_generated {
        // Log only if we generated content but didn't save it (and it wasn't empty, which was logged above)
        // This implies save_mdx failed, and it should have logged the specific file error.
        // Adding another generic log might be redundant.
        // We can rely on save_mdx logging for file errors.
        debug!("Content generated for {} but not saved (save_mdx should have logged file error, or content was empty)", url);
    }

    (saved_filename, extracted_urls)
}

async fn poll_job_status(
    client: &Client,
    job_id: &str,
    api_key: &str,
    tx: &mpsc::Sender<Result<Event, Infallible>>, // Added SSE sender
) -> Result<DeepResearchResponse, String> {
    let log_prefix = format!("[POLLING JOB {}]", job_id);
    sse_log_info(tx, format!("{log_prefix} 😍 Starting polling")).await;

    let start_time = Instant::now();
    let url = format!("https://api.firecrawl.dev/v1/deep-research/{}", job_id);
    let max_duration = Duration::from_secs(600);
    let mut retry_count = 0;
    let max_retries = 5;

    while start_time.elapsed() < max_duration {
        let msg = format!("{log_prefix} ✅ Checking status (Attempt {})", retry_count + 1);
        sse_log_info(tx, msg).await;

        match client.get(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .timeout(Duration::from_secs(30))
            .send()
            .await {
                Ok(response) => {
                    let status_code = response.status();
                    let msg = format!("{log_prefix} RESPONSE Status code: {}", status_code);
                    sse_log_info(tx, msg).await;

                    retry_count = 0; // Reset retries on successful request

                    if !status_code.is_success() {
                        let error_text = response.text().await.unwrap_or_default();
                        let err_msg = format!("{log_prefix} ⚠️ Non-success status code: {}. Body: {}", status_code, error_text);
                        sse_log_warn(tx, err_msg.clone()).await;
                        return Err(err_msg);
                    }

                    let body_bytes = match response.bytes().await {
                         Ok(b) => b,
                         Err(e) => {
                             let err_msg = format!("{log_prefix} ❌ Failed to read response body bytes: {}", e);
                             sse_log_error(tx, err_msg.clone()).await;
                             return Err(err_msg);
                         }
                     };

                    match serde_json::from_slice::<DeepResearchResponse>(&body_bytes) {
                        Ok(status_data) => {
                            let success_msg = format!("{log_prefix} ✔️ Parsed Status: {:?}, Success: {}",
                                status_data.status,
                                status_data.success);
                            sse_log_info(tx, success_msg).await;

                            if let Some(status) = &status_data.status {
                                if status == "completed" {
                                    let complete_msg = format!("{log_prefix} ✅ Job completed successfully");
                                    sse_log_info(tx, complete_msg).await;
                                    return Ok(status_data);
                                }
                            }

                            let wait_msg = format!("{log_prefix} 🚧 Status not 'completed', waiting 3s...");
                            sse_log_info(tx, wait_msg).await;
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        },
                        Err(e) => {
                            let raw_body = String::from_utf8_lossy(&body_bytes);
                            let err_msg = format!("{log_prefix} ❌ Failed to parse JSON response: {}. Raw: {}", e, raw_body);
                            sse_log_error(tx, err_msg.clone()).await;
                            return Err(err_msg);
                        }
                    }
                },
                Err(e) => {
                    retry_count += 1;
                    let retry_delay = 2_u64.pow(retry_count as u32) * 375; // Exponential backoff
                    let err_msg = format!("{log_prefix} ❌ Request error: {} (Retry {}/{})", e, retry_count, max_retries);
                    sse_log_error(tx, err_msg.clone()).await;

                    if retry_count >= max_retries {
                        let final_err = format!("{log_prefix} ❌ Failed after {} retries: {}", max_retries, e);
                         sse_log_error(tx, final_err.clone()).await;
                        return Err(final_err);
                    }

                    let retry_msg = format!("{log_prefix} ⏳ Waiting {}ms before retry #{}", retry_delay, retry_count + 1);
                    sse_log_info(tx, retry_msg).await;
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                }
            }
    }

    let timeout_msg = format!("{log_prefix} ⚠️ Job polling timed out after {}s", start_time.elapsed().as_secs());
    sse_log_error(tx, timeout_msg.clone()).await;
    Err(timeout_msg)
}

async fn fetch_deep_research_urls(
    client: &Client,
    query: &str,
    max_urls: u32,
    time_limit: u32,
    max_depth: usize,
    api_key: &str,
    tx: &mpsc::Sender<Result<Event, Infallible>>, // Added SSE sender
) -> Result<Vec<String>, String> {
    let firecrawl_url = "https://api.firecrawl.dev/v1/deep-research";

    let start_msg = "[FIRECRAWL START] 🔥 Initiating deep research API call";
    sse_log_info(tx, start_msg.to_string()).await;

    let req_details = format!("[FIRECRAWL REQUEST] Query: {}, Max URLs: {}, Time Limit: {}s, Max Depth: {}",
                              query, max_urls, time_limit, max_depth);
    sse_log_info(tx, req_details).await;

    // Apply constraints
    let validated_time_limit = time_limit.clamp(150, 600);
    let validated_max_depth = max_depth.clamp(1, 5); // Assuming min depth 1
    let validated_max_urls = max_urls.clamp(5, 120);

    if validated_time_limit != time_limit || validated_max_depth != max_depth || validated_max_urls != max_urls {
         let validation_msg = format!("[FIRECRAWL VALIDATED PARAMS] Using MaxUrls: {}, TimeLimit: {}, MaxDepth: {}",
                                      validated_max_urls, validated_time_limit, validated_max_depth);
         sse_log_info(tx, validation_msg).await;
    }

    let payload = json!({
        "query": query,
        "maxUrls": validated_max_urls,
        "timeLimit": validated_time_limit,
        "maxDepth": validated_max_depth
    });

    let payload_msg = format!("[FIRECRAWL PAYLOAD] {}", payload.to_string());
    sse_log_info(tx, payload_msg).await;

    let response_result = client
        .post(firecrawl_url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(60)) // Initial request timeout
        .json(&payload)
        .send()
        .await;

    let response = match response_result {
        Ok(resp) => {
            let status = resp.status();
            let status_msg = format!("[FIRECRAWL RESPONSE] Initial Status: {}", status);
            sse_log_info(tx, status_msg).await;

            if !status.is_success() {
                let error_text = resp.text().await.unwrap_or_default();
                let err_msg = format!("[FIRECRAWL ERROR] Status: {}. Body: {}", status, error_text);
                sse_log_error(tx, err_msg.clone()).await;
                return Err(err_msg);
            }

             let body_bytes = match resp.bytes().await {
                 Ok(b) => b,
                 Err(e) => {
                     let err_msg = format!("[FIRECRAWL ERROR] Failed to read initial response body bytes: {}", e);
                     sse_log_error(tx, err_msg.clone()).await;
                     return Err(err_msg);
                 }
             };

             let body_log_msg = format!("[FIRECRAWL RESPONSE BODY] Received initial response body (length: {})", body_bytes.len());
             sse_log_info(tx, body_log_msg).await;

            match serde_json::from_slice::<DeepResearchResponse>(&body_bytes) {
                Ok(parsed) => parsed,
                Err(e) => {
                    let raw_body = String::from_utf8_lossy(&body_bytes);
                    let err_msg = format!("[FIRECRAWL JSON PARSE ERROR] Failed: {}. Raw: {}", e, raw_body);
                    sse_log_error(tx, err_msg.clone()).await;
                    return Err(err_msg);
                }
            }
        },
        Err(e) => {
            let err_msg = format!("[FIRECRAWL REQUEST ERROR] {}", e);
            sse_log_error(tx, err_msg.clone()).await;
            return Err(err_msg);
        }
    };

    // Process response (polling if necessary)
    let research_urls_result = if response.success {
        match response.status.as_deref() {
            Some("completed") => {
                let completed_msg = "[FIRECRAWL SUCCESS] Research completed immediately in initial response.";
                sse_log_info(tx, completed_msg.to_string()).await;

                if let Some(data) = response.data {
                    let urls: Vec<String> = data.sources.into_iter().map(|s| s.url).collect();
                    let urls_msg = format!("[FIRECRAWL URLS] Found {} URLs directly.", urls.len());
                    sse_log_info(tx, urls_msg).await;
                    for (i, url) in urls.iter().take(5).enumerate() {
                         sse_log_info(tx, format!("[FIRECRAWL URL {}] {}", i+1, url)).await;
                    }
                    if urls.len() > 5 {
                         sse_log_info(tx, format!("[FIRECRAWL URLS] ... and {} more.", urls.len() - 5)).await;
                    }
                    Ok(urls)
                } else {
                    let err_msg = "[FIRECRAWL ERROR] Missing data in completed response";
                    sse_log_error(tx, err_msg.to_string()).await;
                    Err(err_msg.to_string())
                }
            },
            _ => { // Includes "pending", "running", etc. or missing status
                if let Some(job_id) = response.id {
                    let async_msg = format!("[FIRECRAWL ASYNC] 🚀 Job started, polling ID: {}", job_id);
                    sse_log_info(tx, async_msg).await;

                    match poll_job_status(client, &job_id, api_key, tx).await { // Pass tx
                        Ok(poll_result) => {
                            if let Some(data) = poll_result.data {
                                let urls: Vec<String> = data.sources.into_iter().map(|s| s.url).collect();
                                let poll_success_msg = format!("[FIRECRAWL POLL SUCCESS] Found {} URLs after polling.", urls.len());
                                sse_log_info(tx, poll_success_msg).await;
                                Ok(urls)
                            } else {
                                let err_msg = "[FIRECRAWL POLL ERROR] No data in poll response";
                                sse_log_error(tx, err_msg.to_string()).await;
                                Err(err_msg.to_string())
                            }
                        },
                        Err(e) => {
                             // poll_job_status already logs errors
                             let poll_fail_msg = format!("[FIRECRAWL POLL FAILED] {}", e);
                             // sse_log_error(tx, poll_fail_msg.clone()).await; // Avoid double logging
                             Err(poll_fail_msg)
                        }
                    }
                } else {
                    let err_msg = "[FIRECRAWL ERROR] Job status unknown: success=true, but status not 'completed' and no job ID provided.";
                    sse_log_error(tx, err_msg.to_string()).await;
                    Err(err_msg.to_string())
                }
            }
        }
    } else {
        let error_msg = response.error.unwrap_or_else(|| "Unknown Firecrawl API error".to_string());
        let failure_msg = format!("[FIRECRAWL FAILURE] API returned error: {}", error_msg);
        sse_log_error(tx, failure_msg.clone()).await;
        Err(failure_msg)
    };

    research_urls_result
}

fn get_api_key() -> String {
    match env::var("FIRECRAWL_API_KEY") {
        Ok(key) => {
             if key.is_empty() || key == "fc-your-key" {
                 // warn!("⚠️ FIRECRAWL_API_KEY environment variable is empty or default"); // Log in caller
                 "fc-your-key".to_string()
             } else {
                 key
             }
        },
        Err(_) => {
            // warn!("⚠️ Failed to get FIRECRAWL_API_KEY from environment: {}", e); // Log in caller
            if let Ok(content) = fs::read_to_string(".env") {
                for line in content.lines() {
                    if line.starts_with("FIRECRAWL_API_KEY=") {
                        let key_parts: Vec<&str> = line.splitn(2, '=').collect();
                         if key_parts.len() == 2 {
                            let key = key_parts[1].trim().trim_matches(|c| c == '"' || c == '\'');
                             if !key.is_empty() && key != "fc-your-key" {
                                // info!("[ENV] Found Firecrawl API key in .env file"); // Log in caller
                                return key.to_string();
                            }
                         }
                    }
                }
                 // warn!("⚠️ FIRECRAWL_API_KEY not found or invalid in .env file."); // Log in caller
            } else {
                 // warn!("⚠️ .env file not found or unreadable."); // Log in caller
            }
            "fc-your-key".to_string()
        }
    }
}

pub async fn super_crawl(
    req: web::Json<SuperCrawlerRequest>,
    state: web::Data<AppState>
) -> Result<Sse<impl StreamExt<Item = Result<Event, Infallible>>>, actix_web::Error> {

    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(200); // Channel for SSE events

    let req_clone = req.into_inner(); // Clone request data early
    let client = state.client.clone();
    let semaphore = state.semaphore.clone();
    // Note: AppState no longer contains logs or bg_tx

    // Spawn the actual work into a background task
    spawn(async move {
        let overall_start_time = Instant::now();
        let tx_clone = tx.clone(); // Clone sender for the task

        sse_log_info(&tx_clone, "[REQUEST START] 🚀 Received super crawler request".to_string()).await;

        let request_json = serde_json::to_string(&req_clone).unwrap_or_default();
        sse_log_info(&tx_clone, format!("[REQUEST BODY] {}", request_json)).await;

        // --- API Key Handling & Logging ---
        let api_key = match req_clone.firecrawl_api_key.as_deref() {
            Some(key) if !key.is_empty() => {
                let msg = "[API KEY] Using API key provided in the request body.";
                sse_log_info(&tx_clone, msg.to_string()).await;
                key.to_string()
            }
            _ => {
                let msg = "[API KEY] No API key in request body, checking environment/'.env'...".to_string();
                sse_log_info(&tx_clone, msg).await;
                let key_from_env = get_api_key(); // Doesn't log internally anymore
                if key_from_env == "fc-your-key" {
                    let warn_msg = "⚠️ [API KEY WARNING] No valid Firecrawl API key found. Using default placeholder.";
                    sse_log_warn(&tx_clone, warn_msg.to_string()).await;
                } else {
                    let env_msg = "[API KEY] Using API key found in environment/'.env'.";
                    sse_log_info(&tx_clone, env_msg.to_string()).await;
                     // Optionally log masked key here if needed
                     if key_from_env.len() >= 10 {
                         let masked_key = format!("{}...{}", &key_from_env[0..5], &key_from_env[key_from_env.len()-5..]);
                         sse_log_info(&tx_clone, format!("[ENV] Using Firecrawl API key: {}", masked_key)).await;
                     } else {
                         sse_log_info(&tx_clone, "[ENV] Using Firecrawl API key (short key)".to_string()).await;
                     }
                }
                key_from_env
            }
        };

        if api_key == "fc-your-key" {
            let err_msg = "❌ Configuration Error: Firecrawl API key is missing or invalid.";
            sse_log_error(&tx_clone, err_msg.to_string()).await;
            let error_json = json!({ "error": err_msg });
            let _ = tx_clone.send(Ok(Event::Data(Data::new(error_json.to_string()).event("error")))).await;
            // Close the stream implicitly by dropping tx_clone
            return;
        }

        // --- Parameter Validation & Logging ---
        let max_urls = req_clone.max_urls.unwrap_or(20).clamp(5, 120);
        let firecrawl_depth = req_clone.max_depth.unwrap_or(1).clamp(1, 5);
        let crawl_depth = req_clone.crawl_depth.unwrap_or(2).clamp(0, 5); // Allow 0 depth? Let's assume min 1. clamp(1, 5)
        let time_limit_secs = req_clone.time_limit.unwrap_or(600).clamp(150, 600); // Default 600s

        // Log if parameters were clamped
        if req_clone.max_urls.is_some() && req_clone.max_urls.unwrap() != max_urls {
            sse_log_warn(&tx_clone, format!("[PARAM WARNING] Requested max_urls {} clamped to {}", req_clone.max_urls.unwrap(), max_urls)).await;
        }
        if req_clone.max_depth.is_some() && req_clone.max_depth.unwrap() != firecrawl_depth {
            sse_log_warn(&tx_clone, format!("[PARAM WARNING] Requested firecrawl_depth {} clamped to {}", req_clone.max_depth.unwrap(), firecrawl_depth)).await;
        }
        if req_clone.crawl_depth.is_some() && req_clone.crawl_depth.unwrap() != crawl_depth {
             sse_log_warn(&tx_clone, format!("[PARAM WARNING] Requested crawl_depth {} clamped to {}", req_clone.crawl_depth.unwrap(), crawl_depth)).await;
        }
        if req_clone.time_limit.is_some() && req_clone.time_limit.unwrap() != time_limit_secs {
             sse_log_warn(&tx_clone, format!("[PARAM WARNING] Requested time_limit {} clamped to {}", req_clone.time_limit.unwrap(), time_limit_secs)).await;
        }

        let timeout_duration = Duration::from_secs(time_limit_secs as u64);

        let params_log = format!("[VALIDATED PARAMS] Query: {}, MaxUrls: {}, FirecrawlDepth: {}, CrawlDepth: {}, TimeLimit: {}",
             req_clone.query, max_urls, firecrawl_depth, crawl_depth, time_limit_secs);
        sse_log_info(&tx_clone, params_log).await;


        // --- Firecrawl Deep Research ---
        let firecrawl_start_time = Instant::now();
        let deep_research_result = fetch_deep_research_urls(
            &client, &req_clone.query, max_urls, time_limit_secs, firecrawl_depth, &api_key, &tx_clone,
        ).await;
        let firecrawl_elapsed = firecrawl_start_time.elapsed();
        sse_log_info(&tx_clone, format!("[FIRECRAWL TIMING] ⏱️ Firecrawl API took {:.2}s", firecrawl_elapsed.as_secs_f64())).await;

        let initial_urls = match deep_research_result {
            Ok(urls) => {
                sse_log_info(&tx_clone, format!("✅ Retrieved {} URLs from Deep Research API", urls.len())).await;
                urls
            },
            Err(e) => {
                // fetch_deep_research_urls already logs the specific error via SSE
                let err_msg = format!("❌ Failed to fetch URLs from Firecrawl: {}", e);
                // sse_log_error(&tx_clone, err_msg.clone()).await; // Avoid double logging
                let error_json = json!({ "error": err_msg });
                let _ = tx_clone.send(Ok(Event::Data(Data::new(error_json.to_string()).event("error")))).await;
                return;
            }
        };

        if initial_urls.is_empty() {
            let msg = "⚠️ No URLs found from Deep Research API. Nothing more to process.";
            sse_log_warn(&tx_clone, msg.to_string()).await;
             let completion_json = json!({
                 "message": msg,
                 "processed_mdx_url_count": 0,
                 "initial_urls_from_firecrawl": initial_urls, // Empty vec
                 "mdx_files": [], // Empty vec
                 "timings": {
                     "firecrawl_api_seconds": firecrawl_elapsed.as_secs_f64(),
                     "total_seconds": overall_start_time.elapsed().as_secs_f64()
                 },
                 // Include params for context
                 "params": {
                    "query": req_clone.query,
                    "requested_max_urls": req_clone.max_urls,
                    "used_max_urls": max_urls,
                    "requested_firecrawl_depth": req_clone.max_depth,
                    "used_firecrawl_depth": firecrawl_depth,
                    "requested_crawl_depth": req_clone.crawl_depth,
                    "used_crawl_depth": crawl_depth,
                    "requested_time_limit": req_clone.time_limit,
                    "used_time_limit": time_limit_secs,
                }
             });
             let _ = tx_clone.send(Ok(Event::Data(Data::new(completion_json.to_string()).event("completion")))).await;
            return;
        }

        // --- MDX Crawling ---
        sse_log_info(&tx_clone, format!("[MDX CRAWL START] Starting MDX crawl for {} initial URLs up to depth {}", initial_urls.len(), crawl_depth)).await;
        let mdx_crawler_start_time = Instant::now();

        let mut visited_urls = HashSet::<String>::new();
        let mut urls_to_process = VecDeque::<(String, usize)>::new();
        let mut mdx_results = Vec::<(String, String)>::new();
        let mut processed_count = 0;

        let target_domains = initial_urls.iter()
            .filter_map(|url_str| Url::parse(url_str).ok()?.domain().map(|s| s.to_string()))
            .collect::<HashSet<String>>();
        sse_log_info(&tx_clone, format!("[MDX CRAWL SCOPE] Allowed domains: {:?}", target_domains)).await;

        for url_str in &initial_urls {
            if let Ok(parsed_url) = Url::parse(url_str) {
                let url_string = parsed_url.to_string();
                 if visited_urls.insert(url_string.clone()) {
                     urls_to_process.push_back((url_string, 0));
                     sse_log_info(&tx_clone, format!("Queueing initial MDX crawl URL: {}", url_str)).await;
                 }
            } else {
                let warn_msg = format!("Ignoring invalid initial URL from Firecrawl: {}", url_str);
                 sse_log_warn(&tx_clone, warn_msg).await;
            }
        }

        let mut active_tasks = Vec::new();
        let crawl_loop_start_time = Instant::now();
        let mut last_log_time = Instant::now();
        let max_active_tasks = semaphore.available_permits().min(100); // Limit concurrent tasks

        while !urls_to_process.is_empty() || !active_tasks.is_empty() {
             // Check overall timeout
             if overall_start_time.elapsed() > timeout_duration {
                  let timeout_msg = format!("Overall process timed out after {:.2}s during MDX crawl phase", overall_start_time.elapsed().as_secs_f64());
                  sse_log_warn(&tx_clone, timeout_msg).await;
                  break; // Exit the loop on timeout
             }

            // Periodic status update
            if last_log_time.elapsed() > Duration::from_secs(10) {
                let queue_size = urls_to_process.len();
                let active_count = active_tasks.len();
                let permits = semaphore.available_permits();
                let elapsed_crawl = crawl_loop_start_time.elapsed().as_secs_f32();
                let log_msg = format!(
                    "[MDX CRAWL STATUS] {:.1}s elapsed | Queue: {} | Active Tasks: {} | Semaphore Permits: {}",
                    elapsed_crawl, queue_size, active_count, permits
                );
                sse_log_info(&tx_clone, log_msg).await;
                last_log_time = Instant::now();
            }

            // Launch new tasks if slots available and URLs exist
            while active_tasks.len() < max_active_tasks && !urls_to_process.is_empty() {
                if let Some((url, depth)) = urls_to_process.pop_front() {
                    if depth > crawl_depth {
                        let skip_msg = format!("Skipping URL due to max depth ({} > {}): {}", depth, crawl_depth, url);
                        debug!("{}", skip_msg); // Keep debug for server logs, don't flood client
                        // sse_log_info(&tx_clone, skip_msg).await; // Optional: log skips to client too
                        continue;
                    }

                    processed_count += 1;
                    let task_client = client.clone();
                    let task_semaphore = semaphore.clone();
                    let task_tx = tx_clone.clone(); // Clone sender for the task

                    active_tasks.push(spawn(async move {
                        let (filename_option, extracted_urls) = process_url(
                            &task_client,
                            &task_semaphore,
                            url.clone(),
                            &task_tx // Pass SSE sender
                        ).await;
                        (url, depth, filename_option, extracted_urls)
                    }));
                } else {
                     break; // No more URLs to queue right now
                }
            }

            // Process completed tasks
            if !active_tasks.is_empty() {
                let (result, _index, remaining_tasks) = select_all(active_tasks).await;
                active_tasks = remaining_tasks;

                match result {
                    Ok((original_url, current_depth, filename_option, extracted_urls)) => {
                         if let Some(filename) = filename_option {
                             // Attempt to read the saved file to include content in the final result
                             match fs::read_to_string(&filename) {
                                 Ok(content) => {
                                     if !content.trim().is_empty() {
                                         mdx_results.push((original_url.clone(), content));
                                     } else {
                                         sse_log_warn(&tx_clone, format!("Read MDX file {} but content was empty for {}", filename, original_url)).await;
                                     }
                                  },
                                 Err(e) => {
                                     let err_msg = format!("Failed to read saved MDX file {} for {}: {}", filename, original_url, e);
                                     sse_log_error(&tx_clone, err_msg).await;
                                  }
                             }
                         }

                         // Queue newly found URLs if depth allows
                         if current_depth < crawl_depth {
                             let mut queued_count = 0;
                             for next_url_str in extracted_urls {
                                 if let Ok(next_url) = Url::parse(&next_url_str) {
                                     if let Some(domain) = next_url.domain() {
                                        // Check if the domain is one of the initial target domains
                                        if target_domains.contains(domain) {
                                            let next_url_string = next_url.to_string();
                                             if visited_urls.insert(next_url_string.clone()) {
                                                 urls_to_process.push_back((next_url_string, current_depth + 1));
                                                 queued_count += 1;
                                             }
                                         }
                                     }
                                 }
                             }
                              if queued_count > 0 {
                                 debug!("Queued {} new URLs from {}", queued_count, original_url); // Server log only
                             }
                         }
                    }
                    Err(e) => {
                        // Task panicked
                        let panic_msg = format!("A crawl task failed (panicked): {:?}", e);
                        sse_log_error(&tx_clone, panic_msg).await;
                    }
                }
            } else {
                 // No active tasks, wait briefly if queue still has items
                 if !urls_to_process.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                 }
            }
             tokio::task::yield_now().await; // Yield to allow other tasks/IO
        } // End of while loop

        // --- Finalization ---
        let mdx_crawler_elapsed = mdx_crawler_start_time.elapsed();
        let overall_elapsed = overall_start_time.elapsed();

        sse_log_info(&tx_clone, format!(
            "[MDX CRAWL TIMING] ⏱️ MDX crawl phase took {:.2}s, processed {} URLs.",
            mdx_crawler_elapsed.as_secs_f64(), processed_count
        )).await;
         sse_log_info(&tx_clone, format!("[TOTAL TIMING] ⏱️ Total processing time: {:.2}s", overall_elapsed.as_secs_f64())).await;
         sse_log_info(&tx_clone, format!("[RESULTS] Collected {} MDX files.", mdx_results.len())).await;

        // Prepare final JSON data for the completion event
        let timings = json!({
            "firecrawl_api_seconds": firecrawl_elapsed.as_secs_f64(),
            "mdx_crawler_seconds": mdx_crawler_elapsed.as_secs_f64(),
            "total_seconds": overall_elapsed.as_secs_f64(),
        });

        let params_summary = json!({
             "query": req_clone.query,
             "requested_max_urls": req_clone.max_urls,
             "used_max_urls": max_urls,
             "requested_firecrawl_depth": req_clone.max_depth,
             "used_firecrawl_depth": firecrawl_depth,
             "requested_crawl_depth": req_clone.crawl_depth,
             "used_crawl_depth": crawl_depth,
             "requested_time_limit": req_clone.time_limit,
             "used_time_limit": time_limit_secs,
        });

        let response_message = format!(
            "Completed SuperCrawl. Processed {} URLs for MDX in {:.2}s. Collected {} MDX files.",
            processed_count,
            mdx_crawler_elapsed.as_secs_f64(),
            mdx_results.len()
        );

        let final_data = json!({
            "message": response_message,
            "processed_mdx_url_count": processed_count,
            "initial_urls_from_firecrawl": initial_urls,
            "mdx_files": mdx_results, // Include collected MDX content
            "timings": timings,
            "params": params_summary,
            // Logs are streamed, not included here
        });

        // Send the final completion event
        let completion_event = Event::Data(Data::new(final_data.to_string()).event("completion"));
        if tx_clone.send(Ok(completion_event)).await.is_err() {
             warn!("Failed to send final completion event to client.");
        }

        sse_log_info(&tx_clone, "[REQUEST END] ✅ SuperCrawl task finished.".to_string()).await;
        // Dropping tx_clone here will close the stream for the client
    });

    // Return the SSE stream based on the receiver
    let stream = ReceiverStream::new(rx);
    Ok(Sse::from_stream(stream).with_keep_alive(Duration::from_secs(15)))
}
