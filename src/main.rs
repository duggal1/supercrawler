use actix_web::{web, App, HttpServer, HttpResponse, Responder, rt::spawn};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{Semaphore, Mutex, mpsc};
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
use actix_cors::Cors;
use dotenv::dotenv;
use futures_util::{future::select_all, stream::StreamExt};
use serde_json::json;
use actix_web_lab::sse::{self, Sse, Event};
use std::convert::Infallible;
use tokio_stream::wrappers::ReceiverStream;
use std::io::Error as IoError;

mod yt_crawler;
use crate::yt_crawler::scrape_youtube_sse;

mod supercrawler;
use crate::supercrawler::{
    AppState as SuperCrawlerState,
    super_crawl
};

#[derive(Serialize, Deserialize)]
struct CrawlRequest {
    domains: Vec<String>,
    max_depth: Option<usize>,
}

#[derive(Serialize)]
struct FinalCrawlResult {
    message: String,
    logs: Vec<String>,
    mdx_files: Vec<(String, String)>,
}

struct CrawlerState {
    client: Client,
    semaphore: Arc<Semaphore>,
    logs: Arc<Mutex<Vec<String>>>,
}

/// Handles the YouTube crawl request: streams logs via SSE.
async fn yt_crawl_handler(
    query: web::Query<yt_crawler::ScrapeParams>,
    api_key_data: web::Data<String>,
) -> Result<actix_web_lab::sse::Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, actix_web::Error> {
    let api_key_arc = api_key_data.into_inner(); // This is Arc<String>

    if api_key_arc.is_empty() {
        error!("YOUTUBE_API_KEY from state is empty.");
        return Err(actix_web::error::ErrorInternalServerError("API key from state is empty"));
    }

    let params = query.into_inner();
    info!("Received YouTube crawl request: {:?}", params);

    let (tx, rx) = mpsc::channel(100);

    // Clone Arcs for the spawned task
    let api_key_for_task = api_key_arc.clone();
    spawn(scrape_youtube_sse(params, api_key_for_task, tx));

    let stream = ReceiverStream::new(rx)
        .map(|event| Ok::<_, Infallible>(event));

    Ok(Sse::from_stream(stream)
        .with_keep_alive(Duration::from_secs(15)))
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
/// Now also sends logs through the provided SSE sender.
async fn process_url_sse(
    client: &Client,
    semaphore: &Arc<Semaphore>,
    url: String,
    logs: &Arc<Mutex<Vec<String>>>,
    tx: &mpsc::Sender<sse::Event>,
) -> (Option<String>, Vec<String>) {
    let _permit = match semaphore.acquire().await {
        Ok(permit) => permit,
        Err(e) => {
            let err_msg = format!("Failed to acquire semaphore permit for {}: {}", url, e);
            error!("{}", err_msg);
            let _ = tx.send(sse::Event::Data(sse::Data::new(err_msg))).await;
            return (None, Vec::new());
        }
    };

    let log_msg = format!("Processing: {}", url);
    info!("{}", log_msg);
    let _ = tx.send(sse::Event::Data(sse::Data::new(log_msg.clone()))).await;
    logs.lock().await.push(log_msg);

    let resp = match client.get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            let err_msg = format!("Request failed for {}: {}", url, e);
            error!("{}", err_msg);
            let _ = tx.send(sse::Event::Data(sse::Data::new(err_msg.clone()))).await;
            logs.lock().await.push(err_msg);
            return (None, Vec::new());
        }
    };

    if !resp.status().is_success() {
        let err_msg = format!("Request failed for {} with status: {}", url, resp.status());
        error!("{}", err_msg);
        let _ = tx.send(sse::Event::Data(sse::Data::new(err_msg.clone()))).await;
        logs.lock().await.push(err_msg);
        return (None, Vec::new());
    }

    let content_type = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let mut mdx_content: Option<String> = None;
    let mut extracted_urls: Vec<String> = Vec::new();
    let mut content_processed = false;
    let client_ref = client;

    if content_type.contains("application/pdf") {
        let pdf_msg = format!("Fetching PDF content from {}", url);
        info!("{}", pdf_msg);
        let _ = tx.send(sse::Event::Data(sse::Data::new(pdf_msg))).await;
        if let Some(pdf_text) = fetch_pdf(client_ref, &url).await {
            let pdf_mdx = format!(
                "---\ntitle: \"PDF Document: {}\"\ndescription: \"Extracted text from PDF.\"\nsourceUrl: \"{}\"\n---\n\n{}",
                url.split('/').last().unwrap_or("document.pdf"), url, pdf_text
            );
            mdx_content = Some(pdf_mdx);
            content_processed = true;
        } else {
            let err_msg = format!("Failed to extract text from PDF: {}", url);
            error!("{}", err_msg);
            let _ = tx.send(sse::Event::Data(sse::Data::new(err_msg.clone()))).await;
            logs.lock().await.push(err_msg);
        }
    } else if content_type.contains("text/html") {
        let html_msg = format!("Fetching HTML content from {}", url);
        info!("{}", html_msg);
        let _ = tx.send(sse::Event::Data(sse::Data::new(html_msg))).await;
        match resp.text().await {
            Ok(html_content) => {
                mdx_content = Some(clean_to_mdx(&html_content, &url));
                extracted_urls = fetch_and_extract_urls(client_ref, &url).await;
                content_processed = true;
            }
            Err(e) => {
                let err_msg = format!("Failed to read HTML text from response {}: {}", url, e);
                error!("{}", err_msg);
                let _ = tx.send(sse::Event::Data(sse::Data::new(err_msg.clone()))).await;
                logs.lock().await.push(err_msg);
            }
        }
    } else {
        let skip_msg = format!("Skipping unsupported content type '{}' for URL: {}", content_type, url);
        info!("{}", skip_msg);
        let _ = tx.send(sse::Event::Data(sse::Data::new(skip_msg.clone()))).await;
        logs.lock().await.push(skip_msg);
    }

    let saved_filename = if let Some(mdx) = mdx_content {
        if !mdx.trim().is_empty() {
            save_mdx(&url, &mdx)
        } else {
            let warn_msg = format!("Generated MDX was empty for URL: {}", url);
            warn!("{}", warn_msg);
            let _ = tx.send(sse::Event::Data(sse::Data::new(warn_msg.clone()))).await;
            logs.lock().await.push(warn_msg);
            None
        }
    } else {
        None
    };

    if saved_filename.is_some() {
        let success_msg = format!("Successfully processed and saved: {}", url);
        info!("{}", success_msg);
        let _ = tx.send(sse::Event::Data(sse::Data::new(success_msg.clone()))).await;
        logs.lock().await.push(success_msg);
    } else if content_processed {
        let fail_msg = format!("Content processed but failed to save MDX for: {}", url);
        warn!("{}", fail_msg);
        let _ = tx.send(sse::Event::Data(sse::Data::new(fail_msg.clone()))).await;
        logs.lock().await.push(fail_msg);
    }

    (saved_filename, extracted_urls)
}

/// Handles the crawl request: streams logs via SSE, collects results.
async fn start_crawl(req: web::Json<CrawlRequest>, state: web::Data<CrawlerState>) -> actix_web_lab::sse::Sse<impl futures::Stream<Item = Result<Event, IoError>>> {
    let initial_domains = req.domains.clone();
    let max_depth = req.max_depth.unwrap_or(5).min(5);
    let start_time = Instant::now();
    let logs = state.logs.clone();

    let (tx, rx) = mpsc::channel(200);

    let start_msg = format!(
        "Received crawl request for {} domains with max_depth: {}. Streaming logs...",
        initial_domains.len(), max_depth
    );
    info!("{}", start_msg);
    let _ = tx.send(sse::Event::Data(sse::Data::new(start_msg.clone()))).await;
    logs.lock().await.push(start_msg);

    let client = state.client.clone();
    let semaphore = state.semaphore.clone();
    let req_clone = req.into_inner();

    spawn(async move {
        let mut visited_urls = HashSet::<String>::new();
        let mut urls_to_process = VecDeque::<(String, usize)>::new();
        let mut mdx_results = Vec::<(String, String)>::new();
        let mut processed_count = 0;
        let mut crawl_logs = Vec::new();

        let target_domains = initial_domains.iter()
            .filter_map(|d| Url::parse(d).ok()?.domain().map(|s| s.to_string()))
            .collect::<HashSet<String>>();

        for domain_url in initial_domains {
            if let Ok(parsed_url) = Url::parse(&domain_url) {
                let mut start_url = parsed_url;
                start_url.set_fragment(None);
                let url_string = start_url.to_string();

                if visited_urls.insert(url_string.clone()) {
                    urls_to_process.push_back((url_string.clone(), 0));
                    let queue_msg = format!("Queueing initial URL: {}", url_string);
                    info!("{}", queue_msg);
                    let _ = tx.send(sse::Event::Data(sse::Data::new(queue_msg.clone()))).await;
                    crawl_logs.push(queue_msg);
                } else {
                    let warn_msg = format!("Ignoring invalid initial URL: {}", domain_url);
                    warn!("{}", warn_msg);
                    let _ = tx.send(sse::Event::Data(sse::Data::new(warn_msg.clone()))).await;
                    crawl_logs.push(warn_msg.clone());
                    logs.lock().await.push(warn_msg);
                }
            } else {
                let warn_msg = format!("Ignoring invalid initial URL: {}", domain_url);
                warn!("{}", warn_msg);
                let _ = tx.send(sse::Event::Data(sse::Data::new(warn_msg.clone()))).await;
                crawl_logs.push(warn_msg.clone());
                logs.lock().await.push(warn_msg);
            }
        }

        let mut active_tasks = Vec::new();
        let max_active_tasks = semaphore.available_permits().min(100);

        while !urls_to_process.is_empty() || !active_tasks.is_empty() {
            while !urls_to_process.is_empty() && active_tasks.len() < max_active_tasks {
                if let Some((url, depth)) = urls_to_process.pop_front() {
                    if depth > max_depth {
                        let skip_msg = format!("Skipping URL due to max depth ({} > {}): {}", depth, max_depth, url);
                        info!("{}", skip_msg);
                        let _ = tx.send(sse::Event::Data(sse::Data::new(skip_msg))).await;
                        continue;
                    }

                    processed_count += 1;
                    let task_client = client.clone();
                    let task_semaphore = semaphore.clone();
                    let task_logs = logs.clone();
                    let task_tx = tx.clone();

                    active_tasks.push(spawn(async move {
                        let (filename_option, extracted_urls) = process_url_sse(
                            &task_client,
                            &task_semaphore,
                            url.clone(),
                            &task_logs,
                            &task_tx,
                        ).await;
                        (url, depth, filename_option, extracted_urls)
                    }));
                } else {
                    break;
                }
            }

            if active_tasks.is_empty() {
                if urls_to_process.is_empty() { break; }
                else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }

            let (result, _index, remaining_tasks) = select_all(active_tasks).await;
            active_tasks = remaining_tasks;

            match result {
                Ok((original_url, current_depth, filename_option, extracted_urls)) => {
                    if let Some(filename) = filename_option {
                        let read_msg = format!("Attempting to read saved MDX: {}", filename);
                        debug!("{}", read_msg);

                        match fs::read_to_string(&filename) {
                            Ok(content) => {
                                if !content.trim().is_empty() {
                                    let success_read_msg = format!("Successfully read MDX for {}", original_url);
                                    info!("{}", success_read_msg);
                                    mdx_results.push((original_url.clone(), content));
                                } else {
                                    let empty_read_msg = format!("Read MDX file {} but content was empty/whitespace for {}", filename, original_url);
                                    warn!("{}", empty_read_msg);
                                }
                            }
                            Err(e) => {
                                let err_read_msg = format!("Failed to read saved MDX file {} for {}: {}", filename, original_url, e);
                                error!("{}", err_read_msg);
                                let _ = tx.send(sse::Event::Data(sse::Data::new(err_read_msg.clone()))).await;
                                logs.lock().await.push(err_read_msg);
                            }
                        }
                    } else {
                        debug!("No filename returned from process_url_sse for {}", original_url);
                    }

                    if current_depth < max_depth {
                        let found_msg = format!("Found {} URLs at {} (depth {}/{})", extracted_urls.len(), original_url, current_depth, max_depth);
                        debug!("{}", found_msg);

                        let mut queued_count = 0;
                        for next_url_str in extracted_urls {
                            if let Ok(next_url_parsed) = Url::parse(&next_url_str) {
                                if let Some(domain) = next_url_parsed.domain() {
                                    if target_domains.contains(domain) {
                                        let mut next_url = next_url_parsed;
                                        next_url.set_fragment(None);
                                        let next_url_string = next_url.to_string();

                                        if visited_urls.insert(next_url_string.clone()) {
                                            urls_to_process.push_back((next_url_string, current_depth + 1));
                                            queued_count += 1;
                                        }
                                    }
                                }
                            } else {
                                let invalid_found_msg = format!("Ignoring invalid extracted URL: {}", next_url_str);
                                warn!("{}", invalid_found_msg);
                            }
                        }
                        if queued_count > 0 {
                            let queue_batch_msg = format!("Queued {} new URLs from {} (depth {})", queued_count, original_url, current_depth);
                            info!("{}", queue_batch_msg);
                        }
                    }
                }
                Err(e) => {
                    let task_fail_msg = format!("A crawl task failed to complete: {:?}", e);
                    error!("{}", task_fail_msg);
                    let _ = tx.send(sse::Event::Data(sse::Data::new(task_fail_msg.clone()))).await;
                    logs.lock().await.push(task_fail_msg);
                }
            }
            tokio::task::yield_now().await;
        }

        let elapsed = start_time.elapsed();
        let final_message = format!(
            "Crawl completed. Processed {} URLs for {} initial domains in {:.2} seconds. Found {} MDX files.",
            processed_count,
            req_clone.domains.len(),
            elapsed.as_secs_f64(),
            mdx_results.len()
        );
        info!("{}", final_message);
        crawl_logs.push(final_message.clone());
        logs.lock().await.push(final_message.clone());

        let final_result = FinalCrawlResult {
            message: final_message,
            logs: crawl_logs,
            mdx_files: mdx_results,
        };

        let final_json = match serde_json::to_string(&final_result) {
            Ok(json) => json,
            Err(e) => {
                let json_err_msg = format!("Failed to serialize final results: {}", e);
                error!("{}", json_err_msg);
                let error_event = sse::Event::Data(
                    sse::Data::new(json!({ "error": json_err_msg }).to_string())
                        .event("error")
                );
                let _ = tx.send(error_event).await;
                json!({ "error": json_err_msg }).to_string()
            }
        };

        let completion_event = sse::Event::Data(
            sse::Data::new(final_json)
                .event("completion")
        );
        if let Err(e) = tx.send(completion_event).await {
            error!("Failed to send final completion event to client: {}", e);
        }

        info!("Crawl task finished for domains: {:?}", req_clone.domains);
    });

    let stream = ReceiverStream::new(rx).map(|event| Ok::<_, IoError>(event));

    Sse::from_stream(stream)
        .with_keep_alive(Duration::from_secs(15))
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
    let host = "0.0.0.0";
    let port = 8080;

    info!("Initializing Crawler API with SuperCrawler and YouTube endpoints");

    let max_concurrency = 500;
    let request_timeout_secs = 60;
    let connect_timeout_secs = 30;

    info!("Configuration: Max Concurrency={}, Request Timeout={}s, Connect Timeout={}s",
          max_concurrency, request_timeout_secs, connect_timeout_secs);

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

    if let Ok(api_key) = std::env::var("YOUTUBE_API_KEY") {
        if api_key.len() >= 10 {
            let masked_key = format!("{}...{}", &api_key[0..5], &api_key[api_key.len()-5..]);
            info!("Found YOUTUBE_API_KEY in environment: {}", masked_key);
        } else if !api_key.is_empty() {
            info!("Found YOUTUBE_API_KEY in environment (short key)");
        } else {
            warn!("YOUTUBE_API_KEY is set but empty!");
        }
    } else {
        warn!("YOUTUBE_API_KEY not found in environment");
    }

    let client = Client::builder()
        .pool_max_idle_per_host(max_concurrency / 2)
        .timeout(Duration::from_secs(request_timeout_secs))
        .connect_timeout(Duration::from_secs(connect_timeout_secs))
        .pool_idle_timeout(Duration::from_secs(120))
        .user_agent("SuperCrawler/1.6-SSE")
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .expect("Failed to build reqwest client");

    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    let old_crawl_logs = Arc::new(Mutex::new(Vec::new()));
    let regular_state = web::Data::new(CrawlerState {
        client: client.clone(),
        semaphore: semaphore.clone(),
        logs: old_crawl_logs,
    });

    let super_state = web::Data::new(SuperCrawlerState {
        client: client.clone(),
        semaphore: semaphore.clone(),
    });

    let youtube_api_key = match std::env::var("YOUTUBE_API_KEY") {
        Ok(key) if !key.is_empty() => {
            info!("Loaded YOUTUBE_API_KEY from environment.");
            key
        }
        _ => {
            error!("YOUTUBE_API_KEY not set or empty in environment. YouTube crawler endpoint will fail.");
            String::new()
        }
    };
    let youtube_api_key_data = web::Data::new(youtube_api_key);

    info!("Starting server at http://{}:{}", host, port);
    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![actix_web::http::header::ACCEPT, actix_web::http::header::CONTENT_TYPE])
            .max_age(3600);

        App::new()
            .wrap(cors)
            .app_data(regular_state.clone())
            .app_data(super_state.clone())
            .app_data(youtube_api_key_data.clone())
            .route("/crawl", web::post().to(start_crawl))
            .route("/supercrawler", web::post().to(super_crawl))
            .route("/yt-crawler", web::get().to(yt_crawl_handler))
            .route("/mdx/{domain}/{path:.*}", web::get().to(get_mdx))
    })
    .bind((host, port))?
    .run()
    .await
}

