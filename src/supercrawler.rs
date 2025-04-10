use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use std::collections::HashSet;
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
use log::{info, error, debug, warn};
use std::time::{Duration, Instant};
use actix_cors::Cors;
use futures::future::join_all;
use std::env;
use serde_json::json;

// Define the AppState struct
pub struct AppState {
    pub client: Client,
    pub semaphore: Arc<Semaphore>,
    pub tx: mpsc::Sender<(String, usize, usize, String)>,
    pub logs: Arc<tokio::sync::Mutex<Vec<String>>>,
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

struct CrawlerState {
    client: Client,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<(String, usize, usize, String)>,
    logs: Arc<tokio::sync::Mutex<Vec<String>>>,
}

/// Converts an HTML element's inline content to Markdown syntax.
fn element_to_markdown(element: ElementRef) -> String {
    let mut markdown = String::new();
    for node in element.children() {
        if let Some(text) = node.value().as_text() {
            markdown.push_str(text.trim());
        } else if let Some(child_elem) = node.value().as_element() {
            if let Some(child_ref) = ElementRef::wrap(node) {
                match child_elem.name() {
                    "a" => {
                        let href = child_elem.attr("href").unwrap_or("");
                        let link_text = child_ref.text().collect::<Vec<_>>().join(" ").trim().to_string();
                        markdown.push_str(&format!("[{}]({})", link_text, href));
                    }
                    "strong" | "b" => {
                        let inner = element_to_markdown(child_ref);
                        markdown.push_str(&format!("**{}**", inner));
                    }
                    "em" | "i" => {
                        let inner = element_to_markdown(child_ref);
                        markdown.push_str(&format!("*{}*", inner));
                    }
                    "code" => {
                        let inner = element_to_markdown(child_ref);
                        markdown.push_str(&format!("`{}`", inner));
                    }
                    "span" => {
                        // Preserve spans as they may contain important text
                        markdown.push_str(&element_to_markdown(child_ref));
                    }
                    "br" => {
                        // Add proper line break for <br> tags
                        markdown.push_str("\n");
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
            let level = tag[1..].parse::<usize>().unwrap();
            let text = element.text().collect::<Vec<_>>().join(" ").trim().to_string();
            format!("{} {}\n\n", "#".repeat(level), text)
        }
        "p" => {
            let text = element_to_markdown(element);
            if text.is_empty() { String::new() } else { format!("{}\n\n", text) }
        }
        "ul" | "ol" => {
            let list_type = if tag == "ul" { "*" } else { "1." };
            let mut list_mdx = String::new();
            for li in element.select(&Selector::parse("li").unwrap()) {
                let li_text = element_to_markdown(li);
                if !li_text.is_empty() {
                    list_mdx.push_str(&format!("{} {}\n", list_type, li_text));
                }
            }
            if list_mdx.is_empty() { String::new() } else { format!("{}\n", list_mdx) }
        }
        "pre" => {
            if let Some(code) = element.select(&Selector::parse("code").unwrap()).next() {
                // Extract language class if available
                let class_attr = code.value().attr("class").unwrap_or("");
                let language = if class_attr.contains("language-") {
                    let re = Regex::new(r"language-(\w+)").unwrap();
                    if let Some(caps) = re.captures(class_attr) {
                        caps.get(1).map_or("", |m| m.as_str())
                    } else {
                        ""
                    }
                } else if class_attr.contains("jsx") || class_attr.contains("javascript") {
                    "jsx"
                } else if class_attr.contains("typescript") || class_attr.contains("ts") {
                    "tsx"
                } else if class_attr.contains("bash") || class_attr.contains("shell") || class_attr.contains("sh") {
                    "bash"
                } else {
                    ""
                };
                
                // Clean up the code text - remove excessive whitespace and normalize line breaks
                let code_text = code.text().collect::<Vec<_>>().join("\n")
                    .lines()
                    .map(|line| line.trim_end())
                    .collect::<Vec<_>>()
                    .join("\n")
                    .trim()
                    .to_string();
                
                if code_text.is_empty() { 
                    String::new() 
                } else { 
                    // For Next.js and React code, default to jsx if no language specified
                    let lang_hint = if language.is_empty() && 
                       (code_text.contains("import") && (code_text.contains("react") || code_text.contains("next"))) {
                        "jsx"
                    } else {
                        language
                    };
                    
                    format!("```{}\n{}\n```\n\n", lang_hint, code_text) 
                }
            } else {
                String::new()
            }
        }
        "img" => {
            if let Some(src) = element.value().attr("src") {
                let alt = element.value().attr("alt").unwrap_or("");
                format!("![{}]({})\n\n", alt, src)
            } else {
                String::new()
            }
        }
        "a" => {
            if let Some(href) = element.value().attr("href") {
                let text = element_to_markdown(element);
                if text.is_empty() {
                    String::new()
                } else {
                    format!("[{}]({})\n\n", text, href)
                }
            } else {
                String::new()
            }
        }
        "blockquote" => {
            let content = element.children()
                .filter_map(ElementRef::wrap)
                .map(|e| process_element(e))
                .collect::<Vec<_>>()
                .join("");

            if content.is_empty() {
                String::new()
            } else {
                // Add proper blockquote formatting by prefixing each line with >
                let quoted = content.lines()
                    .map(|line| if line.is_empty() { ">".to_string() } else { format!("> {}", line) })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{}\n\n", quoted)
            }
        }
        "table" => {
            let mut table_mdx = String::new();
            let mut headers = Vec::new();
            let mut rows = Vec::new();
            
            // Extract headers
            if let Some(thead) = element.select(&Selector::parse("thead tr").unwrap()).next() {
                for th in thead.select(&Selector::parse("th").unwrap()) {
                    headers.push(element_to_markdown(th));
                }
            }
            
            // Extract rows
            for tr in element.select(&Selector::parse("tbody tr").unwrap()) {
                let mut row = Vec::new();
                for td in tr.select(&Selector::parse("td").unwrap()) {
                    row.push(element_to_markdown(td));
                }
                rows.push(row);
            }
            
            // Build table
            if !headers.is_empty() {
                // Header row
                table_mdx.push_str(&format!("| {} |\n", headers.join(" | ")));
                
                // Separator row
                table_mdx.push_str(&format!("| {} |\n", headers.iter().map(|_| "---").collect::<Vec<_>>().join(" | ")));
                
                // Data rows
                for row in rows {
                    table_mdx.push_str(&format!("| {} |\n", row.join(" | ")));
                }
                
                table_mdx.push_str("\n");
            }
            
            table_mdx
        }
        "div" | "section" | "article" | "main" => {
            let mut mdx = String::new();
            for child in element.children() {
                if let Some(child_elem) = ElementRef::wrap(child) {
                    mdx.push_str(&process_element(child_elem));
                }
            }
            mdx
        }
        "nav" | "footer" | "aside" | "script" | "style" | "noscript" | "iframe" | "form" => {
            String::new() // Ignore these elements
        }
        _ => {
            let mut mdx = String::new();
            for child in element.children() {
                if let Some(child_elem) = ElementRef::wrap(child) {
                    mdx.push_str(&process_element(child_elem));
                }
            }
            mdx
        }
    }
}

/// Converts raw content (HTML or plain text) to structured MDX.
fn clean_to_mdx(content: &str) -> String {
    let re_noise = Regex::new(r"(?i)(advertisement|footer|sidebar|nav|script|style|header)").unwrap();
    let clean = re_noise.replace_all(content, "").to_string();
    let doc = Html::parse_document(&clean);

    let mut mdx = String::new();

    // Extract metadata
    let title = doc.select(&Selector::parse("title").unwrap())
        .next()
        .map(|t| t.text().collect::<Vec<_>>().join(" "))
        .unwrap_or("Untitled".to_string());
    
    let description = doc.select(&Selector::parse("meta[name='description']").unwrap())
        .next()
        .and_then(|m| m.value().attr("content"))
        .unwrap_or("No description");
    
    let keywords = doc.select(&Selector::parse("meta[name='keywords']").unwrap())
        .next()
        .and_then(|m| m.value().attr("content"))
        .unwrap_or("No keywords");
    
    // Extract additional metadata for enriched context
    let author = doc.select(&Selector::parse("meta[name='author']").unwrap())
        .next()
        .and_then(|m| m.value().attr("content"))
        .unwrap_or("");
    
    let published_date = doc.select(&Selector::parse("meta[property='article:published_time']").unwrap())
        .next()
        .and_then(|m| m.value().attr("content"))
        .unwrap_or("");
    
    // Build frontmatter with richer metadata
    mdx.push_str("---\n");
    mdx.push_str(&format!("title: {}\n", title));
    mdx.push_str(&format!("description: {}\n", description));
    mdx.push_str(&format!("keywords: {}\n", keywords));
    
    if !author.is_empty() {
        mdx.push_str(&format!("author: {}\n", author));
    }
    
    if !published_date.is_empty() {
        mdx.push_str(&format!("date: {}\n", published_date));
    }
    
    // Add source URL as reference
    if let Some(canonical) = doc.select(&Selector::parse("link[rel='canonical']").unwrap())
        .next()
        .and_then(|m| m.value().attr("href")) {
        mdx.push_str(&format!("canonicalUrl: {}\n", canonical));
    }
    
    mdx.push_str("---\n\n");

    // Add table of contents placeholder for long articles
    if doc.select(&Selector::parse("h2, h3, h4, h5, h6").unwrap()).count() > 3 {
        mdx.push_str("## Table of Contents\n\n");
        mdx.push_str("{/* Auto-generated table of contents */}\n\n");
    }

    // Process body content
    if let Some(body) = doc.select(&Selector::parse("body").unwrap()).next() {
        let main_content = doc.select(&Selector::parse("main, article, .content, #content, .post, .article").unwrap())
            .next()
            .unwrap_or(body);
            
        for child in main_content.children() {
            if let Some(element) = ElementRef::wrap(child) {
                mdx.push_str(&process_element(element));
            }
        }
    }

    // Add a section for related links if they exist
    let related_links: Vec<_> = doc.select(&Selector::parse("a[rel='related'], .related a, .see-also a").unwrap()).collect();
    if !related_links.is_empty() {
        mdx.push_str("\n## Related Resources\n\n");
        for link in related_links {
            if let Some(href) = link.value().attr("href") {
                let text = link.text().collect::<Vec<_>>().join(" ").trim().to_string();
                if !text.is_empty() {
                    mdx.push_str(&format!("- [{}]({})\n", text, href));
                }
            }
        }
    }

    mdx.trim().to_string()
}

/// Saves MDX content to a file based on the URL.
fn save_mdx(url: &str, mdx: &str) {
    let parsed = match Url::parse(url) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to parse URL {}: {}", url, e);
            return;
        }
    };
    let domain = parsed.domain().unwrap_or("unknown");
    let path_binding = parsed.path().replace('/', "_").trim_start_matches('_').to_string();
    let filename = format!("./output/{}/{}.mdx", domain, path_binding);
    let dir = Path::new(&filename).parent().unwrap();

    if let Err(e) = fs::create_dir_all(dir) {
        error!("Failed to create directory {}: {}", dir.display(), e);
        return;
    }
    if let Ok(mut file) = File::create(&filename) {
        if let Err(e) = file.write_all(mdx.as_bytes()) {
            error!("Failed to write MDX to {}: {}", filename, e);
        } else {
            info!("Successfully saved MDX file: {}", filename);
        }
    }
}

/// Generates a filename from a URL for storing MDX files.
fn url_to_filename(url: &str) -> String {
    let parsed = Url::parse(url).unwrap_or_else(|_| Url::parse("http://unknown").unwrap());
    let domain = parsed.domain().unwrap_or("unknown");
    let path_binding = parsed.path().replace('/', "_").trim_start_matches('_').to_string();
    format!("./output/{}/{}.mdx", domain, path_binding)
}

/// Extracts URLs from an HTML page for further crawling.
async fn fetch_and_extract_urls(client: &Client, url: &str) -> Vec<String> {
    let mut urls = Vec::new();
    if url.ends_with(".pdf") {
        return urls;
    }
    if let Ok(resp) = client.get(url).send().await {
        if let Ok(text) = resp.text().await {
            let doc = Html::parse_document(&text);
            let selector = Selector::parse("a[href]").unwrap();
            for element in doc.select(&selector) {
                if let Some(href) = element.value().attr("href") {
                    if let Ok(abs_url) = Url::parse(url).unwrap().join(href) {
                        urls.push(abs_url.to_string());
                    }
                }
            }
        }
    }
    urls
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

/// Process a URL and extract content
async fn process_url(
    client: &Client,
    semaphore: &Arc<Semaphore>,
    url: &str,
    current_depth: usize,
    max_depth: usize,
    domain: &str,
    tx: &mpsc::Sender<(String, usize, usize, String)>,
    logs: &Arc<tokio::sync::Mutex<Vec<String>>>,
) -> bool {
    let permit = semaphore.acquire().await.unwrap();
    info!("🔄 Fetching URL: {} (depth: {}/{})", url, current_depth, max_depth);
    logs.lock().await.push(format!("🔄 Fetching URL: {} (depth: {}/{})", url, current_depth, max_depth));

    let resp = match client.get(url).send().await {
        Ok(resp) => {
            info!("✅ Got response for URL: {} - Status: {}", url, resp.status());
            logs.lock().await.push(format!("✅ Got response for URL: {} - Status: {}", url, resp.status()));
            resp
        },
        Err(e) => {
            error!("❌ Failed to fetch URL {}: {}", url, e);
            logs.lock().await.push(format!("❌ Failed to fetch URL {}: {}", url, e));
            drop(permit);
            return false;
        }
    };

    let content_type = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");
    
    info!("📄 Content type for {}: {}", url, content_type);
    logs.lock().await.push(format!("📄 Content type for {}: {}", url, content_type));
    
    let content = if content_type.contains("application/pdf") {
        logs.lock().await.push(format!("📚 Processing PDF content from {}", url));
        fetch_pdf(client, url).await
    } else if content_type.contains("text/html") {
        logs.lock().await.push(format!("🌐 Processing HTML content from {}", url));
        fetch_html(client, url).await
    } else {
        logs.lock().await.push(format!("⚠️ Unsupported content type for {}: {}", url, content_type));
        None
    };

    let mut success = false;
    if let Some(content) = content {
        info!("✅ Successfully fetched content from {}, length: {} bytes", url, content.len());
        logs.lock().await.push(format!("✅ Successfully fetched content from {}, length: {} bytes", url, content.len()));
        
        let mdx = clean_to_mdx(&content);
        save_mdx(url, &mdx);
        info!("📝 Successfully converted and saved MDX for URL: {}", url);
        logs.lock().await.push(format!("📝 Successfully converted and saved MDX for URL: {}", url));
        success = true;
    }

    drop(permit);
    if current_depth < max_depth {
        let urls = fetch_and_extract_urls(client, url).await;
        info!("🔗 Found {} URLs at {} (depth: {}/{})", urls.len(), url, current_depth, max_depth);
        logs.lock().await.push(format!("🔗 Found {} URLs at {} (depth: {}/{})", urls.len(), url, current_depth, max_depth));
        
        for next_url in urls {
            if let Ok(parsed) = Url::parse(&next_url) {
                if let Ok(domain_url) = Url::parse(domain) {
                    if parsed.domain() == domain_url.domain() {
                        info!("📋 Queueing URL for processing: {} at depth {}", next_url, current_depth + 1);
                        logs.lock().await.push(format!("📋 Queueing URL for processing: {} at depth {}", next_url, current_depth + 1));
                        let _ = tx.send((next_url.clone(), current_depth + 1, max_depth, domain.to_string())).await;
                    }
                }
            }
        }
    }
    
    success
}

/// Poll the job status from Firecrawl Deep Research API
async fn poll_job_status(client: &Client, job_id: &str, api_key: &str) -> Result<DeepResearchResponse, String> {
    info!("[POLLING START] 😍 Job ID: {}", job_id);
    let start_time = Instant::now();
    let url = format!("https://api.firecrawl.dev/v1/deep-research/{}", job_id);
    let max_duration = Duration::from_secs(600); // 10 minutes max (reduced from 15min)
    let mut retry_count = 0;
    let max_retries = 5;

    while start_time.elapsed() < max_duration {
        info!("[POLLING] ✅ Checking status for Job ID: {}", job_id);
        
        match client.get(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .timeout(Duration::from_secs(30)) // Add explicit timeout per request
            .send()
            .await {
                Ok(response) => {
                    let status_code = response.status();
                    info!("[POLLING RESPONSE] Status code: {}", status_code);
                    retry_count = 0; // Reset retry count on success
                    
                    if !status_code.is_success() {
                        warn!("[POLLING WARNING] ⚠️ Non-success status code: {}", status_code);
                        let error_text = response.text().await.unwrap_or_default();
                        warn!("[POLLING ERROR BODY] {}", error_text);
                        return Err(format!("API returned error status: {}, body: {}", status_code, error_text));
                    }

                    match response.json::<DeepResearchResponse>().await {
                        Ok(status_data) => {
                            info!("[POLLING RESPONSE] ✔️ Status: {:?}, Has ID: {}, Success: {}", 
                                status_data.status, 
                                status_data.id.is_some(),
                                status_data.success);
                            
                            if let Some(status) = &status_data.status {
                                if status == "completed" {
                                    info!("[POLLING COMPLETE] ✅ Job {} finished", job_id);
                                    return Ok(status_data);
                                }
                            }
                            
                            info!("[POLLING WAIT] 🚧 Status not completed, waiting 3s...");
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        },
                        Err(e) => {
                            error!("[POLLING JSON ERROR] ❌ Failed to parse response: {}", e);
                            return Err(format!("Failed to parse status response: {}", e));
                        }
                    }
                },
                Err(e) => {
                    retry_count += 1;
                    let retry_delay = 2_u64.pow(retry_count as u32) * 375; // Exponential backoff: 0.75s, 1.5s, 3s, 6s, 12s
                    error!("[POLLING REQUEST ERROR] ❌ Failed to check status: {} (Retry {}/{})", e, retry_count, max_retries);
                    
                    if retry_count >= max_retries {
                        return Err(format!("Failed to check job status after {} retries: {}", max_retries, e));
                    }
                    
                    info!("[POLLING RETRY] Waiting {}ms before retry #{}", retry_delay, retry_count + 1);
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                }
            }
    }

    error!("[POLLING TIMEOUT] ⚠️ Job {} exceeded timeout of {}s", job_id, max_duration.as_secs());
    Err(format!("Job polling timed out after {}s", start_time.elapsed().as_secs()))
}

/// Fetch deep research URLs from Firecrawl API
async fn fetch_deep_research_urls(
    client: &Client,
    query: &str,
    max_urls: u32,
    time_limit: u32,
    max_depth: usize,
    api_key: &str,
) -> Result<Vec<String>, String> {
    let firecrawl_url = "https://api.firecrawl.dev/v1/deep-research";
    
    info!("[FIRECRAWL START] 🔥 Initiating deep research API call");
    info!("[FIRECRAWL REQUEST] Query: {}, Max URLs: {}, Time Limit: {}s, Max Depth: {}", 
          query, max_urls, time_limit, max_depth);
    
    // Ensure time_limit is within reasonable bounds (150-600 seconds)
    let validated_time_limit = if time_limit < 150 {
        info!("[FIRECRAWL PARAM] Time limit too low, setting to minimum 150s");
        150
    } else if time_limit > 600 {
        info!("[FIRECRAWL PARAM] Time limit too high, setting to maximum 600s");
        600
    } else {
        time_limit
    };
    
    // Ensure max_depth is within reasonable bounds (0-5)
    let validated_max_depth = if max_depth > 5 {
        info!("[FIRECRAWL PARAM] Max depth too high, setting to maximum 5");
        5
    } else {
        max_depth
    };
    
    let payload = json!({
        "query": query,
        "maxUrls": max_urls,
        "timeLimit": validated_time_limit,
        "maxDepth": validated_max_depth
    });

    info!("[FIRECRAWL PAYLOAD] {}", payload.to_string());

    let response = match client
        .post(firecrawl_url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(60)) // 60s timeout for initial request
        .json(&payload)
        .send()
        .await {
            Ok(resp) => {
                let status = resp.status();
                info!("[FIRECRAWL RESPONSE] Status: {}", status);
                
                if !status.is_success() {
                    let error_text = resp.text().await.unwrap_or_default();
                    error!("[FIRECRAWL ERROR] Status: {}, Body: {}", status, error_text);
                    return Err(format!("Firecrawl API error: {} - {}", status, error_text));
                }
                
                // Debug response body as text before trying to parse as JSON
                let body_text = resp.text().await.unwrap_or_default();
                info!("[FIRECRAWL RESPONSE BODY] {}", body_text);
                
                // Parse the response body manually since we already consumed it as text
                match serde_json::from_str::<DeepResearchResponse>(&body_text) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        error!("[FIRECRAWL JSON PARSE ERROR] Failed to parse response: {}", e);
                        error!("[FIRECRAWL RAW RESPONSE] {}", body_text);
                        return Err(format!("Failed to parse response: {}, Raw response: {}", e, body_text));
                    }
                }
            },
            Err(e) => {
                error!("[FIRECRAWL REQUEST ERROR] {}", e);
                return Err(format!("Request error: {}", e));
            }
        };

    // Process response based on status
    let research_urls = if response.success {
        match response.status.as_deref() {
            Some("completed") => {
                info!("[FIRECRAWL SUCCESS] Research completed immediately");
                
                if let Some(data) = response.data {
                    let urls: Vec<String> = data.sources.into_iter()
                        .map(|source| source.url)
                        .collect();
                    
                    info!("[FIRECRAWL URLS] Found {} URLs", urls.len());
                    for (i, url) in urls.iter().enumerate() {
                        info!("[FIRECRAWL URL {}] {}", i+1, url);
                    }
                    
                    Ok(urls)
                } else {
                    error!("[FIRECRAWL ERROR] Missing data in completed response");
                    Err("Missing data field in completed response".to_string())
                }
            },
            _ => {
                // Handle async job
                if let Some(job_id) = response.id {
                    info!("[FIRECRAWL ASYNC] 🚀 Job started, polling ID: {}", job_id);
                    
                    match poll_job_status(client, &job_id, api_key).await {
                        Ok(poll_result) => {
                            if let Some(data) = poll_result.data {
                                let urls: Vec<String> = data.sources.into_iter()
                                    .map(|source| source.url)
                                    .collect();
                                
                                info!("[FIRECRAWL POLL SUCCESS] Found {} URLs after polling", urls.len());
                                for (i, url) in urls.iter().enumerate() {
                                    info!("[FIRECRAWL URL {}] {}", i+1, url);
                                }
                                
                                Ok(urls)
                            } else {
                                error!("[FIRECRAWL POLL ERROR] No data in poll response");
                                Err("No data in poll response".to_string())
                            }
                        },
                        Err(e) => {
                            error!("[FIRECRAWL POLL FAILED] {}", e);
                            Err(format!("Polling failed: {}", e))
                        }
                    }
                } else {
                    error!("[FIRECRAWL ERROR] No job ID for async job");
                    Err("No job ID returned for async job".to_string())
                }
            }
        }
    } else {
        let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
        error!("[FIRECRAWL FAILURE] API returned error: {}", error_msg);
        Err(format!("API error: {}", error_msg))
    };

    research_urls
}

/// Get API key from environment or .env file
fn get_api_key() -> String {
    match env::var("FIRECRAWL_API_KEY") {
        Ok(key) => {
            if key.is_empty() || key == "fc-your-key" {
                warn!("⚠️ FIRECRAWL_API_KEY environment variable is empty or default");
                "fc-your-key".to_string()
            } else {
                let masked_key = format!("{}...{}", &key[0..5], &key[key.len()-5..key.len()]);
                info!("[ENV] Using Firecrawl API key: {}", masked_key);
                key
            }
        },
        Err(e) => {
            warn!("⚠️ Failed to get FIRECRAWL_API_KEY: {}", e);
            
            // Check .env file directly as fallback
            if let Ok(content) = fs::read_to_string(".env") {
                for line in content.lines() {
                    if line.starts_with("FIRECRAWL_API_KEY=") {
                        let key = line.split('=').nth(1).unwrap_or("").trim();
                        if !key.is_empty() && key != "fc-your-key" {
                            info!("[ENV] Found Firecrawl API key in .env file");
                            return key.to_string();
                        }
                    }
                }
            }
            
            "fc-your-key".to_string()
        }
    }
}

pub async fn super_crawl(req: web::Json<SuperCrawlerRequest>, state: web::Data<AppState>) -> impl Responder {
    // Log start of request
    info!("[REQUEST START] 🚀 Received super crawler request");
    state.logs.lock().await.push("[REQUEST START] 🚀 Received super crawler request".to_string());
    
    let request_json = serde_json::to_string(&req).unwrap_or_default();
    info!("[REQUEST BODY] {}", request_json);
    state.logs.lock().await.push(format!("[REQUEST BODY] {}", request_json));
    
    // Track overall request timing
    let overall_start_time = Instant::now();
    
    // Get API key from environment with better error handling
    let api_key = get_api_key();
    
    // Validate parameters with new defaults
    let max_urls = req.max_urls.unwrap_or(20).min(120).max(15); // Range: 15-120, default: 20
    let firecrawl_depth = req.max_depth.unwrap_or(1).min(5); // Range: 0-5, default: 1
    
    // The depth for MDX crawling - default to 0 (just the pages themselves) if not specified
    let crawl_depth = req.crawl_depth.unwrap_or(0).min(5);
    
    // Ensure time_limit is within reasonable bounds (150-600 seconds)
    let time_limit = match req.time_limit {
        Some(t) if t >= 150 && t <= 600 => t,
        Some(t) if t < 150 => {
            info!("[PARAM WARNING] Time limit {} too low, using minimum 150s", t);
            state.logs.lock().await.push(format!("[PARAM WARNING] Time limit {} too low, using minimum 150s", t));
            150
        },
        Some(t) if t > 600 => {
            info!("[PARAM WARNING] Time limit {} too high, using maximum 600s", t);
            state.logs.lock().await.push(format!("[PARAM WARNING] Time limit {} too high, using maximum 600s", t));
            600
        },
        _ => {
            info!("[PARAM INFO] Using default time limit of 600s");
            state.logs.lock().await.push("[PARAM INFO] Using default time limit of 600s".to_string());
            600 // New default is 600
        }
    };
    
    info!("[VALIDATED PARAMS] Query: {}, MaxUrls: {}, FirecrawlDepth: {}, CrawlDepth: {}, TimeLimit: {}", 
         req.query, max_urls, firecrawl_depth, crawl_depth, time_limit);
    state.logs.lock().await.push(format!("[VALIDATED PARAMS] Query: {}, MaxUrls: {}, FirecrawlDepth: {}, CrawlDepth: {}, TimeLimit: {}", 
                                       req.query, max_urls, firecrawl_depth, crawl_depth, time_limit));
    
    // Track Firecrawl API timing
    let firecrawl_start_time = Instant::now();
    
    // First, get URLs from Deep Research API
    let deep_research_result = fetch_deep_research_urls(
        &state.client,
        &req.query,
        max_urls,
        time_limit,
        firecrawl_depth,
        &api_key,
    ).await;
    
    // Calculate Firecrawl time
    let firecrawl_elapsed = firecrawl_start_time.elapsed();
    info!("[FIRECRAWL TIMING] ⏱️ Firecrawl API took {:.2}s", firecrawl_elapsed.as_secs_f64());
    state.logs.lock().await.push(format!("[FIRECRAWL TIMING] ⏱️ Firecrawl API took {:.2}s", firecrawl_elapsed.as_secs_f64()));

    let urls = match deep_research_result {
        Ok(urls) => {
            let msg = format!("✅ Retrieved {} URLs from Deep Research API", urls.len());
            info!("{}", msg);
            state.logs.lock().await.push(msg);
            urls
        },
        Err(e) => {
            let msg = format!("❌ Failed to fetch URLs: {}", e);
            error!("{}", msg);
            state.logs.lock().await.push(msg.clone());
            
            return HttpResponse::InternalServerError().json(json!({
                "error": msg,
                "logs": state.logs.lock().await.clone(),
                "timings": {
                    "firecrawl_api_seconds": firecrawl_elapsed.as_secs_f64(),
                    "total_seconds": overall_start_time.elapsed().as_secs_f64()
                }
            }));
        }
    };

    if urls.is_empty() {
        let msg = "⚠️ No URLs found from Deep Research API";
        warn!("{}", msg);
        state.logs.lock().await.push(msg.to_string());
        
        return HttpResponse::NotFound().json(json!({
            "message": "No URLs found to crawl",
            "logs": state.logs.lock().await.clone(),
            "timings": {
                "firecrawl_api_seconds": firecrawl_elapsed.as_secs_f64(),
                "total_seconds": overall_start_time.elapsed().as_secs_f64()
            }
        }));
    }

    // Track MDX crawler timing
    let mdx_crawler_start_time = Instant::now();
    
    // Process URLs in parallel with optimized concurrency
    info!("[CRAWL CONFIG] CrawlDepth: {}, FirecrawlDepth: {}, URLs to process: {}", 
          crawl_depth, firecrawl_depth, urls.len());
    state.logs.lock().await.push(format!(
        "[CRAWL CONFIG] CrawlDepth: {}, FirecrawlDepth: {}, URLs to process: {}", 
        crawl_depth, firecrawl_depth, urls.len()
    ));
    
    let mut tasks = Vec::new();
    let processed_urls = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

    for url in urls.clone() {
        let client = state.client.clone();
        let semaphore = state.semaphore.clone();
        let tx = state.tx.clone();
        let logs = state.logs.clone();
        let processed_urls = processed_urls.clone();
        let url_clone = url.clone();

        tasks.push(tokio::spawn(async move {
            if processed_urls.lock().await.contains(&url) {
                info!("⏭️ Skipping already processed URL: {}", url);
                return None;
            }
            processed_urls.lock().await.insert(url.clone());
            info!("🔄 Starting to process URL: {}", url);

            process_url(
                &client,
                &semaphore,
                &url,
                0,
                crawl_depth, // Use crawl_depth for MDX generation
                &url_clone,
                &tx,
                &logs
            ).await.then(|| Some(url))
        }));
    }

    // Wait for all tasks with timeout
    let timeout_duration = Duration::from_secs(req.time_limit.unwrap_or(150) as u64);
    info!("[TIMEOUT] Set crawl timeout to {}s", timeout_duration.as_secs());
    state.logs.lock().await.push(format!("[TIMEOUT] Set crawl timeout to {}s", timeout_duration.as_secs()));
    
    let results = tokio::time::timeout(timeout_duration, join_all(tasks)).await;

    let mut successful_urls = Vec::new();
    match results {
        Ok(task_results) => {
            info!("✅ All tasks completed within timeout");
            state.logs.lock().await.push("✅ All tasks completed within timeout".to_string());
            
            for result in task_results {
                match result {
                    Ok(Some(url_str)) => {
                        info!("✅ Successfully processed URL: {:?}", url_str);
                        state.logs.lock().await.push(format!("✅ Successfully processed URL: {:?}", url_str));
                        successful_urls.push(url_str);
                    },
                    Ok(None) => {
                        debug!("⏭️ URL was skipped (duplicate or already processed)");
                    },
                    Err(e) => {
                        error!("❌ Task error: {}", e);
                        state.logs.lock().await.push(format!("❌ Task error: {}", e));
                    }
                }
            }
        },
        Err(_) => {
            warn!("⚠️ Timeout reached, some tasks did not complete");
            state.logs.lock().await.push("⚠️ Timeout reached, some tasks did not complete".to_string());
        }
    }

    // Now at the end of the function, update the response to include timing information:
    let mdx_crawler_elapsed = mdx_crawler_start_time.elapsed();
    let overall_elapsed = overall_start_time.elapsed();
    
    info!("[MDX CRAWLER TIMING] ⏱️ MDX crawler took {:.2}s for {} URLs at depth {}", 
          mdx_crawler_elapsed.as_secs_f64(), successful_urls.len(), crawl_depth);
    state.logs.lock().await.push(format!("[MDX CRAWLER TIMING] ⏱️ MDX crawler took {:.2}s for {} URLs at depth {}", 
                                        mdx_crawler_elapsed.as_secs_f64(), successful_urls.len(), crawl_depth));
    
    info!("[TOTAL TIMING] ⏱️ Total processing time: {:.2}s", overall_elapsed.as_secs_f64());
    state.logs.lock().await.push(format!("[TOTAL TIMING] ⏱️ Total processing time: {:.2}s", overall_elapsed.as_secs_f64()));
    
    let final_logs = state.logs.lock().await.clone();
    
    let timings = json!({
        "firecrawl_api_seconds": firecrawl_elapsed.as_secs_f64(),
        "mdx_crawler_seconds": mdx_crawler_elapsed.as_secs_f64(),
        "total_seconds": overall_elapsed.as_secs_f64(),
        "urls_per_second": successful_urls.len() as f64 / mdx_crawler_elapsed.as_secs_f64(),
        "crawl_depth": crawl_depth,
        "firecrawl_depth": firecrawl_depth
    });
    
    let response = json!({
        "message": format!("🎉 Processed {} URLs out of {} in {:.2} seconds", 
                          successful_urls.len(), urls.len(), overall_elapsed.as_secs_f64()),
        "processed_urls": successful_urls,
        "original_urls": urls,
        "timings": timings,
        "logs": final_logs
    });

    info!("[RESPONSE] {}", response.to_string());
    HttpResponse::Ok().json(response)
}

pub async fn background_crawler(
    client: Client,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<(String, usize, usize, String)>,
    mut rx: mpsc::Receiver<(String, usize, usize, String)>,
    logs: Arc<tokio::sync::Mutex<Vec<String>>>,
) {
    info!("🚀 Starting background crawler task");
    let mut visited = HashSet::<String>::new();
    while let Some((url, depth, max_depth, domain)) = rx.recv().await {
        if visited.contains(&url) {
            info!("⏭️ Skipping already visited URL: {}", url);
            continue;
        }
        info!("🔄 Processing URL in background: {} (depth: {}/{})", url, depth, max_depth);
        visited.insert(url.clone());
        
        let success = process_url(
            &client,
            &semaphore,
            &url,
            depth,
            max_depth,
            &domain,
            &tx,
            &logs
        ).await;

        if success {
            info!("✅ Successfully processed: {}", url);
            logs.lock().await.push(format!("✅ Successfully processed: {}", url));
        } else {
            info!("❌ Failed to process: {}", url);
            logs.lock().await.push(format!("❌ Failed to process: {}", url));
        }
    }
}

/// Handles the crawl request, initiating the crawling process.
async fn start_crawl(_req: web::Json<CrawlRequest>, _state: web::Data<CrawlerState>) -> impl Responder {
    // Implementation remains the same
    HttpResponse::Ok().json(json!({
        "message": "Crawl endpoint has been moved to supercrawler API",
        "logs": ["Please use the /supercrawler endpoint instead"]
    }))
}

/// Serves an MDX file based on domain and path.
async fn get_mdx(path: web::Path<(String, String)>, _state: web::Data<CrawlerState>) -> impl Responder {
    let (domain, path) = path.into_inner();
    let path_binding = path.replace('/', "_").trim_start_matches('_').to_string();
    let filename = format!("./output/{}/{}.mdx", domain, path_binding);
    match fs::read_to_string(&filename) {
        Ok(content) => HttpResponse::Ok().body(content),
        Err(_) => HttpResponse::NotFound().body("MDX not found"),
    }
}

/// Main entry point, sets up the server and background crawler task.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new().filter_level(log::LevelFilter::Info).init();
    let host = "127.0.0.1";
    let port = 8080;

    info!("🚀 Initializing both Crawler and Super Crawler APIs with 2025 optimizations");
    let max_concurrency = 1000;
    let client = Client::builder()
        .pool_max_idle_per_host(1000)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    
    // Create separate channels for each crawler
    let (regular_tx, mut regular_rx) = mpsc::channel::<(String, usize, usize, String)>(1_000_000);
    let (super_tx, super_rx) = mpsc::channel::<(String, usize, usize, String)>(1_000_000);
    let logs = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Spawn regular crawler background task
    let regular_client = client.clone();
    let regular_semaphore = semaphore.clone();
    let regular_logs = logs.clone();
    let regular_tx_clone = regular_tx.clone();  // Clone before moving into async block
    tokio::spawn(async move {
        let mut visited = HashSet::<String>::new();
        while let Some((url, depth, max_depth, domain)) = regular_rx.recv().await {
            if visited.contains(&url) {
                continue;
            }
            visited.insert(url.clone());
            
            let success = process_url(
                &regular_client,
                &regular_semaphore,
                &url,
                depth,
                max_depth,
                &domain,
                &regular_tx_clone,  // Use cloned sender
                &regular_logs
            ).await;

            if success {
                regular_logs.lock().await.push(format!("✅ Successfully processed: {}", url));
            }
        }
    });

    // Spawn super crawler background task
    let super_client = client.clone();
    let super_semaphore = semaphore.clone();
    let super_logs = logs.clone();
    tokio::spawn(background_crawler(
        super_client,
        super_semaphore,
        super_tx.clone(),
        super_rx,
        super_logs
    ));

    // Set up states for both crawlers
    let regular_state = web::Data::new(CrawlerState {
        client: client.clone(),
        semaphore: semaphore.clone(),
        tx: regular_tx,
        logs: logs.clone(),
    });

    let super_state = web::Data::new(AppState {
        client,
        semaphore,
        tx: super_tx,
        logs,
    });

    info!("🌐 Starting server at http://{}:{}", host, port);
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