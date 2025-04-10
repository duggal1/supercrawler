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
use log::{info, error};
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

/// Processes a single URL, fetching content and saving it as MDX.
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
    info!("Fetching URL: {} (depth: {}/{})", url, current_depth, max_depth);

    let resp = match client.get(url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            error!("Failed to fetch URL {}: {}", url, e);
            logs.lock().await.push(format!("Failed to fetch URL {}: {}", url, e));
            drop(permit);
            return false;
        }
    };

    let content_type = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");
    let content = if content_type.contains("application/pdf") {
        fetch_pdf(client, url).await
    } else if content_type.contains("text/html") {
        fetch_html(client, url).await
    } else {
        None
    };

    let mut success = false;
    if let Some(content) = content {
        let mdx = clean_to_mdx(&content);
        save_mdx(url, &mdx);
        info!("Successfully processed URL: {}", url);
        success = true;
    }

    drop(permit);
    if current_depth < max_depth {
        let urls = fetch_and_extract_urls(client, url).await;
        if urls.len() > 10 {
            logs.lock().await.push(format!("Found {} URLs at {} (depth: {}/{})", urls.len(), url, current_depth, max_depth));
        }
        
        for next_url in urls {
            if let Ok(parsed) = Url::parse(&next_url) {
                if parsed.domain() == Url::parse(domain).unwrap().domain() {
                    let _ = tx.send((next_url.clone(), current_depth + 1, max_depth, domain.to_string())).await;
                }
            }
        }
    }
    
    success
}

/// Handles the crawl request, initiating the crawling process.
async fn start_crawl(req: web::Json<CrawlRequest>, state: web::Data<CrawlerState>) -> impl Responder {
    let domains = req.domains.clone()
        .into_iter()
        .filter(|d| Url::parse(d).is_ok())
        .collect::<Vec<_>>();
    let max_depth = req.max_depth.unwrap_or(5).min(5);
    info!("Received crawl request for {} domains with max_depth: {}", domains.len(), max_depth);
    state.logs.lock().await.push(format!("Received crawl request for {} domains with max_depth: {}", domains.len(), max_depth));

    let start_time = Instant::now();
    let mut mdx_files = Vec::new();
    let logs = state.logs.clone();
    
    // Tracking structure for processing domains
    let (complete_tx, mut complete_rx) = mpsc::channel::<(String, bool)>(domains.len() * 100);
    let (progress_tx, mut progress_rx) = mpsc::channel::<(usize, usize, usize)>(1000);
    
    // Create a task to process each domain
    let mut total_spawned = 0;
    
    for domain in domains.iter() {
        let tx = state.tx.clone();
        let domain_clone = domain.clone();
        let complete_tx = complete_tx.clone();
        let client = state.client.clone();
        let semaphore = state.semaphore.clone();
        let logs_clone = logs.clone();
        let _progress_tx = progress_tx.clone();
        
        total_spawned += 1;
        tokio::spawn(async move {
            // Queue the initial URL
            if let Err(e) = tx.send((domain_clone.clone(), 0, max_depth, domain_clone.clone())).await {
                error!("Failed to send initial URL {}: {}", domain_clone, e);
                let _ = complete_tx.send((domain_clone, false)).await;
                return;
            }
            
            // Wait to ensure processing starts
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Check if the URL was successfully processed
            let success = process_url(
                &client, 
                &semaphore, 
                &domain_clone, 
                0, 
                max_depth, 
                &domain_clone, 
                &tx, 
                &logs_clone
            ).await;
            
            // Signal completion
            let _ = complete_tx.send((domain_clone, success)).await;
        });
    }

    // Process URLs in parallel with optimized concurrency
    let max_depth = req.max_depth.unwrap_or(5).min(5);
    let mut tasks = Vec::new();
    let processed_urls = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

    // Create a vector of URLs to process to avoid lifetime issues
    let urls_to_process: Vec<String> = domains.iter().cloned().collect();
    
    for url in urls_to_process {
        let client = state.client.clone();
        let semaphore = state.semaphore.clone();
        let tx = state.tx.clone();
        let logs = state.logs.clone();
        let processed_urls = processed_urls.clone();
        let url_clone = url.clone();

        tasks.push(tokio::spawn(async move {
            if processed_urls.lock().await.contains(&url) {
                return None;
            }
            processed_urls.lock().await.insert(url.clone());

            process_url(
                &client,
                &semaphore,
                &url,
                0,
                max_depth,
                &url_clone,
                &tx,
                &logs
            ).await.then(|| url)
        }));
    }
    
    // Real-time status monitoring task
    let logs_for_monitor = logs.clone();
    tokio::spawn(async move {
        let mut current_depth_counts = vec![0; max_depth as usize + 1];
        let mut current_depth_processed = vec![0; max_depth as usize + 1];
        
        while let Some((depth, total, processed)) = progress_rx.recv().await {
            if depth <= max_depth as usize {
                current_depth_counts[depth] = total;
                current_depth_processed[depth] = processed;
                
                let progress_msg = format!(
                    "Progress: [Depth 0: {}/{}, Depth 1: {}/{}, Depth 2: {}/{}, Depth 3: {}/{}, Depth 4: {}/{}, Depth 5: {}/{}] - {:?}s elapsed",
                    current_depth_processed[0], current_depth_counts[0],
                    current_depth_processed.get(1).unwrap_or(&0), current_depth_counts.get(1).unwrap_or(&0),
                    current_depth_processed.get(2).unwrap_or(&0), current_depth_counts.get(2).unwrap_or(&0),
                    current_depth_processed.get(3).unwrap_or(&0), current_depth_counts.get(3).unwrap_or(&0),
                    current_depth_processed.get(4).unwrap_or(&0), current_depth_counts.get(4).unwrap_or(&0),
                    current_depth_processed.get(5).unwrap_or(&0), current_depth_counts.get(5).unwrap_or(&0),
                    start_time.elapsed().as_secs()
                );
                info!("{}", progress_msg);
                logs_for_monitor.lock().await.push(progress_msg);
            }
        }
    });
    
    // Drop sender so channel closes when all tasks complete
    drop(complete_tx);
    drop(progress_tx);
    
    // Process domain completion reports
    let mut processed_count = 0;
    let mut successful_count = 0;
    
    while let Some((domain, success)) = complete_rx.recv().await {
        processed_count += 1;
        if success {
            successful_count += 1;
            
            // Check for MDX file
            let filename = url_to_filename(&domain);
            if let Ok(content) = fs::read_to_string(&filename) {
                mdx_files.push((domain.clone(), content));
            }
        }
        
        // Log progress
        let elapsed = start_time.elapsed();
        let log_msg = format!(
            "Progress: {}/{} domains processed ({} successful) in {:.2} seconds", 
            processed_count, 
            total_spawned,
            successful_count,
            elapsed.as_secs_f64()
        );
        info!("{}", log_msg);
        logs.lock().await.push(log_msg);
        
        // If all domains are processed, we're done
        if processed_count >= total_spawned {
            break;
        }
    }

    let elapsed = start_time.elapsed();
    let final_message = format!(
        "Completed crawling {} domains ({} successful) in {:.2} seconds", 
        total_spawned, 
        successful_count,
        elapsed.as_secs_f64()
    );
    info!("{}", final_message);
    logs.lock().await.push(final_message.clone());

    let response_logs = logs.lock().await.clone();
    HttpResponse::Ok().json(CrawlResponse {
        message: final_message,
        logs: response_logs,
        mdx_files,
    })
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
    // Load .env file if it exists
    dotenv().ok();
    
    env_logger::Builder::new().filter_level(log::LevelFilter::Info).init();
    let host = "127.0.0.1";
    let port = 8080;

    info!("Initializing both Crawler and Super Crawler APIs with 2025 optimizations");
    
    // Log environment variables (with masking for secrets)
    if let Ok(api_key) = std::env::var("FIRECRAWL_API_KEY") {
        if !api_key.is_empty() {
            let masked_key = format!("{}...{}", &api_key[0..5], &api_key[api_key.len()-5..]);
            info!("Found FIRECRAWL_API_KEY in environment: {}", masked_key);
        } else {
            info!("FIRECRAWL_API_KEY is set but empty");
        }
    } else {
        info!("FIRECRAWL_API_KEY not found in environment");
    }

    let max_concurrency = 1000;
    let client = Client::builder()
        .pool_max_idle_per_host(1000)
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(15))
        .pool_idle_timeout(Duration::from_secs(90))
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
                regular_logs.lock().await.push(format!("Successfully processed: {}", url));
            }
        }
    });

    // Spawn super crawler background task
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

    // Set up states for both crawlers
    let regular_state = web::Data::new(CrawlerState {
        client: client.clone(),
        semaphore: semaphore.clone(),
        tx: regular_tx,
        logs: logs.clone(),
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