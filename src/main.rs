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
use futures::future::join_all;
use std::time::{Duration, Instant};
use actix_cors::Cors;
use anyhow;

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

struct AppState {
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
                let code_text = code.text().collect::<Vec<_>>().join("\n").trim().to_string();
                if code_text.is_empty() { String::new() } else { format!("```\n{}\n```\n\n", code_text) }
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
        "nav" | "footer" | "aside" | "script" | "style" => {
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
    mdx.push_str(&format!(
        "---\ntitle: {}\ndescription: {}\nkeywords: {}\n---\n\n",
        title, description, keywords
    ));

    // Process body content
    if let Some(body) = doc.select(&Selector::parse("body").unwrap()).next() {
        for child in body.children() {
            if let Some(element) = ElementRef::wrap(child) {
                mdx.push_str(&process_element(element));
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
    info!("Fetching URL: {}", url);
    logs.lock().await.push(format!("Fetching URL: {}", url));

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
        info!("Successfully processed and saved MDX for URL: {}", url);
        logs.lock().await.push(format!("Successfully processed and saved MDX for URL: {}", url));
        success = true;
    }

    drop(permit);
    if current_depth < max_depth {
        let urls = fetch_and_extract_urls(client, url).await;
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
async fn start_crawl(req: web::Json<CrawlRequest>, state: web::Data<AppState>) -> impl Responder {
    let domains = req.domains.clone()
        .into_iter()
        .filter(|d| Url::parse(d).is_ok())
        .collect::<Vec<_>>();
    let max_depth = req.max_depth.unwrap_or(5).min(5);
    info!("Received crawl request for {} domains with max_depth: {}", domains.len(), max_depth);

    let start_time = Instant::now();
    let batch_size = 10;
    let mut mdx_files = Vec::new();
    let logs = state.logs.clone();
    
    // Tracking structure for processing domains
    let (complete_tx, mut complete_rx) = mpsc::channel::<(String, bool)>(domains.len() * 10);
    
    // Create a task to process each domain
    let mut total_spawned = 0;
    
    for domain in domains.iter() {
        let tx = state.tx.clone();
        let domain_clone = domain.clone();
        let complete_tx = complete_tx.clone();
        let client = state.client.clone();
        let semaphore = state.semaphore.clone();
        let logs_clone = logs.clone();
        
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
    
    // Drop sender so channel closes when all tasks complete
    drop(complete_tx);
    
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
        domains.len(), 
        successful_count,
        elapsed.as_secs_f64()
    );
    info!("{}", final_message);

    let response_logs = logs.lock().await.clone();
    HttpResponse::Ok().json(CrawlResponse {
        message: final_message,
        logs: response_logs,
        mdx_files,
    })
}

/// Serves an MDX file based on domain and path.
async fn get_mdx(path: web::Path<(String, String)>, _state: web::Data<AppState>) -> impl Responder {
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

    info!("Initializing crawler with extreme concurrency and scalability");
    let max_concurrency = 1000;
    let client = Client::builder()
        .pool_max_idle_per_host(1000)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    let (tx, mut rx) = mpsc::channel::<(String, usize, usize, String)>(1_000_000);
    let logs = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let tx_clone = tx.clone();
    let client_clone = client.clone();
    let semaphore_clone = semaphore.clone();
    let logs_clone = logs.clone();

    tokio::spawn(async move {
        let mut visited = HashSet::with_capacity(1_000_000);
        let mut active_tasks = 0;
        let (task_tx, mut task_rx) = mpsc::channel::<()>(10_000);
        
        info!("Background crawler task started");
        
        loop {
            tokio::select! {
                // Process new URLs
                Some((url, current_depth, max_depth, domain)) = rx.recv() => {
                    if visited.contains(&url) {
                        continue;
                    }
                    visited.insert(url.clone());
                    
                    let client = client_clone.clone();
                    let semaphore = semaphore_clone.clone();
                    let tx = tx_clone.clone();
                    let logs = logs_clone.clone();
                    let task_tx = task_tx.clone();
                    
                    active_tasks += 1;
                    info!("Active crawler tasks: {}", active_tasks);
                    
                    tokio::spawn(async move {
                        let _ = process_url(&client, &semaphore, &url, current_depth, max_depth, &domain, &tx, &logs).await;
                        let _ = task_tx.send(()).await;
                    });
                }
                
                // Track completed tasks
                Some(_) = task_rx.recv() => {
                    active_tasks -= 1;
                    info!("Task completed. Active crawler tasks: {}", active_tasks);
                }
                
                // Exit if channel is closed and no active tasks
                else => {
                    if active_tasks == 0 {
                        info!("Background crawler task completed - all URLs processed");
                        break;
                    }
                }
            }
        }
    });

    let state = web::Data::new(AppState {
        client,
        semaphore,
        tx,
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
            .app_data(state.clone())
            .route("/crawl", web::post().to(start_crawl))
            .route("/mdx/{domain}/{path:.*}", web::get().to(get_mdx))
    })
    .bind((host, port))?
    .run()
    .await
}