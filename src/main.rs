use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use reqwest::Client;
use scraper::{Html, Selector};
use url::Url;
use pdf_extract::extract_text;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;
use log::{info, error};

#[derive(Serialize, Deserialize)]
struct CrawlRequest {
    domain: String,
    max_depth: Option<usize>,
}

#[derive(Serialize)]
struct CrawlResponse {
    message: String,
}

struct AppState {
    client: Client,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<(String, usize, usize, String)>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new().filter_level(log::LevelFilter::Info).init();
    let host = "127.0.0.1";
    let port = 8080;

    info!("Initializing crawler with max concurrency and large channel buffer");
    let max_concurrency = 10_000;
    let client = Client::builder().pool_max_idle_per_host(1_000).build().unwrap();
    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    let (tx, mut rx) = mpsc::channel::<(String, usize, usize, String)>(100_000);

    let tx_clone = tx.clone();
    let client_clone = client.clone();
    let semaphore_clone = semaphore.clone();

    tokio::spawn(async move {
        let mut visited = HashSet::new();
        info!("Background crawler task started");
        while let Some((url, current_depth, max_depth, domain)) = rx.recv().await {
            if visited.contains(&url) { continue; }
            visited.insert(url.clone());
            let client = client_clone.clone();
            let semaphore = semaphore_clone.clone();
            let tx = tx_clone.clone();
            tokio::spawn(async move {
                process_url(&client, &semaphore, &url, current_depth, max_depth, &domain, &tx).await;
            });
        }
    });

    let state = web::Data::new(AppState { client, semaphore, tx });
    info!("Starting server at http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/crawl", web::post().to(start_crawl))
            .route("/mdx/{domain}/{path:.*}", web::get().to(get_mdx))
    })
    .bind((host, port))?
    .run()
    .await
}

async fn start_crawl(req: web::Json<CrawlRequest>, state: web::Data<AppState>) -> impl Responder {
    let domain = req.domain.clone();
    let max_depth = req.max_depth.unwrap_or(usize::MAX);
    info!("Received crawl request for domain: {} with max_depth: {}", domain, max_depth);
    if let Err(e) = state.tx.send((domain.clone(), 0, max_depth, domain.clone())).await {
        error!("Failed to send initial URL: {}", e);
        return HttpResponse::InternalServerError().body("Failed to start crawl");
    }
    HttpResponse::Ok().json(CrawlResponse {
        message: format!("Started crawling {}", domain),
    })
}

async fn process_url(
    client: &Client,
    semaphore: &Arc<Semaphore>,
    url: &str,
    current_depth: usize,
    max_depth: usize,
    domain: &str,
    tx: &mpsc::Sender<(String, usize, usize, String)>,
) {
    let permit = semaphore.acquire().await.unwrap();
    let resp = match client.get(url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            error!("Failed to fetch URL {}: {}", url, e);
            drop(permit);
            return;
        }
    };

    let content_type = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("unknown");
    let content = if content_type.contains("application/pdf") {
        fetch_pdf(client, url).await
    } else if content_type.contains("text/html") {
        fetch_html(client, url).await
    } else {
        None
    };

    if let Some(content) = content {
        let mdx = clean_to_mdx(&content);
        save_mdx(url, &mdx);
        info!("Successfully processed and saved MDX for URL: {}", url);
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
}

async fn fetch_and_extract_urls(client: &Client, url: &str) -> Vec<String> {
    let mut urls = Vec::new();
    if url.ends_with(".pdf") { return urls; }
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

fn clean_to_mdx(content: &str) -> String {
    let re_noise = Regex::new(r"(?i)(advertisement|footer|sidebar|nav|script|style)").unwrap();
    let clean = re_noise.replace_all(content, "").to_string();
    let doc = Html::parse_document(&clean);

    let mut mdx = String::new();
    // Metadata (example, could be expanded with real meta tags)
    mdx.push_str("---\n");
    mdx.push_str("title: Page Content\n");
    mdx.push_str("description: Extracted content from webpage\n");
    mdx.push_str("keywords: web, crawl, content\n");
    mdx.push_str("---\n\n");

    // Headings
    for level in 1..=6 {
        let selector = Selector::parse(&format!("h{}", level)).unwrap();
        for element in doc.select(&selector) {
            let text = element.text().collect::<Vec<_>>().join(" ").trim().to_string();
            if !text.is_empty() {
                mdx.push_str(&format!("{} {}\n\n", "#".repeat(level as usize), text));
            }
        }
    }

    // Paragraphs
    let p_selector = Selector::parse("p").unwrap();
    for element in doc.select(&p_selector) {
        let text = element.text().collect::<Vec<_>>().join(" ").trim().to_string();
        if !text.is_empty() {
            mdx.push_str(&format!("{}\n\n", text));
        }
    }

    // Lists
    let list_selector = Selector::parse("ul, ol").unwrap();
    for element in doc.select(&list_selector) {
        let items = element.select(&Selector::parse("li").unwrap())
            .map(|li| li.text().collect::<Vec<_>>().join(" ").trim().to_string())
            .collect::<Vec<_>>();
        if !items.is_empty() {
            let ordered = element.value().name() == "ol";
            for (i, item) in items.iter().enumerate() {
                if ordered {
                    mdx.push_str(&format!("{}. {}\n", i + 1, item));
                } else {
                    mdx.push_str(&format!("- {}\n", item));
                }
            }
            mdx.push_str("\n");
        }
    }

    // Blockquotes
    let blockquote_selector = Selector::parse("blockquote").unwrap();
    for element in doc.select(&blockquote_selector) {
        let text = element.text().collect::<Vec<_>>().join(" ").trim().to_string();
        if !text.is_empty() {
            mdx.push_str(&format!("> {}\n\n", text));
        }
    }

    // Code Blocks
    let code_selector = Selector::parse("pre > code").unwrap();
    for element in doc.select(&code_selector) {
        let text = element.text().collect::<Vec<_>>().join("\n").trim().to_string();
        if !text.is_empty() {
            mdx.push_str(&format!("```plaintext\n{}\n```\n\n", text));
        }
    }

    mdx.trim().to_string()
}

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

async fn get_mdx(path: web::Path<(String, String)>, _state: web::Data<AppState>) -> impl Responder {
    let (domain, path) = path.into_inner();
    let path_binding = path.replace('/', "_").trim_start_matches('_').to_string();
    let filename = format!("./output/{}/{}.mdx", domain, path_binding);
    match fs::read_to_string(&filename) {
        Ok(content) => HttpResponse::Ok().body(content),
        Err(_) => HttpResponse::NotFound().body("MDX not found"),
    }
}