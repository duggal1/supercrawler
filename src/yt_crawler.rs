use actix_web::{web, App, HttpServer, Error as ActixError};
use actix_web_lab::sse;
use tokio::sync::mpsc;
use futures_util::stream::StreamExt;
use reqwest::{Client};
use serde::Deserialize;
use serde_json::Value;
use chrono::Utc;
use log::{info, error, warn};
use html_escape;
use regex::Regex;
use tokio_stream::wrappers::ReceiverStream;
use std::io::Error as IoError;
use futures_util::stream::Stream;
use std::sync::Arc;

// Struct to represent a YouTube video
#[derive(Debug, Clone)]
pub struct Video {
    pub id: String,
    pub title: String,
    pub description: String,
    pub transcript: Option<String>,
}

// Search YouTube using the Data API
pub async fn search_youtube(query: &str, limit: usize, api_key: &str) -> Result<Vec<Video>, String> {
    let url = format!(
        "https://www.googleapis.com/youtube/v3/search?part=snippet&q={}&maxResults={}&type=video&key={}",
        query, limit, api_key
    );
    info!("Searching YouTube with URL: {}", url);
    let client = reqwest::Client::new();
    let response = client.get(&url).send().await
        .map_err(|e| format!("Failed to send YouTube search request: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_else(|_| "Could not read error body".to_string());
        return Err(format!("YouTube API request failed with status {}: {}", status, text));
    }

    let response_text = response.text().await
        .map_err(|e| format!("Failed to read YouTube search response text: {}", e))?;

    let json: Value = serde_json::from_str(&response_text)
        .map_err(|e| format!("Failed to parse YouTube search response JSON: {}", e))?;

    let items = json.get("items").and_then(Value::as_array)
        .ok_or_else(|| {
            warn!("No 'items' array found in YouTube response: {:?}", json);
            "No items in response".to_string()
        })?;

    let videos: Vec<Video> = items.iter().filter_map(|item| {
        let video_id = item.get("id")?.get("videoId")?.as_str()?.to_string();
        let snippet = item.get("snippet")?;
        let title = snippet.get("title")?.as_str()?.to_string();
        let description = snippet.get("description")?.as_str()?.to_string();
        Some(Video {
            id: video_id,
            title,
            description,
            transcript: None,
        })
    }).collect();

    Ok(videos)
}

// **REVERTED** Fetch transcript using the timedtext API
// pub async fn fetch_transcript(video_id: &str) -> Option<String> {
//     let url = format!("https://www.youtube.com/api/timedtext?lang=en&v={}", video_id);
//     info!("Fetching transcript using timedtext API for video {}: {}", video_id, url);
//     let client = reqwest::Client::new();

//     let response = match client.get(&url).send().await {
//         Ok(resp) => resp,
//         Err(e) => {
//             error!("Failed request to fetch timedtext transcript for video {}: {}", video_id, e);
//             return None;
//         }
//     };

//     if !response.status().is_success() {
//         // Log status code for non-success
//         let status = response.status();
//          warn!(
//             "Timedtext API request failed for video {}: Status {}",
//             video_id, status
//         );
//         // Optionally try to read body for more info, but often not useful for timedtext failures
//          // let err_text = response.text().await.unwrap_or_else(|_| "Could not read error body".to_string());
//          // warn!(" -> Body: {}", err_text);
//         return None;
//     }

//     let text = match response.text().await {
//         Ok(text) => text,
//         Err(e) => {
//             error!("Failed to read timedtext response text for video {}: {}", video_id, e);
//             return None;
//         }
//     };

//     // Check if the response looks like valid XML captions
//     if text.trim().is_empty() || !text.contains("<text") {
//         info!("Timedtext transcript empty or invalid XML for video {}", video_id);
//         // Log less severely here, as this is common for videos without timedtext captions
//         // warn!("Transcript empty or invalid for video {}: {}", video_id, text);
//         None
//     } else {
//         info!("Successfully fetched timedtext transcript for video {}", video_id);
//         Some(text) // Return the raw XML-like text
//     }
// }

// Generate Markdown from videos, cleaning the transcript
pub fn generate_markdown(videos: &[Video]) -> String {
    let mut markdown = String::new();
    markdown.push_str("# YouTube Search Results\n\n");

    if videos.is_empty() {
        markdown.push_str("No videos found or processed.\n");
        return markdown;
    }

    // let re_tags = Regex::new(r"<[^>]+>").unwrap(); // Commented out as transcript is disabled
    // let re_escapes = Regex::new(r"&#?\w+;").unwrap(); // Commented out as transcript is disabled

    for (i, video) in videos.iter().enumerate() {
        markdown.push_str("---\n\n");
        markdown.push_str(&format!("## {}. {}\n\n", i + 1, video.title));
        markdown.push_str(&format!(
            "**Video URL:** https://www.youtube.com/watch?v={}\n\n",
            video.id
        ));
        markdown.push_str("**Description:**\n");
        markdown.push_str(&format!("{}\n\n", video.description.trim()));

        // markdown.push_str("**Transcript:**\n\n"); // Commented out as transcript is disabled
        // if let Some(transcript) = &video.transcript {
        //     let cleaned = re_tags.replace_all(transcript, "");
        //     let unescaped = re_escapes.replace_all(&cleaned, |caps: &regex::Captures| {
        //         html_escape::decode_html_entities(&caps[0]).into_owned()
        //     });
        //     let final_text = unescaped.trim();

        //     if final_text.is_empty() {
        //         markdown.push_str("_Transcript found but empty after cleaning._\n\n");
        //     } else {
        //         markdown.push_str("```\n");
        //         markdown.push_str(final_text);
        //         markdown.push_str("\n```\n\n");
        //     }
        // } else {
        //     markdown.push_str("_Transcript fetching disabled or failed._\n\n"); // Updated message
        // }
    }
    markdown
}

// Struct for query parameters
#[derive(Deserialize, Debug)]
pub struct ScrapeParams {
    pub query: String,
    pub limit: Option<usize>,
}

// Scrape YouTube and stream results via SSE
pub async fn scrape_youtube_sse(
    params: ScrapeParams,
    api_key: Arc<String>,
    tx: mpsc::Sender<sse::Event>
) {
    let query = params.query;
    // Removed .min(25) limit
    let limit = params.limit.unwrap_or(5);

    let log_and_send = |msg: String| {
        let tx_clone = tx.clone();
        async move {
            info!("{}", msg);
            if let Err(e) = tx_clone.send(sse::Event::Data(sse::Data::new(msg))).await {
                error!("Failed to send SSE event: {}", e);
            }
        }
    };

    log_and_send(format!(
        "[{}] Starting YouTube scrape for query: '{}', limit: {}",
        Utc::now().to_rfc3339(), query, limit
    )).await;

    let videos = match search_youtube(&query, limit, &api_key).await { // Renamed 'mut videos' to 'videos' as it's not mutated later
        Ok(v) => v,
        Err(e) => {
            let error_msg = format!("Error searching YouTube: {}", e);
            log_and_send(error_msg.clone()).await;
            let _ = tx.send(sse::Event::Data(sse::Data::new(format!("ERROR: {}", e)).event("error"))).await;
            return;
        }
    };

    log_and_send(format!(
        "[{}] Found {} potential videos", Utc::now().to_rfc3339(), videos.len()
    )).await;

    if videos.is_empty() {
        log_and_send("No videos found matching the query.".to_string()).await;
        let markdown = generate_markdown(&videos);
        log_and_send("--- MARKDOWN BELOW ---".to_string()).await;
        let _ = tx.send(sse::Event::Data(sse::Data::new(markdown))).await;
        return;
    }

    // --- Start: Commented out transcript fetching ---
    // let mut tasks = Vec::new();
    // for video in &videos { // Use immutable reference as we don't modify videos here anymore
    //     let video_id = video.id.clone();
    //     // Note: We are cloning videos vector here to move ownership into the async block
    //     // Consider using Arc<Video> if performance becomes an issue with large number of videos
    //     tasks.push(tokio::spawn(async move {
    //         let transcript = fetch_transcript(&video_id).await;
    //         (video_id, transcript)
    //     }));
    // }

    // let mut videos_with_transcripts = videos; // Re-assign to a mutable variable if needed, or pass immutable ref if not modified

    // for result in futures::future::join_all(tasks).await {
    //     match result {
    //         Ok((video_id, transcript_option)) => {
    //             if let Some(video) = videos_with_transcripts.iter_mut().find(|v| v.id == video_id) {
    //                 video.transcript = transcript_option;
    //             } else {
    //                 warn!("Task completed for unknown video ID: {}", video_id);
    //             }
    //         }
    //         Err(e) => {
    //             let error_msg = format!("Transcript fetching task failed to join: {}", e);
    //             error!("{}", error_msg);
    //             let _ = tx.send(sse::Event::Data(sse::Data::new(error_msg).event("task_error"))).await;
    //         }
    //     }
    // }
    // --- End: Commented out transcript fetching ---


    log_and_send(format!("[{}] Generating final Markdown", Utc::now().to_rfc3339())).await;
    // Pass the original videos vec which now doesn't have transcripts
    let markdown = generate_markdown(&videos);

    log_and_send("--- MARKDOWN BELOW ---".to_string()).await;

    const CHUNK_SIZE: usize = 8000;
    for chunk in markdown.chars().collect::<Vec<char>>().chunks(CHUNK_SIZE) {
        let chunk_str: String = chunk.iter().collect();
        if let Err(e) = tx.send(sse::Event::Data(sse::Data::new(chunk_str))).await {
            error!("Failed to send Markdown chunk: {}", e);
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    let _ = tx.send(sse::Event::Data(sse::Data::new("END_OF_STREAM").event("end"))).await;

    info!("Finished YouTube scrape SSE process for query: '{}'", query);
}

// SSE handler
async fn scrape_handler(
    params: web::Query<ScrapeParams>,
    api_key: web::Data<String>,
) -> Result<sse::Sse<impl Stream<Item = Result<sse::Event, IoError>>>, ActixError> {
    let (tx, rx) = mpsc::channel::<sse::Event>(100);
    let params = params.into_inner();
    let api_key = Arc::clone(&api_key);

    tokio::spawn(scrape_youtube_sse(params, api_key, tx));

    let stream = ReceiverStream::new(rx).map(Ok::<_, IoError>);
    Ok(sse::Sse::from_stream(stream).with_keep_alive(std::time::Duration::from_secs(15)))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let api_key = std::env::var("YOUTUBE_API_KEY").expect("YOUTUBE_API_KEY not set");
    let api_key = web::Data::new(api_key);

    HttpServer::new(move || {
        App::new()
            .app_data(api_key.clone())
            .route("/yt-crawler", web::get().to(scrape_handler))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
