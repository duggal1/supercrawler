// ...existing code...

// YouTube Data API Search Implementation
// This function performs a search query against the YouTube Data API v3
// It constructs the API URL with the given parameters and handles the response
pub async fn search_youtube(query: &str, limit: usize, api_key: &str) -> Result<Vec<Video>, String> {
    // Construct the YouTube API search URL with query parameters
    let url = format!(
        "https://www.googleapis.com/youtube/v3/search?part=snippet&q={}&maxResults={}&type=video&key={}",
        query, limit, api_key
    );
    info!("Searching YouTube with URL: {}", url);
    
    // Initialize HTTP client and send the request
    let client = reqwest::Client::new();
    let response = client.get(&url).send().await
        .map_err(|e| format!("Failed to send YouTube search request: {}", e))?;

    // Check for successful response status
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_else(|_| "Could not read error body".to_string());
        return Err(format!("YouTube API request failed with status {}: {}", status, text));
    }

    // Parse the response body into text
    let response_text = response.text().await
        .map_err(|e| format!("Failed to read YouTube search response text: {}", e))?;

    // Parse the JSON response
    let json: Value = serde_json::from_str(&response_text)
        .map_err(|e| format!("Failed to parse YouTube search response JSON: {}", e))?;

    // Extract the items array from the response
    let items = json.get("items").and_then(Value::as_array)
        .ok_or_else(|| {
            warn!("No 'items' array found in YouTube response: {:?}", json);
            "No items in response".to_string()
        })?;

    // Transform JSON items into Video structs
    // Filters out any items that don't have all required fields
    let videos: Vec<Video> = items.iter().filter_map(|item| {
        // Extract required fields from the JSON structure
        let video_id = item.get("id")?.get("videoId")?.as_str()?.to_string();
        let snippet = item.get("snippet")?;
        let title = snippet.get("title")?.as_str()?.to_string();
        let description = snippet.get("description")?.as_str()?.to_string();
        
        // Create Video struct with extracted data
        Some(Video {
            id: video_id,
            title,
            description,
            transcript: None,
        })
    }).collect();

    Ok(videos)
}

// ...existing code...