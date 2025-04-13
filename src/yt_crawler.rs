// Essential web framework imports for handling HTTP server and SSE
use actix_web::{web, App, HttpServer, Error as ActixError};
use actix_web_lab::sse;  // Server-Sent Events support

// Async runtime and stream processing
use tokio::sync::mpsc;  // Multi-producer, single-consumer channel for async communication
use futures_util::stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use futures_util::stream::Stream;

// HTTP client and data serialization
use reqwest::{Client};  // HTTP client for making requests to YouTube API
use serde::Deserialize;  // For deserializing JSON responses
use serde_json::Value;  // For flexible JSON handling

// Utility imports
use chrono::Utc;  // Timestamp handling
use log::{info, error, warn};  // Structured logging
use html_escape;  // HTML entity handling
use regex::Regex;  // Text pattern matching
use std::io::Error as IoError;
use std::sync::Arc;  // Thread-safe reference counting

// Core data structure representing a YouTube video
// This struct holds essential video information retrieved from the YouTube API
#[derive(Debug, Clone)]
pub struct Video {
    pub id: String,      // YouTube video ID
    pub title: String,   // Video title
    pub description: String,  // Video description
    pub transcript: Option<String>,  // Optional transcript (currently disabled)
}

// ...existing code...