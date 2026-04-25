use crate::models::download::{DownloadOverrides, FormatOptions};
use crate::models::DownloadItem;
use crate::runners::template_context::TemplateContext;
use crate::runners::ytdlp_download::{run_ytdlp_download, YtdlpDownloadError};
use crate::scheduling::concurrency::DynamicSemaphore;
use crate::scheduling::dispatcher::{DispatchEntry, DispatchRequest, GenericDispatcher};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use tauri::AppHandle;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone)]
pub struct DownloadSender(pub UnboundedSender<DispatchRequest<DownloadRequest>>);

#[derive(Clone)]
pub enum DownloadRequest {
  Batch {
    group_id: String,
    items: Vec<DownloadItem>,
  },
}

#[derive(Clone)]
pub struct DownloadEntry {
  pub group_id: String,
  pub id: String,
  pub url: String,
  pub format: FormatOptions,
  pub overrides: Option<DownloadOverrides>,
  pub template_context: TemplateContext,
}

impl From<(DownloadItem, String)> for DownloadEntry {
  fn from(item: (DownloadItem, String)) -> Self {
    Self {
      group_id: item.1,
      id: item.0.id,
      url: item.0.url,
      format: item.0.format,
      overrides: item.0.overrides,
      template_context: item.0.template_context,
    }
  }
}

impl DispatchEntry for DownloadEntry {
  fn group_id(&self) -> &String {
    &self.group_id
  }
  fn group_key(&self) -> Option<&String> {
    self.template_context.values.get("playlist_id")
  }
  fn set_numbering(&mut self, autonumber: u64, group_autonumber: Option<u64>) {
    self
      .template_context
      .values
      .insert("autonumber".to_string(), autonumber.to_string());
    if let Some(group_autonumber) = group_autonumber {
      self.template_context.values.insert(
        "playlist_autonumber".to_string(),
        group_autonumber.to_string(),
      );

      let missing_playlist_index = self
        .template_context
        .values
        .get("playlist_index")
        .map_or(true, |value| value.trim().parse::<u64>().is_err());

      if missing_playlist_index {
        self.template_context.values.insert(
          "playlist_index".to_string(),
          group_autonumber.saturating_sub(1).to_string(),
        );
      }
    }
  }
}

static DOWNLOAD_COUNTERS: LazyLock<Mutex<HashMap<String, usize>>> =
  LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn setup_download_dispatcher(
  app: &AppHandle,
  sem: Arc<DynamicSemaphore>,
) -> GenericDispatcher<DownloadRequest> {
  GenericDispatcher::start(
    app.clone(),
    sem,
    |req: DownloadRequest| match req {
      DownloadRequest::Batch { group_id, items } => {
        let total = items.len();
        DOWNLOAD_COUNTERS
          .lock()
          .unwrap()
          .insert(group_id.clone(), total);
        items
          .into_iter()
          .map(|item| DownloadEntry::from((item, group_id.clone())))
          .collect()
      }
    },
    |tx, app: AppHandle, entry: DownloadEntry| async move {
      tracing::info!("starting download id={} url={}", entry.id, entry.url);

      if let Err(e) = run_ytdlp_download(app.clone(), entry.clone()).await {
        tracing::warn!(
          download_id = %entry.id,
          group_id = %entry.group_id,
          error = %e,
          "Failed to run ytdlp download",
        );
        if should_report_to_sentry(&e) {
          sentry::capture_error(&e);
        }
      }

      let mut counters = DOWNLOAD_COUNTERS.lock().unwrap();
      if let Some(cnt) = counters.get_mut(&entry.group_id) {
        *cnt -= 1;
        if *cnt == 0 {
          counters.remove(&entry.group_id);
          let _ = tx.send(DispatchRequest::Cleanup {
            group_id: entry.group_id.clone(),
          });
        }
      }
    },
  )
}

fn should_report_to_sentry(err: &YtdlpDownloadError) -> bool {
  matches!(
    err,
    YtdlpDownloadError::SpawnFailed(_)
      | YtdlpDownloadError::InvalidDiagnosticRules(_)
      | YtdlpDownloadError::EventStreamEnded
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::models::TrackType;

  fn make_entry(values: &[(&str, &str)]) -> DownloadEntry {
    DownloadEntry {
      group_id: "group".into(),
      id: "id".into(),
      url: "https://example.com/video".into(),
      format: FormatOptions {
        track_type: TrackType::Both,
        abr: None,
        height: None,
        fps: None,
        audio_encoding: None,
        video_encoding: None,
        audio_track: None,
        video_track: None,
      },
      overrides: None,
      template_context: TemplateContext {
        values: values
          .iter()
          .map(|(key, value)| (key.to_string(), value.to_string()))
          .collect(),
      },
    }
  }

  #[test]
  fn set_numbering_keeps_existing_playlist_index() {
    let mut entry = make_entry(&[("playlist_index", "7")]);

    entry.set_numbering(12, Some(3));

    assert_eq!(
      entry.template_context.values.get("playlist_index").unwrap(),
      "7"
    );
    assert_eq!(
      entry
        .template_context
        .values
        .get("playlist_autonumber")
        .unwrap(),
      "3"
    );
  }

  #[test]
  fn set_numbering_derives_missing_playlist_index_from_group_autonumber() {
    let mut entry = make_entry(&[]);

    entry.set_numbering(12, Some(3));

    assert_eq!(
      entry.template_context.values.get("playlist_index").unwrap(),
      "2"
    );
    assert_eq!(
      entry
        .template_context
        .render_template("%(playlist_index+1)02d-%(title)s"),
      "03-%(title)s"
    );
  }

  #[test]
  fn set_numbering_derives_invalid_playlist_index_from_group_autonumber() {
    let mut entry = make_entry(&[("playlist_index", "None")]);

    entry.set_numbering(12, Some(3));

    assert_eq!(
      entry.template_context.values.get("playlist_index").unwrap(),
      "2"
    );
  }
}
