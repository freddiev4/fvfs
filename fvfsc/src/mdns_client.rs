/// mDNS discovery client for fvfsc.
///
/// Browses for `_fvfs._tcp.local.` and returns the address of the first
/// resolved daemon.  Re-browses automatically if the connection is lost.
use mdns_sd::{ServiceDaemon, ServiceEvent};
use std::time::Duration;
use tokio::time::timeout;
use tracing::{error, info, warn};

const SERVICE_TYPE: &str = "_fvfs._tcp.local.";
const DISCOVER_TIMEOUT: Duration = Duration::from_secs(10);

/// Attempt to discover a fvfsd instance on the local network.
/// Returns an HTTP base URL like `http://192.168.1.10:7734`.
pub async fn discover() -> Option<String> {
    let mdns = match ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to create mDNS daemon: {e}");
            return None;
        }
    };

    let receiver = match mdns.browse(SERVICE_TYPE) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to browse mDNS: {e}");
            return None;
        }
    };

    let result = timeout(DISCOVER_TIMEOUT, async {
        loop {
            match receiver.recv_async().await {
                Ok(ServiceEvent::ServiceResolved(info)) => {
                    let addresses = info.get_addresses();
                    if let Some(addr) = addresses.iter().next() {
                        let port = info.get_port();
                        let url = format!("http://{}:{}", addr, port);
                        info!(url = %url, "fvfsd discovered via mDNS");
                        return Some(url);
                    }
                }
                Ok(ServiceEvent::ServiceRemoved(_, name)) => {
                    warn!(name = %name, "fvfsd service removed");
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("mDNS receiver error: {e}");
                    return None;
                }
            }
        }
    })
    .await;

    match result {
        Ok(url) => url,
        Err(_) => {
            warn!("mDNS discovery timed out after {}s", DISCOVER_TIMEOUT.as_secs());
            None
        }
    }
}
