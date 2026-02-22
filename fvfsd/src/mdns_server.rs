/// mDNS/DNS-SD registration for fvfsd.
///
/// Registers the service `_fvfs._tcp.local.` pointing to port 7734.
/// Client devices browse for this service to discover the daemon.
use mdns_sd::{ServiceDaemon, ServiceInfo};
use tracing::{error, info};

const SERVICE_TYPE: &str = "_fvfs._tcp.local.";

pub async fn register_mdns(port: u16) {
    let hostname = get_hostname();
    let instance_name = format!("fvfsd-{}", hostname);

    let mdns = match ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to start mDNS daemon: {e}");
            return;
        }
    };

    let service = match ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &format!("{}.local.", hostname),
        (),
        port,
        None,
    ) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create mDNS service info: {e}");
            return;
        }
    };

    match mdns.register(service) {
        Ok(()) => {
            info!(
                service_type = SERVICE_TYPE,
                instance = %instance_name,
                port = port,
                "mDNS service registered"
            );
        }
        Err(e) => {
            error!("Failed to register mDNS service: {e}");
            return;
        }
    }

    // Keep the daemon alive — mdns_sd uses a background thread internally.
    // We hold the ServiceDaemon here so it isn't dropped (which would
    // unregister the service).
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

fn get_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "fvfsd".to_string())
}
