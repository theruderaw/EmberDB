use emberdb::codec::{CodecError, ErrorCode, Response, decode_request, encode_response};
use emberdb::command::execute;
use emberdb::config::{AppConfig, ServerConfig};
use emberdb::connection::ConnectionLimiter;
use emberdb::persistence::{load_snapshot, save_snapshot};
use emberdb::protocol::{read_frame, write_frame};
use emberdb::store::Store;
use emberdb::wal::lock::acquire_persistence_locks;

use std::io::{self, ErrorKind};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::{fs, thread};

fn send_response(stream: &mut TcpStream, response: &Response) -> io::Result<()> {
    let payload = encode_response(response).map_err(|error| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("response encoding failed: {error:?}"),
        )
    })?;

    write_frame(stream, &payload)
}

fn handle_client(
    mut stream: TcpStream,
    store: &Store,
    config: &ServerConfig,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    // Keep client sockets in blocking mode even though the listener
    // is nonblocking.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(config.read_timeout))?;
    stream.set_write_timeout(Some(config.write_timeout))?;

    let address = stream.peer_addr()?;
    println!("Client connected: {address}");

    loop {
        if shutdown.load(Ordering::SeqCst) {
            println!("Client {address} closing for shutdown");
            break;
        }
        let payload = match read_frame(&mut stream) {
            Ok(Some(payload)) => payload,

            Ok(None) => break,

            // Current framing implementation uses InvalidData
            // for an oversized incoming frame.
            Err(error) if error.kind() == ErrorKind::InvalidData => {
                send_response(&mut stream, &Response::Error(ErrorCode::PayloadTooLarge))?;
                break;
            }

            Err(error) if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) => {
                println!("Client {address} timed out");
                break;
            }

            Err(error) => return Err(error),
        };

        let response = match decode_request(&payload) {
            Ok(request) => execute(store, request),

            Err(CodecError::Malformed) => Response::Error(ErrorCode::BadRequest),

            Err(CodecError::TooLarge) => Response::Error(ErrorCode::PayloadTooLarge),

            Err(CodecError::UnknownOpcode) => Response::Error(ErrorCode::NotImplemented),

            Err(CodecError::UnsupportedVersion) => Response::Error(ErrorCode::VersionNotSupported),
        };

        send_response(&mut stream, &response)?;
    }

    println!("Client disconnected: {address}");

    Ok(())
}

fn main() -> io::Result<()> {
    // Configuration and shared server resources.
    let app_config = AppConfig::load()?;
    let _persistence_locks = acquire_persistence_locks(
        &app_config.persistence.snapshot_path,
        &app_config.persistence.wal.path,
    );
    println!("Exclusive persistence locks acquired");
    let snapshot_path = app_config.persistence.snapshot_path.clone();
    let store = Arc::new(Store::new());

    match load_snapshot(&snapshot_path)? {
        Some(snapshot) => {
            store.restore(&snapshot).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot restoration failed {error:?}"),
                )
            })?;

            println!("EmberDB snapshot restored successfully");
        }

        None => {
            println!("No existing snapshot; starting with empty database");
        }
    }

    let config = Arc::new(app_config.server);

    let limiter = Arc::new(ConnectionLimiter::new(config.max_clients));

    // Bind the TCP listener.
    let listener = TcpListener::bind(&config.bind_addr)?;
    listener.set_nonblocking(true)?;

    println!("EmberDB listening on {}", config.bind_addr);

    // Shared shutdown signal.
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&shutdown);

    ctrlc::set_handler(move || {
        signal.store(true, Ordering::SeqCst);
    })
    .map_err(io::Error::other)?;

    // Accept incoming connections until shutdown is requested.
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _address)) => {
                // Do not admit another client after Ctrl+C.
                if shutdown.load(Ordering::SeqCst) {
                    drop(stream);
                    break;
                }

                // Enforce maximum concurrent clients.
                let Some(permit) = limiter.try_acquire() else {
                    eprintln!(
                        "Connection rejected: limit of {} reached",
                        config.max_clients
                    );

                    let mut stream = stream;

                    if let Err(error) = stream.set_nonblocking(false) {
                        eprintln!("Failed to configure rejected connection: {error}");
                        continue;
                    }

                    if let Err(error) = stream.set_write_timeout(Some(config.write_timeout)) {
                        eprintln!("Failed to set rejection timeout: {error}");
                        continue;
                    }

                    if let Err(error) =
                        send_response(&mut stream, &Response::Error(ErrorCode::ServiceUnavailable))
                    {
                        eprintln!("Failed to send rejection: {error}");
                    }

                    // Dropping the stream closes the rejected connection.
                    drop(stream);
                    continue;
                };

                // Clone shared state for the new worker.
                let store = Arc::clone(&store);
                let config = Arc::clone(&config);
                let worker_shutdown = Arc::clone(&shutdown);

                if let Err(error) = thread::Builder::new().spawn(move || {
                    // The permit remains alive while the worker runs.
                    // Dropping it releases the connection slot.
                    let _permit = permit;

                    if let Err(error) = handle_client(stream, &store, &config, &worker_shutdown) {
                        eprintln!("Client error: {error}");
                    }
                }) {
                    eprintln!("Failed to spawn client thread: {error}");
                }
            }

            // Nonblocking listener: no pending connections.
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }

            // Interrupted accept: retry.
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                continue;
            }

            Err(error) => return Err(error),
        }
    }

    println!("EmberDB stopped accepting connections");

    drop(listener);

    println!("Waiting for {} active connection(s)", limiter.active());

    let shutdown_started = Instant::now();

    while limiter.active() > 0 {
        if shutdown_started.elapsed() >= config.shutdown_timeout {
            eprintln!(
                "Shutdown deadline exceeded; {} connection(s) still active",
                limiter.active()
            );
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    if limiter.active() == 0 {
        println!("All client connections closed");

        let snapshot = store
            .snapshot()
            .map_err(|error| io::Error::other(format!("snapshot creation failed: {error:?}")))?;

        let parent = snapshot_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));

        fs::create_dir_all(parent)?;

        save_snapshot(&snapshot_path, &snapshot)?;

        println!(
            "Snapshot saved: {} entries to {}",
            snapshot.entries.len(),
            snapshot_path.display()
        )
    } else {
        eprintln!("Exiting with active connections; snapshot skipped to avoid concurrent writes")
    }
    println!("EmberDB shutdown complete");

    Ok(())
}
