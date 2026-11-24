#![allow(unused_crate_dependencies)]
#![cfg(unix)]

use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("child still owned").id()
    }

    fn wait_with_timeout(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let child = self.child.as_mut().expect("child still owned");
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().expect("poll child exit status") {
                self.child = None;
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.kill());
            drop(child.wait());
        }
    }
}

#[test]
fn sigterm_triggers_graceful_shutdown_with_zero_exit_code() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_firestore-emulator"))
        .args(["--host-port", "127.0.0.1:0"])
        .env("RUST_LOG", "OFF")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn firestore-emulator binary");

    let stderr = child.stderr.take().expect("piped stderr");
    let mut guard = ChildGuard::new(child);

    let (tx, rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            match line {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut stderr_lines = Vec::new();
    let startup_deadline = Instant::now() + Duration::from_secs(10);
    let mut saw_listening = false;
    while Instant::now() < startup_deadline {
        let remaining = startup_deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(line) => {
                let is_listening = line.contains("Firestore emulator listening on");
                stderr_lines.push(line);
                if is_listening {
                    saw_listening = true;
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => break,
        }
    }
    assert!(
        saw_listening,
        "emulator did not print listening banner within 10s; stderr:\n{}",
        stderr_lines.join("\n")
    );

    let pid = guard.id();
    let kill_status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("invoke kill -TERM");
    assert!(
        kill_status.success(),
        "kill -TERM {pid} failed with {kill_status}"
    );

    let shutdown_deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_eof = false;
    while Instant::now() < shutdown_deadline {
        let remaining = shutdown_deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(line) => stderr_lines.push(line),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                saw_eof = true;
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => break,
        }
    }
    assert!(
        saw_eof,
        "emulator did not close stderr within 5s after SIGTERM; stderr:\n{}",
        stderr_lines.join("\n")
    );

    let remaining = shutdown_deadline
        .saturating_duration_since(Instant::now())
        .max(Duration::from_millis(500));
    let status = guard.wait_with_timeout(remaining).unwrap_or_else(|| {
        panic!(
            "emulator did not exit within bounded time after SIGTERM; stderr:\n{}",
            stderr_lines.join("\n")
        )
    });
    drop(reader.join());

    assert_eq!(
        status.code(),
        Some(0),
        "expected clean exit code 0 after SIGTERM, got {status:?}; stderr:\n{}",
        stderr_lines.join("\n")
    );
    assert!(
        stderr_lines
            .iter()
            .any(|line| line.contains("Firestore emulator stopped")),
        "expected 'Firestore emulator stopped' in stderr; got:\n{}",
        stderr_lines.join("\n")
    );
}
