//! Userspace TCP fault proxy (the kernel here has no netem). Sits between a client and a node:
//!   faultproxy LISTEN_PORT UPSTREAM_PORT CONF_FILE
//! CONF_FILE is re-read every 50 ms: "mode lat_ms jitter_ms stall_prob stall_ms"
//!   mode: normal | refuse (close new and existing connections) | blackhole (accept, swallow bytes,
//!         never answer: a silent network partition)
//!   every forwarded chunk is delayed by lat_ms + U(0, jitter_ms), and with probability stall_prob
//!   by an extra stall_ms (a stand-in for a retransmission after packet loss)
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Normal,
    Refuse,
    Blackhole,
}
struct Conf {
    mode: AtomicU64,
    lat_us: AtomicU64,
    jit_us: AtomicU64,
    stall_ppm: AtomicU64,
    stall_us: AtomicU64,
    epoch: AtomicU64, // bumped on refuse so live connections close
}
impl Conf {
    fn mode(&self) -> Mode {
        match self.mode.load(Relaxed) {
            1 => Mode::Refuse,
            2 => Mode::Blackhole,
            _ => Mode::Normal,
        }
    }
}
fn rnd(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn pump(mut from: TcpStream, mut to: TcpStream, conf: Arc<Conf>, epoch: u64, seed: u64) {
    let mut buf = vec![0u8; 64 * 1024];
    let mut r = seed | 1;
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if conf.epoch.load(Relaxed) != epoch {
            break;
        }
        match conf.mode() {
            Mode::Blackhole => continue, // swallow: the peer waits until its own timeout
            Mode::Refuse => break,
            Mode::Normal => {}
        }
        let mut d = conf.lat_us.load(Relaxed);
        let j = conf.jit_us.load(Relaxed);
        if j > 0 {
            d += rnd(&mut r) % j;
        }
        if conf.stall_ppm.load(Relaxed) > rnd(&mut r) % 1_000_000 {
            d += conf.stall_us.load(Relaxed);
        }
        if d > 0 {
            std::thread::sleep(Duration::from_micros(d));
        }
        if to.write_all(&buf[..n]).is_err() {
            break;
        }
    }
    let _ = to.shutdown(std::net::Shutdown::Both);
    let _ = from.shutdown(std::net::Shutdown::Both);
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (lp, up, cf) = (a[1].clone(), a[2].clone(), a[3].clone());
    let conf = Arc::new(Conf {
        mode: AtomicU64::new(0),
        lat_us: AtomicU64::new(0),
        jit_us: AtomicU64::new(0),
        stall_ppm: AtomicU64::new(0),
        stall_us: AtomicU64::new(0),
        epoch: AtomicU64::new(0),
    });
    {
        let conf = conf.clone();
        std::thread::spawn(move || {
            let mut last = String::new();
            loop {
                if let Ok(s) = std::fs::read_to_string(&cf) {
                    if s != last {
                        let f: Vec<&str> = s.split_whitespace().collect();
                        let num = |i: usize| f.get(i).and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
                        let m = match f.first().copied().unwrap_or("normal") {
                            "refuse" => 1,
                            "blackhole" => 2,
                            _ => 0,
                        };
                        if m == 1 {
                            conf.epoch.fetch_add(1, Relaxed);
                        }
                        conf.mode.store(m, Relaxed);
                        conf.lat_us.store((num(1) * 1000.0) as u64, Relaxed);
                        conf.jit_us.store((num(2) * 1000.0) as u64, Relaxed);
                        conf.stall_ppm.store((num(3) * 1e6) as u64, Relaxed);
                        conf.stall_us.store((num(4) * 1000.0) as u64, Relaxed);
                        last = s;
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        });
    }
    let l = TcpListener::bind(format!("127.0.0.1:{lp}")).unwrap();
    let mut seed = 0x9E3779B97F4A7C15u64;
    for c in l.incoming() {
        let Ok(c) = c else { continue };
        if conf.mode() == Mode::Refuse {
            drop(c);
            continue;
        }
        let Ok(u) = TcpStream::connect(format!("127.0.0.1:{up}")) else { continue };
        let _ = c.set_nodelay(true);
        let _ = u.set_nodelay(true);
        let epoch = conf.epoch.load(Relaxed);
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let (c2, u2) = (c.try_clone().unwrap(), u.try_clone().unwrap());
        let (k1, k2) = (conf.clone(), conf.clone());
        let s1 = seed;
        std::thread::spawn(move || pump(c, u, k1, epoch, s1));
        std::thread::spawn(move || pump(u2, c2, k2, epoch, s1 ^ 0xabcdef));
    }
}
