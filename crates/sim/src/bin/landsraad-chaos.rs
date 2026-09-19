//! `landsraad-chaos`: runs many randomized fault schedules against the
//! simulated cluster and reports any invariant violation with the seed that
//! reproduces it.
//!
//!   landsraad-chaos [--runs N] [--ticks T] [--seed S] [--churn]
//!   landsraad-chaos --measure
//!
//! Each run's cluster size and fault intensities are derived from its seed,
//! so `--seed S --runs 1` replays run S exactly. `--churn` uses short
//! election timeouts and frequent crashes/partitions (the hardest setting).

use std::process::ExitCode;

use channel::FaultProfile;
use sim::{Cluster, ClusterConfig, Faults, Rng};

fn config_for(seed: u64, churn: bool) -> ClusterConfig {
    let mut r = Rng::new(seed ^ 0x1234_5678_9ABC_DEF0);
    let f = |r: &mut Rng, max: f64| (r.next_u64() >> 11) as f64 / (1u64 << 53) as f64 * max;
    let mut cfg = ClusterConfig::new(if r.chance(0.4) { 5 } else { 3 }, seed);
    cfg.profile = FaultProfile {
        loss: f(&mut r, 0.3),
        duplication: f(&mut r, 0.2),
        reorder_max_delay: r.below(30) as u64,
        corruption: f(&mut r, 0.1),
        truncation: f(&mut r, 0.1),
        base_delay: 1 + r.below(3) as u64,
    };
    let scale = if churn { 2.0 } else { 1.0 };
    cfg.faults = Faults {
        crash: f(&mut r, 0.01) * scale,
        restart: 0.02 + f(&mut r, 0.05),
        partition: f(&mut r, 0.01) * scale,
        heal: 0.02 + f(&mut r, 0.05),
        crash_after_event: f(&mut r, 0.003) * scale,
        propose: 0.05 + f(&mut r, 0.15),
    };
    if churn {
        cfg.election_timeout = (20, 45);
        cfg.heartbeat_interval = 6;
    }
    cfg
}

fn summarize(mut v: Vec<u64>) -> String {
    v.sort_unstable();
    let p = |q: f64| v[((v.len() - 1) as f64 * q) as usize];
    format!(
        "min {:>5}  median {:>5}  p99 {:>5}  max {:>5}  (n={})",
        v[0],
        p(0.5),
        p(0.99),
        v[v.len() - 1],
        v.len()
    )
}

fn measure() {
    println!("landsraad measurements (deterministic virtual ticks; election timeout 150-300, heartbeat 50)\n");
    for (name, profile) in [
        ("clean network", FaultProfile::CLEAN),
        (
            "10% loss, 5% dup, reorder<=10",
            FaultProfile {
                loss: 0.10,
                duplication: 0.05,
                reorder_max_delay: 10,
                corruption: 0.0,
                truncation: 0.0,
                base_delay: 1,
            },
        ),
    ] {
        for n in [3usize, 5] {
            let mut election = Vec::new();
            let mut commit = Vec::new();
            for seed in 0..200u64 {
                let mut cfg = ClusterConfig::new(n, seed);
                cfg.profile = profile;
                let mut c = Cluster::new(cfg);
                while c.leader().is_none() {
                    c.step().unwrap();
                }
                election.push(c.now());
                for k in 0..10 {
                    let Ok((l, idx)) = c.propose(format!("m{k}").into_bytes()) else {
                        break;
                    };
                    let t0 = c.now();
                    let mut waited = 0;
                    while c.node(l).is_some_and(|x| x.commit_index() < idx) && waited < 3000 {
                        c.step().unwrap();
                        waited += 1;
                    }
                    commit.push(c.now() - t0);
                }
            }
            println!("{name}, {n} nodes");
            println!("  first leader elected: {}", summarize(election));
            println!("  propose -> committed:  {}\n", summarize(commit));
        }
    }
}

fn main() -> ExitCode {
    let (mut runs, mut ticks, mut start, mut churn, mut do_measure) =
        (200u64, 4000u64, 0u64, false, false);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut num = |name: &str| -> u64 {
            args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
                eprintln!("{name} needs a number");
                std::process::exit(2)
            })
        };
        match a.as_str() {
            "--runs" => runs = num("--runs"),
            "--ticks" => ticks = num("--ticks"),
            "--seed" => start = num("--seed"),
            "--churn" => churn = true,
            "--measure" => do_measure = true,
            other => {
                eprintln!("unknown argument {other:?}\nusage: landsraad-chaos [--runs N] [--ticks T] [--seed S] [--churn] | --measure");
                return ExitCode::from(2);
            }
        }
    }
    if do_measure {
        measure();
        return ExitCode::SUCCESS;
    }

    let (mut violations, mut commits, mut terms, mut crashes) = (0u64, 0usize, 0u64, 0u64);
    let mut faults = channel::FaultStats::default();
    for seed in start..start + runs {
        let mut c = Cluster::new(config_for(seed, churn));
        match c.run(ticks) {
            Ok(()) => {
                commits += c.committed().len();
                terms += c.stats().terms_with_a_leader;
                crashes += c.stats().crashes;
                let s = c.channel_stats();
                faults.sent += s.sent;
                faults.dropped += s.dropped;
                faults.duplicated += s.duplicated;
                faults.corrupted += s.corrupted;
            }
            Err(v) => {
                violations += 1;
                println!(
                    "VIOLATION seed {seed}{}: {v}\n  reproduce: landsraad-chaos --seed {seed} --runs 1 --ticks {ticks}{}",
                    if churn { " (churn)" } else { "" },
                    if churn { " --churn" } else { "" }
                );
            }
        }
    }
    println!(
        "{runs} runs x {ticks} ticks{}: {violations} violation(s); {commits} entries committed, {terms} leader terms, {crashes} crashes; datagrams sent {}, dropped {}, duplicated {}, corrupted {}",
        if churn { " (churn)" } else { "" },
        faults.sent, faults.dropped, faults.duplicated, faults.corrupted
    );
    if violations == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}
