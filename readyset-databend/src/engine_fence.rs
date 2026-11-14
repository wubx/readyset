//! Single-base marker fence shared by legacy and heterogeneous-source adapters.
//! Never triggers a replay AFTER inserting the marker. It is not a JOIN/aggregate barrier.
use crate::reader_fence;
use anyhow::{Context, Result, bail, ensure};
use readyset_client::{ReaderAddress, Table, TableOperation, View};
use readyset_data::DfValue;
use readyset_server::Readers;
use std::time::{Duration, Instant};

fn epoch(readers: &Readers, address: &ReaderAddress) -> Option<usize> {
    readers
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_mut(address)
        .map(|r| r.eviction_epoch())
}

pub(crate) async fn fence(
    table: &mut Table,
    targets: &mut [View],
    columns: &[impl AsRef<str>],
    readers: &Readers,
    marker: impl Fn() -> Result<Vec<DfValue>>,
    timeout: Duration,
) -> Result<()> {
    if targets.is_empty() {
        return Ok(());
    }
    let until = Instant::now() + timeout;
    loop {
        ensure!(
            Instant::now() < until,
            "marker fence timeout under eviction pressure"
        );
        let row = marker()?;
        let mut probes = Vec::new();
        let mut valid = true;
        for view in targets.iter_mut() {
            let View::Single(reader) = view else {
                bail!("single reader required")
            };
            let address = ReaderAddress {
                node: *reader.node(),
                name: reader.name().clone(),
            };
            let (local, before_epoch) = {
                let mut shared = readers.lock().unwrap_or_else(|e| e.into_inner());
                let reader = shared.get_mut(&address).context("fence reader missing")?;
                let epoch = reader.eviction_epoch();
                (reader.clone(), epoch)
            };
            let key = reader_fence::key_values(view, columns, &row)?;
            let comparison = reader_fence::point_comparison(view, &key)?;
            // New full readers may still be replaying. Only this pre-insert phase may warm.
            loop {
                match reader_fence::point_contents(view, &key).await {
                    Ok(_) => break,
                    Err(_) if Instant::now() < until => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    Err(e) => return Err(e.context("fence reader never answered")),
                }
            }
            let wanted = reader_fence::row_values(view, columns, &row)?;
            let before = match local.get_multi(std::slice::from_ref(&comparison)) {
                Ok(rows) => rows
                    .iter()
                    .flat_map(|r| r.iter())
                    .filter(|r| r.as_ref() == wanted.as_slice())
                    .count(),
                Err(e) if e.is_miss() => {
                    valid = false;
                    break;
                }
                Err(e) => bail!("local marker preflight failed: {e:?}"),
            };
            // A marker must be absent physically, not merely distinct in source columns
            // erased by projection. Small-domain-only views are rejected before migration.
            if before != 0 {
                valid = false;
                break;
            }
            probes.push((address, local, before_epoch, comparison, wanted, before));
        }
        if !valid
            || probes
                .iter()
                .any(|(a, _, e, ..)| epoch(readers, a) != Some(*e))
        {
            continue;
        }
        table
            .perform_all(vec![TableOperation::Insert(row.clone())])
            .await?;
        let mut stable = true;
        for added in [1, 0] {
            if added == 0 {
                table
                    .perform_all(vec![TableOperation::DeleteRow { row: row.clone() }])
                    .await?;
                if !stable {
                    break;
                }
            }
            loop {
                ensure!(Instant::now() < until, "marker fence timeout");
                let mut complete = true;
                for (address, local, before_epoch, comparison, wanted, before) in &mut probes {
                    if epoch(readers, address) != Some(*before_epoch) {
                        stable = false;
                        break;
                    }
                    let count = match local.get_multi(std::slice::from_ref(comparison)) {
                        Ok(rows) => rows
                            .iter()
                            .flat_map(|r| r.iter())
                            .filter(|r| r.as_ref() == wanted.as_slice())
                            .count(),
                        Err(e) if e.is_miss() => {
                            stable = false;
                            break;
                        }
                        Err(e) => bail!("local marker probe failed: {e:?}"),
                    };
                    complete &= count == *before + added;
                }
                if !stable || complete {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        if stable
            && probes
                .iter()
                .all(|(a, _, e, ..)| epoch(readers, a) == Some(*e))
        {
            return Ok(());
        }
    }
}
