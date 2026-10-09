//! Serve a recorded Tycho feed block by block, stepped by a client.
//!
//! A recording is the zstd-compressed JSON that `tools/record-market` writes: a token list, a
//! snapshot message, then one message per block. Replaying it through [`TychoFeed`] gives the
//! solver the market a live feed would have built at each of those blocks, so quotes can be taken
//! at past blocks through the normal API.
//!
//! The first block (the snapshot) is served from the start. Each later block is applied when a
//! client calls [`ReplayControl::advance`], which answers once the block is applied and its
//! derived data computed, so every block can be measured exactly once however long the client's
//! work takes.
//!
//! Live gas prices would be wrong for a past block, so each block's gas price comes from a
//! separate schedule (`{"<block number>": "<wei>", ...}`).

use std::{collections::BTreeMap, path::Path, time::Duration};

use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use tycho_simulation::{
    protocol::models::Update,
    tycho_client::feed::{dto, FeedMessage},
    tycho_common::models::{token::Token, Chain},
    tycho_ethereum::gas::{BlockGasPrice, GasPrice},
};

use crate::feed::{
    market_data::MarketData, protocol_registry::decode_recorded_messages, tycho_feed::TychoFeed,
    DataFeedError,
};

/// How long a derived-data pass may take before a step answers without it.
const DERIVED_WAIT: Duration = Duration::from_secs(60);

/// The parts of a recording file the replay reads.
#[derive(Deserialize)]
struct RecordingFile {
    metadata: RecordingMetadata,
    tokens: Vec<Token>,
    messages: Vec<dto::FeedMessage>,
}

#[derive(Deserialize)]
struct RecordingMetadata {
    chain: String,
    protocols: Vec<String>,
    min_token_quality: i32,
}

/// The block a replay serves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReplayPosition {
    /// Block number now served.
    pub block: u64,
    /// That block's timestamp (Unix seconds).
    pub timestamp: u64,
    /// Its index in the recording, from 0 (the snapshot).
    pub index: usize,
    /// Number of blocks in the recording.
    pub total: usize,
    /// True at the recording's last block; further steps return it again.
    pub done: bool,
}

/// A request to step: to the next block, or on to the first block at or after `to_block`.
struct Step {
    to_block: Option<u64>,
    reply: oneshot::Sender<ReplayPosition>,
}

/// Steps a recorded feed.
#[derive(Clone)]
pub struct ReplayControl {
    steps: mpsc::Sender<Step>,
}

impl ReplayControl {
    /// Applies the next block, or every block up to the first at or after `to_block`, and returns
    /// the block then served once its derived data is computed.
    ///
    /// A `to_block` at or before the current block does not step. At the recording's end the last
    /// block is returned again.
    ///
    /// # Errors
    ///
    /// Fails when the replay has stopped.
    pub async fn advance(&self, to_block: Option<u64>) -> Result<ReplayPosition, String> {
        let (reply, answer) = oneshot::channel();
        self.steps
            .send(Step { to_block, reply })
            .await
            .map_err(|_| "the replay has stopped".to_string())?;
        answer
            .await
            .map_err(|_| "the replay has stopped".to_string())
    }
}

/// A decoded recording and the gas price for each of its blocks, ready to replay.
pub struct RecordedFeed {
    chain: Chain,
    protocols: Vec<String>,
    updates: Vec<Update>,
    gas_price_by_block: BTreeMap<u64, BigUint>,
    control: ReplayControl,
    steps: mpsc::Receiver<Step>,
}

impl RecordedFeed {
    /// Reads and decodes a recording and its gas schedule.
    ///
    /// # Errors
    ///
    /// Fails when either file cannot be read or parsed, the chain is unknown, the recording holds
    /// a protocol a block-by-block replay cannot serve, or a message does not decode.
    pub async fn load(recording_path: &Path, gas_path: &Path) -> Result<Self, String> {
        let compressed = std::fs::read(recording_path)
            .map_err(|e| format!("cannot read recording {}: {e}", recording_path.display()))?;
        let json = zstd::decode_all(compressed.as_slice())
            .map_err(|e| format!("recording {} is not zstd: {e}", recording_path.display()))?;
        let recording: RecordingFile = serde_json::from_slice(&json)
            .map_err(|e| format!("cannot parse recording {}: {e}", recording_path.display()))?;
        drop(json);
        let gas_price_by_block = read_gas_schedule(gas_path)?;

        // Decoding every message up front leaves tycho-simulation's shared contract storage at
        // the last block (see `decode_recorded_messages`), so pools that read it at simulation
        // time would quote every block against the last one.
        if let Some(system) = recording
            .metadata
            .protocols
            .iter()
            .find(|system| system.starts_with("vm:") || system.as_str() == "uniswap_v4_hooks")
        {
            return Err(format!(
                "recording holds {system}, whose pools read shared contract storage that a \
                 block-by-block replay cannot keep per block; record native protocols only"
            ));
        }
        let chain =
            crate::types::parse_chain(&recording.metadata.chain).map_err(|e| e.to_string())?;
        let min_token_quality =
            u32::try_from(recording.metadata.min_token_quality).map_err(|_| {
                format!("min_token_quality {} is negative", recording.metadata.min_token_quality)
            })?;
        let updates = decode_recorded_messages(
            chain,
            &recording.metadata.protocols,
            min_token_quality,
            recording.tokens,
            recording
                .messages
                .into_iter()
                .map(FeedMessage::from),
        )
        .await?;
        let (steps_tx, steps) = mpsc::channel(16);
        Ok(Self {
            chain,
            protocols: recording.metadata.protocols,
            updates,
            gas_price_by_block,
            control: ReplayControl { steps: steps_tx },
            steps,
        })
    }

    /// The chain the recording was made on.
    pub fn chain(&self) -> Chain {
        self.chain
    }

    /// The protocol systems the recording holds.
    pub fn protocols(&self) -> &[String] {
        &self.protocols
    }

    /// The handle that steps this replay.
    pub fn control(&self) -> ReplayControl {
        self.control.clone()
    }

    /// Serves the first block, then applies blocks as clients step, then keeps serving the last.
    ///
    /// The feed never ends on its own: the server treats a finished feed as a failure and shuts
    /// down, and the last block should stay quotable. `derived_done` reports the last block the
    /// derived computations finished; a step waits on it.
    pub(crate) async fn run(
        mut self,
        feed: TychoFeed,
        market_data: MarketData,
        mut derived_done: watch::Receiver<u64>,
    ) -> Result<(), DataFeedError> {
        let total = self.updates.len();
        let mut updates = std::mem::take(&mut self.updates)
            .into_iter()
            .enumerate();
        let Some((index, first)) = updates.next() else {
            tracing::warn!("recording holds no blocks");
            std::future::pending::<()>().await;
            return Ok(());
        };
        let mut position = self
            .apply(&feed, &market_data, first, index, total)
            .await?;
        while let Some(step) = self.steps.recv().await {
            // The derived computations run only for blocks that change the market; wait for the
            // last such block this step applied.
            let mut recompute_block = None;
            while !position.done &&
                step.to_block
                    .is_none_or(|target| position.block < target)
            {
                let Some((index, update)) = updates.next() else { break };
                let changes = !update.new_pairs.is_empty() ||
                    !update.removed_pairs.is_empty() ||
                    !update.states.is_empty();
                position = self
                    .apply(&feed, &market_data, update, index, total)
                    .await?;
                if changes {
                    recompute_block = Some(position.block);
                }
                if step.to_block.is_none() {
                    break;
                }
            }
            if let Some(block) = recompute_block {
                wait_for_derived(&mut derived_done, block).await;
            }
            let _ = step.reply.send(position.clone());
        }
        std::future::pending::<()>().await;
        Ok(())
    }

    /// Applies one block and its gas price, returning the block now served.
    async fn apply(
        &self,
        feed: &TychoFeed,
        market_data: &MarketData,
        update: Update,
        index: usize,
        total: usize,
    ) -> Result<ReplayPosition, DataFeedError> {
        feed.handle_tycho_message(update)
            .await?;
        let head = market_data
            .read()
            .await
            .last_updated()
            .cloned();
        let (block, timestamp) = head
            .map(|head| (head.number(), head.timestamp()))
            .unwrap_or_default();
        match self.gas_price_by_block.get(&block) {
            Some(gas_price) => market_data
                .write()
                .await
                .update_gas_price(BlockGasPrice {
                    block_number: block,
                    block_hash: Default::default(),
                    block_timestamp: timestamp,
                    pricing: GasPrice::Legacy { gas_price: gas_price.clone() },
                }),
            None => tracing::warn!(
                block,
                "no gas price for this block in the schedule; keeping the previous one"
            ),
        }
        tracing::info!(block, index, total, "replayed block");
        Ok(ReplayPosition { block, timestamp, index, total, done: index + 1 == total })
    }
}

/// Reads a gas schedule: `{"<block number>": "<wei>", ...}`.
fn read_gas_schedule(path: &Path) -> Result<BTreeMap<u64, BigUint>, String> {
    let json = std::fs::read(path)
        .map_err(|e| format!("cannot read gas schedule {}: {e}", path.display()))?;
    let by_block: BTreeMap<String, String> = serde_json::from_slice(&json)
        .map_err(|e| format!("cannot parse gas schedule {}: {e}", path.display()))?;
    by_block
        .into_iter()
        .map(|(block, wei)| {
            let number = block
                .parse()
                .map_err(|e| format!("gas schedule block {block:?} is not a number: {e}"))?;
            let price = wei
                .parse()
                .map_err(|e| format!("gas price {wei:?} for block {block} is not a number: {e}"))?;
            Ok((number, price))
        })
        .collect()
}

/// Waits until the derived computations have run for `block`, or [`DERIVED_WAIT`] passes.
async fn wait_for_derived(derived_done: &mut watch::Receiver<u64>, block: u64) {
    let finished = tokio::time::timeout(DERIVED_WAIT, derived_done.wait_for(|done| *done >= block));
    if finished.await.is_err() {
        tracing::warn!(block, "derived data not computed in time; stepping without it");
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::feed::TychoFeedConfig;

    fn fixture() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/market_recording.json.zst")
    }

    /// The fixture's blocks, from its own messages.
    fn fixture_blocks() -> Vec<u64> {
        let json = zstd::decode_all(
            std::fs::read(fixture())
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        value["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|message| {
                let msgs = message["state_msgs"]
                    .as_object()
                    .unwrap();
                msgs.values().next().unwrap()["header"]["number"]
                    .as_u64()
                    .unwrap()
            })
            .collect()
    }

    fn gas_schedule(dir: &Path, blocks: &[u64]) -> PathBuf {
        let path = dir.join("gas.json");
        let schedule: BTreeMap<String, String> = blocks
            .iter()
            .map(|block| (block.to_string(), (block * 10).to_string()))
            .collect();
        std::fs::write(&path, serde_json::to_vec(&schedule).unwrap()).unwrap();
        path
    }

    /// What a test replay needs kept alive.
    struct Replay {
        _dir: tempfile::TempDir,
        _market_events: tokio::sync::broadcast::Receiver<crate::feed::events::MarketEvent>,
    }

    /// Starts a replay of the fixture; the returned sender plays the derived computations.
    async fn start() -> (ReplayControl, MarketData, watch::Sender<u64>, Vec<u64>, Replay) {
        let dir = tempfile::tempdir().unwrap();
        let blocks = fixture_blocks();
        let recorded = RecordedFeed::load(&fixture(), &gas_schedule(dir.path(), &blocks))
            .await
            .unwrap();
        let control = recorded.control();
        let market_data = MarketData::new_shared();
        let feed = TychoFeed::new(
            TychoFeedConfig::new(
                "ws://replay".to_string(),
                recorded.chain(),
                None,
                false,
                vec![],
                0.0,
            ),
            market_data.clone(),
        );
        // The feed fails a broadcast with no receiver; in the server the worker pools subscribe.
        let market_events = feed.subscribe();
        let (derived_tx, derived_rx) = watch::channel(u64::MAX);
        tokio::spawn(recorded.run(feed, market_data.clone(), derived_rx));
        (
            control,
            market_data,
            derived_tx,
            blocks,
            Replay { _dir: dir, _market_events: market_events },
        )
    }

    async fn served(market_data: &MarketData) -> (u64, Option<BigUint>) {
        let state = market_data.read().await;
        let block = state
            .last_updated()
            .map(|b| b.number())
            .unwrap();
        let gas = state.gas_price().map(|g| {
            let GasPrice::Legacy { gas_price } = &g.pricing else {
                panic!("the replay sets legacy gas prices, got {:?}", g.pricing)
            };
            gas_price.clone()
        });
        (block, gas)
    }

    #[tokio::test]
    async fn test_advance_steps_one_block_with_its_gas_price() {
        let (control, market_data, _derived, blocks, _dir) = start().await;
        for (index, &block) in blocks.iter().enumerate().skip(1) {
            let position = control.advance(None).await.unwrap();
            assert_eq!((position.block, position.index), (block, index));
            assert_eq!(served(&market_data).await, (block, Some(BigUint::from(block * 10))));
        }
        let last = blocks.len() - 1;
        let again = control.advance(None).await.unwrap();
        assert_eq!((again.block, again.index, again.done), (blocks[last], last, true));
    }

    #[tokio::test]
    async fn test_advance_to_block() {
        let (control, market_data, _derived, blocks, _dir) = start().await;
        let position = control
            .advance(Some(blocks[3]))
            .await
            .unwrap();
        assert_eq!((position.block, position.index), (blocks[3], 3));
        assert_eq!(served(&market_data).await.0, blocks[3]);
        let unchanged = control
            .advance(Some(blocks[1]))
            .await
            .unwrap();
        assert_eq!(unchanged.block, blocks[3]);
    }

    #[tokio::test]
    async fn test_advance_waits_for_derived_data() {
        let (control, _market_data, derived, blocks, _dir) = start().await;
        derived.send_replace(0);
        let step = tokio::spawn(async move { control.advance(None).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!step.is_finished(), "answered before the derived data was computed");
        derived.send_replace(blocks[1]);
        assert_eq!(step.await.unwrap().unwrap().block, blocks[1]);
    }

    #[tokio::test]
    async fn test_load_refuses_vm_protocols() {
        let dir = tempfile::tempdir().unwrap();
        let recording = dir.path().join("vm.json.zst");
        let json = serde_json::json!({
            "metadata": {"chain": "ethereum", "protocols": ["vm:curve"], "min_token_quality": 100},
            "tokens": [],
            "messages": [],
        });
        std::fs::write(&recording, zstd::encode_all(json.to_string().as_bytes(), 3).unwrap())
            .unwrap();
        let error = match RecordedFeed::load(&recording, &gas_schedule(dir.path(), &[])).await {
            Err(error) => error,
            Ok(_) => panic!("a VM recording loaded"),
        };
        assert!(error.contains("vm:curve"), "{error}");
    }
}
