use crate::{
    dag::{ResolveError, UnexpectedResolved},
    Error, Ipfs, IpfsTypes,
};
use async_stream::stream;
use bitswap::Block;
use cid::Cid;
use futures::stream::Stream;
use ipfs_unixfs::file::{visit::IdleFileVisit, FileReadFailed};
use std::borrow::Borrow;
use std::ops::Range;

/// IPFS cat operation, producing a stream of file bytes. This is generic over the different kinds
/// of ways to own an `Ipfs` value in order to support both operating with borrowed `Ipfs` value
/// and an owned value. Passing an owned value allows the return value to be `'static`, which can
/// be helpful in some contexts, like the http.
///
/// Returns a stream of bytes on the file pointed with the Cid.
pub async fn cat<'a, Types, MaybeOwned>(
    ipfs: MaybeOwned,
    starting_point: impl Into<StartingPoint>,
    range: Option<Range<u64>>,
) -> Result<impl Stream<Item = Result<Vec<u8>, TraversalFailed>> + Send + 'a, TraversalFailed>
where
    Types: IpfsTypes,
    MaybeOwned: Borrow<Ipfs<Types>> + Send + 'a,
{
    let mut visit = IdleFileVisit::default();
    if let Some(range) = range {
        visit = visit.with_target_range(range);
    }

    // Get the root block to start the traversal. The stream does not expose any of the file
    // metadata. To get to it the user needs to create a Visitor over the first block.
    let Block { cid, data } = match starting_point.into() {
        StartingPoint::Left(path) => {
            let borrow = ipfs.borrow();
            let dag = borrow.dag();
            let (resolved, _) = dag
                .resolve(path, true)
                .await
                .map_err(TraversalFailed::Resolving)?;
            resolved
                .into_unixfs_block()
                .map_err(TraversalFailed::Path)?
        }
        StartingPoint::Right(block) => block,
    };

    let mut cache = None;
    // Start the visit from the root block. We need to move the both components as Options into the
    // stream as we can't yet return them from this Future context.
    let (visit, bytes) = match visit.start(&data) {
        Ok((bytes, _, _, visit)) => {
            let bytes = if !bytes.is_empty() {
                Some(bytes.to_vec())
            } else {
                None
            };

            (visit, bytes)
        }
        Err(e) => {
            return Err(TraversalFailed::Walking(cid, e));
        }
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let jh = tokio::spawn({
        use crate::IpfsPath;
        use futures::stream::{StreamExt, TryStreamExt};
        let ipfs = ipfs.borrow().to_owned();
        async move {
            rx.for_each_concurrent(2, |cid| {
                let ipfs = ipfs.clone();
                async move {
                    crate::refs::refs(
                        ipfs,
                        futures::stream::once(futures::future::ready(Ok::<_, crate::Error>(
                            IpfsPath::from(cid),
                        ))),
                        None,
                        true,
                    )
                    .try_fold(0usize, |count, _| futures::future::ready(Ok(count + 1)))
                    .await
                    .unwrap();
                }
            })
            .await;

            /*let prefetched: Result<usize, _> = crate::refs::refs(
                ipfs,
                rx.map(|cid: Cid| Ok::<_, crate::Error>(IpfsPath::from(cid))),
                None,
                true,
            )
            .try_fold(0usize, |count, _| futures::future::ready(Ok(count + 1)))
            .await;

            match prefetched {
                Ok(count) => info!("prefetched {} blocks", count),
                Err(e) => info!("prefetching stopped on {}", e),
            }*/
        }
    });

    // FIXME: we could use the above file_size to set the content-length ... but calculating it
    // with the ranges is not ... trivial?

    // using async_stream here at least to get on faster; writing custom streams is not too easy
    // but this might be easy enough to write open.
    Ok(stream! {

        {
            if let Some(bytes) = bytes {
                yield Ok(bytes);
            }

            let mut visit = match visit {
                Some(visit) => visit,
                None => return,
            };

            let mut prefetch = true;
            let mut last_pending = None;

            loop {
                // TODO: if it was possible, it would make sense to start downloading N of these
                // we could just create an FuturesUnordered which would drop the value right away. that
                // would probably always cost many unnecessary clones, but it would be nice to "shut"
                // the subscriber so that it will only resolve to a value but still keep the operation
                // going. Not that we have any "operation" concept of the Want yet.
                let next = {
                    let (next, links) = visit.pending_links();

                    if prefetch {
                        let pending = links.skip(last_pending.unwrap_or(0)).map(|cid| cid.to_owned());

                        let mut count = 0;

                        for cid in pending {
                            eprintln!("prefetching {}", cid);
                            if tx.send(cid).is_ok() {
                                count += 1;
                                prefetch = false;
                            }
                        }

                        if count == 0 && last_pending.unwrap_or(0) > 0 {
                            // the pending are starting to contract, we can stop prefetching more
                            prefetch = false;
                        }

                        last_pending = Some(last_pending.unwrap_or(0) + count);
                    }

                    next
                };

                let borrow = ipfs.borrow();
                let Block { cid, data } = match borrow.get_block(&next).await {
                    Ok(block) => block,
                    Err(e) => {
                        yield Err(TraversalFailed::Loading(next.to_owned(), e));
                        break;
                    },
                };

                match visit.continue_walk(&data, &mut cache) {
                    Ok((bytes, next_visit)) => {
                        if !bytes.is_empty() {
                            // TODO: manual implementation could allow returning just the slice
                            yield Ok(bytes.to_vec());
                        }

                        match next_visit {
                            Some(v) => visit = v,
                            None => break,
                        }
                    }
                    Err(e) => {
                        yield Err(TraversalFailed::Walking(cid, e));
                        break;
                    }
                }
            }
        }

        drop(tx);
        jh.await.unwrap();
    })
}

/// The starting point for unixfs walks. Can be converted from IpfsPath and Blocks, and Cids can be
/// converted to IpfsPath.
pub enum StartingPoint {
    Left(crate::IpfsPath),
    Right(Block),
}

impl<T: Into<crate::IpfsPath>> From<T> for StartingPoint {
    fn from(a: T) -> Self {
        Self::Left(a.into())
    }
}

impl From<Block> for StartingPoint {
    fn from(b: Block) -> Self {
        Self::Right(b)
    }
}

/// Types of failures which can occur while walking the UnixFS graph.
#[derive(Debug, thiserror::Error)]
pub enum TraversalFailed {
    /// Failure to resolve the given path; does not happen when given a block.
    #[error("path resolving failed")]
    Resolving(#[source] ResolveError),

    /// The given path was resolved to non dag-pb block, does not happen when starting the walk
    /// from a block.
    #[error("path resolved to unexpected")]
    Path(#[source] UnexpectedResolved),

    /// Loading of a block during walk failed
    #[error("loading of {} failed", .0)]
    Loading(Cid, #[source] Error),

    /// Processing of the block failed
    #[error("walk failed on {}", .0)]
    Walking(Cid, #[source] FileReadFailed),
}
