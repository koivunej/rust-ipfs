use std::ops::Range;

use crate::file::reader::{FileContent, FileReader, Traversal};
use crate::file::{FileMetadata, FileReadFailed, UnwrapBorrowedExt};

// Long term goal is to be able have more of the visit traits for implementing `ipfs.get`
// operation or to visit a directories with files, symlinks and nested directories.
pub trait Visitor {
    /// Visits the bytes inside of an unixfs file.
    fn visit_content(&mut self, content: &[u8]);

    /// Called when starting to process a file with it's metadata, which might be empty.
    fn visit_metadata(&mut self, _metadata: &FileMetadata) {}
}

/// IdleFileVisit represents a prepared file visit over a tree. The user has to know the CID and be
/// able to get the block for the visit.
pub struct IdleFileVisit<V> {
    visitor: V,
    range: Option<Range<u64>>,
}

impl<V: Visitor> IdleFileVisit<V> {
    pub fn new(visitor: V) -> Self {
        Self {
            visitor,
            range: None,
        }
    }

    /// Target range represents the target byte range of the file we are interested in visiting.
    pub fn with_target_range(self, range: Range<u64>) -> Self {
        Self {
            visitor: self.visitor,
            range: Some(range),
        }
    }

    /// Begins the visitation by offering the first block to be visited.
    pub fn start(mut self, block: &[u8]) -> Result<Visitation<V>, FileReadFailed> {
        let fr = FileReader::from_block(block)?;

        self.visitor.visit_metadata(fr.as_ref());

        let (content, traversal) = fr.content();

        match content {
            FileContent::Just(content) => {
                let content = if let Some(range) = self.range {
                    // FIXME: check and error if out of range?
                    &content[(range.start as usize)..(range.end as usize)]
                } else {
                    content
                };
                self.visitor.visit_content(content);
                Ok(Visitation::Completed(self.visitor))
            }
            FileContent::Spread(iter) => {
                // we need to select suitable here
                let mut pending = iter
                    .filter_map(|(link, range)| {
                        if let Some(target_range) = self.range.as_ref() {
                            if !partially_match_range(&range, &target_range) {
                                return None;
                            }
                        }

                        let hash = link.Hash.unwrap_borrowed().to_vec();

                        Some((hash, range))
                    })
                    .collect::<Vec<_>>();

                // order is reversed to consume them in the depth first order
                pending.reverse();

                if pending.is_empty() {
                    Ok(Visitation::Completed(self.visitor))
                } else {
                    Ok(Visitation::Continues(FileVisit {
                        visitor: self.visitor,
                        pending,
                        state: traversal,
                        range: self.range,
                    }))
                }
            }
        }
    }
}

fn partially_match_range(block: &Range<u64>, target: &Range<u64>) -> bool {
    use std::cmp::{max, min};

    max(block.start, target.start) <= min(block.end, target.end)
}

fn overlapping_slice<'a>(content: &'a [u8], block: &Range<u64>, target: &Range<u64>) -> &'a [u8] {
    use std::cmp::min;

    // println!("content of {}..{}, block {:?}, target: {:?}", 0, content.len(), block, target);

    if !partially_match_range(block, target) {
        &[][..]
    } else {
        let start;
        let end;

        // FIXME: these must have bugs and must be possible to simplify
        if target.start < block.start {
            // we mostly need something before
            start = 0;
            end = (min(target.end, block.end) - block.start) as usize;
        } else if target.end > block.end {
            // we mostly need something after
            start = (target.start - block.start) as usize;
            end = (min(target.end, block.end) - block.start) as usize;
        } else {
            // inside
            start = (target.start - block.start) as usize;
            end = start + (target.end - target.start) as usize;
        }

        // println!("{}..{} or {}..{}", start, end, block.start + start as u64, block.start + end as u64);

        &content[start..end]
    }
}

/// FileVisit represents an ongoing visitation over an UnixFs File tree.
///
/// The file visitor does **not** implement size validation of merkledag links at the moment. This
/// could be implmented with generational storage and it would require an u64 per link.
pub struct FileVisit<V> {
    visitor: V,
    /// The internal cache for pending work. Order is such that the next is always the last item,
    /// so it can be popped. This currently does use a lot of memory for very large files.
    ///
    /// One workaround would be to transform excess links to relative links to some block of a Cid.
    // FIXME: use Cid instead of Vec
    pending: Vec<(Vec<u8>, Range<u64>)>,
    /// Target range, if any. Used to filter the links so that we will only visit interesting
    /// parts.
    range: Option<Range<u64>>,
    state: Traversal,
}

impl<V: Visitor> FileVisit<V> {
    /// Access hashes of all pending links for prefetching purposes. The block for the first item
    /// returned by this iterator is the one which needs to be processed next with `continue_walk`.
    // FIXME: this must change to Cid
    pub fn pending_links(&self) -> impl Iterator<Item = &[u8]> {
        self.pending.iter().rev().map(|(link, _)| link.as_slice())
    }

    /// Continues the walk with the data for the first `pending_link` key.
    pub fn continue_walk(mut self, next: &[u8]) -> Result<Visitation<V>, FileReadFailed> {
        let traversal = self.state;
        let (_, range) = self
            .pending
            .pop()
            .expect("User called continue_walk there must have been a next link");

        // interesting, validation doesn't trigger if the range is the same?
        // FIXME: get rid of clone
        let fr = traversal.continue_walk(next, &range)?;
        let (content, traversal) = fr.content();
        match content {
            FileContent::Just(content) => {
                let content = if let Some(target_range) = self.range.as_ref() {
                    // this can be empty slice, if we can't find anything. it might be good to make
                    // this as an error but go-ipfs doesn't seem to consider it an error.
                    overlapping_slice(content, &range, target_range)
                } else {
                    content
                };

                self.visitor.visit_content(content);

                if !self.pending.is_empty() {
                    self.state = traversal;
                    Ok(Visitation::Continues(self))
                } else {
                    Ok(Visitation::Completed(self.visitor))
                }
            }
            FileContent::Spread(iter) => {
                let target_range = self.range.clone();
                let filtered = iter.filter_map(move |(link, range)| {
                    if let Some(target_range) = target_range.as_ref() {
                        if !partially_match_range(&range, &target_range) {
                            return None;
                        }
                    }

                    let hash = link.Hash.unwrap_borrowed().to_vec();
                    Some((hash, range))
                });

                let before = self.pending.len();
                self.pending.extend(filtered);
                (&mut self.pending[before..]).reverse();

                self.state = traversal;
                Ok(Visitation::Continues(self))
            }
        }
    }
}

/// Visitation represents the state after processing a single block. It becomes completed if there
/// are no more links to process in order and in that case, the unwrapped visitor is given back.
pub enum Visitation<V> {
    Completed(V),
    Continues(FileVisit<V>),
}

impl<V> Visitation<V> {
    /// Allows expecting the visitation was continued instead of it being completed.
    pub fn unwrap_continued(self) -> FileVisit<V> {
        match self {
            Visitation::Continues(fv) => fv,
            _ => panic!("unexpected completion of visit"),
        }
    }

    /// Allows expecintg the visitation was completed instead of it needing to be continued.
    pub fn unwrap_completion(self) -> V {
        match self {
            Visitation::Completed(v) => v,
            _ => panic!("unexpected continuation of visit"),
        }
    }
}
