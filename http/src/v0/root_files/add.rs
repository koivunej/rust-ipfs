use super::AddArgs;
use crate::v0::support::StringError;
use bytes::{buf::BufMutExt, Buf, BufMut, Bytes, BytesMut};
use cid::Cid;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use ipfs::unixfs::ll::{
    dir::builder::{BufferingTreeBuilder, TreeBuildingFailed, TreeConstructionFailed},
    file::adder::FileAdder,
};
use ipfs::{Block, Ipfs, IpfsTypes};
use mime::Mime;
use mpart_async::server::{MultipartError, MultipartStream};
use serde::Serialize;
use std::borrow::Cow;
use std::fmt;
use warp::{Rejection, Reply};

pub(super) async fn add_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    _opts: AddArgs,
    content_type: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Unpin + 'static,
) -> Result<impl Reply, Rejection> {
    let boundary = content_type
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| StringError::from("missing 'boundary' on content-type"))?;

    let stream = MultipartStream::new(Bytes::from(boundary), body.map_ok(|mut buf| buf.to_bytes()));

    // Stream<Output = Result<Json, impl Rejection>>
    //
    // refine it to
    //
    // Stream<Output = Result<Json, AddError>>
    //                          |      |
    //                          |   convert rejection and stop the stream?
    //                          |      |
    //                          |     /
    // Stream<Output = Result<impl Into<Bytes>, impl std::error::Error + Send + Sync + 'static>>

    let st = add_stream(ipfs, stream);

    // TODO: we could map the errors into json objects at least? (as we cannot return them as
    // trailers)

    let body = crate::v0::support::StreamResponse(st);

    Ok(body)
}

#[derive(Debug)]
enum AddError {
    Parsing(MultipartError),
    Header(MultipartError),
    InvalidFilename(std::str::Utf8Error),
    UnsupportedField(String),
    UnsupportedContentType(String),
    ResponseSerialization(serde_json::Error),
    Persisting(ipfs::Error),
    TreeGathering(TreeBuildingFailed),
    TreeBuilding(TreeConstructionFailed),
}

impl From<MultipartError> for AddError {
    fn from(e: MultipartError) -> AddError {
        AddError::Parsing(e)
    }
}

impl fmt::Display for AddError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for AddError {}

fn add_stream<St, E>(
    ipfs: Ipfs<impl IpfsTypes>,
    mut fields: MultipartStream<St, E>,
) -> impl Stream<Item = Result<Bytes, AddError>> + Send + 'static
where
    St: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<anyhow::Error> + Send + 'static,
{
    async_stream::try_stream! {
        // TODO: wrap-in-directory option
        let mut tree = BufferingTreeBuilder::default();

        let mut buffer = BytesMut::new();

        tracing::trace!("stream started");

        while let Some(mut field) = fields
            .try_next()
            .await?
        {

            let field_name = field.name().map_err(AddError::Header)?;

            // files are file{,-1,-2,-3,..}
            // directories are dir{,-1,-2,-3,..}

            let _ = if !field_name.starts_with("file") {
                // this seems constant for files and directories
                Err(AddError::UnsupportedField(field_name.to_string()))
            } else {
                // this is a bit ackward with the ? operator but it should save us the yield
                // Err(..) followed by return; this is only available in the `stream!` variant,
                // which continues after errors by default..
                Ok(())
            }?;

            let filename = field.filename().map_err(AddError::Header)?;
            let filename = percent_encoding::percent_decode_str(filename)
                .decode_utf8()
                .map(|cow| cow.into_owned())
                .map_err(AddError::InvalidFilename)?;

            let content_type = field.content_type().map_err(AddError::Header)?;

            let next = match content_type {
                "application/octet-stream" => {
                    tracing::trace!("processing file {:?}", filename);
                    let mut adder = FileAdder::default();
                    let mut total = 0u64;

                    loop {
                        let next = field
                            .try_next()
                            .await
                            .map_err(AddError::Parsing)?;

                        match next {
                            Some(next) => {
                                let mut read = 0usize;
                                while read < next.len() {
                                    let (iter, used) = adder.push(&next.slice(read..));
                                    read += used;

                                    let maybe_tuple = import_all(&ipfs, iter).await.map_err(AddError::Persisting)?;

                                    total += maybe_tuple.map(|t| t.1).unwrap_or(0);
                                }

                                tracing::trace!("read {} bytes", read);
                            }
                            None => break,
                        }
                    }

                    let (root, subtotal) = import_all(&ipfs, adder.finish())
                        .await
                        .map_err(AddError::Persisting)?
                        .expect("I think there should always be something from finish -- except if the link block has just been compressed?");

                    total += subtotal;

                    tracing::trace!("completed processing file of {} bytes: {:?}", total, filename);

                    // using the filename as the path since we can tolerate a single empty named file
                    // however the second one will cause issues
                    tree.put_file(&filename, root.clone(), total)
                        .map_err(AddError::TreeGathering)?;

                    let filename: Cow<'_, str> = if filename.is_empty() {
                        // cid needs to be repeated if no filename was given
                        Cow::Owned(root.to_string())
                    } else {
                        Cow::Owned(filename)
                    };

                    serde_json::to_writer((&mut buffer).writer(), &Response::Added {
                        name: filename,
                        hash: Quoted(&root),
                        size: Quoted(total),
                    }).map_err(AddError::ResponseSerialization)?;

                    buffer.put(&b"\r\n"[..]);

                    Ok(buffer.split().freeze())
                },
                /*"application/x-directory"
                |*/ unsupported => {
                    Err(AddError::UnsupportedContentType(unsupported.to_string()))
                }
            }?;

            yield next;
        }

        let mut full_path = String::new();
        let mut block_buffer = Vec::new();

        let mut iter = tree.build(&mut full_path, &mut block_buffer);

        while let Some(res) = iter.next_borrowed() {
            let (path, cid, total, block) = res.map_err(AddError::TreeBuilding)?;

            // shame we need to allocate once again here..
            ipfs.put_block(Block { cid: cid.to_owned(), data: block.into() }).await.map_err(AddError::Persisting)?;

            serde_json::to_writer((&mut buffer).writer(), &Response::Added {
                name: Cow::Borrowed(path),
                hash: Quoted(cid),
                size: Quoted(total),
            }).map_err(AddError::ResponseSerialization)?;

            buffer.put(&b"\r\n"[..]);

            yield buffer.split().freeze();
        }
    }
}

async fn import_all(
    ipfs: &Ipfs<impl IpfsTypes>,
    iter: impl Iterator<Item = (Cid, Vec<u8>)>,
) -> Result<Option<(Cid, u64)>, ipfs::Error> {
    // TODO: use FuturesUnordered
    let mut last: Option<Cid> = None;
    let mut total = 0u64;

    for (cid, data) in iter {
        total += data.len() as u64;
        let block = Block {
            cid,
            data: data.into_boxed_slice(),
        };

        let cid = ipfs.put_block(block).await?;

        last = Some(cid);
    }

    Ok(last.map(|cid| (cid, total)))
}

/// The possible response messages from /add.
#[derive(Debug, Serialize)]
#[serde(untagged)] // rename_all="..." doesn't seem to work at this level
enum Response<'a> {
    /// When progress=true query parameter has been given, this will be output every N bytes, or
    /// perhaps every chunk.
    #[allow(unused)] // unused == not implemented yet
    Progress {
        /// Probably the name of the file being added or empty if none was provided.
        name: Cow<'a, str>,
        /// Bytes processed since last progress; for a file, all progress reports must add up to
        /// the total file size.
        bytes: u64,
    },
    /// Output for every input item.
    #[serde(rename_all = "PascalCase")]
    Added {
        /// The resulting Cid as a string.
        hash: Quoted<&'a Cid>,
        /// Name of the file added from filename or the resulting Cid.
        name: Cow<'a, str>,
        /// Stringified version of the total cumulative size in bytes.
        size: Quoted<u64>,
    },
}

#[derive(Debug)]
struct Quoted<D>(pub D);

impl<D: fmt::Display> serde::Serialize for Quoted<D> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::v0::root_files::add;

    #[tokio::test]
    async fn add_single_block_file() {
        let ipfs = tokio_ipfs().await;

        // this is from interface-ipfs-core, pretty much simplest add a buffer test case
        // but the body content is from the pubsub test case I copied this from
        let response = warp::test::request()
            .path("/add")
            .header(
                "content-type",
                "multipart/form-data; boundary=-----------------------------Z0oYi6XyTm7_x2L4ty8JL",
            )
            .body(
                &b"-------------------------------Z0oYi6XyTm7_x2L4ty8JL\r\n\
                    Content-Disposition: form-data; name=\"file\"; filename=\"testfile.txt\"\r\n\
                    Content-Type: application/octet-stream\r\n\
                    \r\n\
                    Plz add me!\n\
                    \r\n-------------------------------Z0oYi6XyTm7_x2L4ty8JL--\r\n"[..],
            )
            .reply(&add(&ipfs))
            .await;

        let body = std::str::from_utf8(response.body()).unwrap();

        assert_eq!(
            body,
            "{\"Hash\":\"Qma4hjFTnCasJ8PVp3mZbZK5g2vGDT4LByLJ7m8ciyRFZP\",\"Name\":\"testfile.txt\",\"Size\":\"20\"}\r\n"
        );
    }

    async fn tokio_ipfs() -> ipfs::Ipfs<ipfs::TestTypes> {
        let options = ipfs::IpfsOptions::inmemory_with_generated_keys();
        let (ipfs, fut) = ipfs::UninitializedIpfs::new(options, None)
            .await
            .start()
            .await
            .unwrap();

        tokio::spawn(fut);
        ipfs
    }
}
