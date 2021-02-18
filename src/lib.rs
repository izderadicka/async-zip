use std::{io, path::{Path, PathBuf}};
use tokio::{
    fs,
    io::AsyncReadExt,
    sync::mpsc::{channel, Receiver, Sender},
};

use crate::error::Result;
use crate::zip::{Descriptor, Directory, FileHeader, ToBytes};

mod date;
pub mod error;
mod zip;
pub struct Zipper<P> {
    files: Box<dyn Iterator<Item = P> + Send>,
}

impl<P> Zipper<P>
where
    P: AsRef<Path> + Send + Sync + 'static,

{
    
    pub fn from_iter<I>(files: I) -> Self
    where
        I: Iterator<Item = P> + Send + 'static,
    {
        Zipper {
            files: Box::new(files),
        }
    }

    async fn main_loop(
        mut files: Box<dyn Iterator<Item = P> + Send>,
        sender: Sender<std::result::Result<Vec<u8>, io::Error>>,
    ) -> Result<()> {
        let mut pos: u64 = 0;
        let mut dir = Directory::new();

        macro_rules! send {
            ($data:ident) => {
                pos += $data.len() as u64;
                sender.send(Ok($data)).await.expect("receiver gone");
            };
        }

        while let Some(path) = files.next() {
            let mut f = fs::File::open(&path).await?;
            let meta = f.metadata().await?;
            // send header
            let file_header = FileHeader::new(path, meta.modified()?);
            let file_header_bytes = file_header.to_bytes()?;
            let file_header_offset = pos;
            send!(file_header_bytes);

            let file_content_offset = pos;
            let mut hasher = crc32fast::Hasher::new();
            loop {
                let mut data = Vec::with_capacity(8 * 1024);
                let read = f.read_buf(&mut data).await?;
                if read == 0 {
                    break;
                }
                hasher.update(&data);
                send!(data);
            }

            let file_size = pos - file_content_offset;
            let crc = hasher.finalize();
            let desc = Descriptor::new(file_size, crc);
            let desc_bytes = desc.to_bytes()?;
            send!(desc_bytes);
            dir.add_entry(file_header, desc, file_header_offset);
        }
        let directory_bytes = dir.finalize(pos)?;
        send!(directory_bytes);

        Ok(())
    }

    pub fn zipped_stream(self) -> Receiver<std::result::Result<Vec<u8>, io::Error>> {
        let (s, r) = channel(64);

        tokio::spawn(async move {
            let sender = s.clone();
            let res = Zipper::main_loop(self.files, sender).await;
            if let Err(e) = res {
                s.send(Err(e.into())).await.ok();
            }
        });
        r
    }
}

impl Zipper<PathBuf> {
    pub async fn from_directory(path: impl AsRef<Path>) -> std::result::Result<Zipper<PathBuf>, io::Error> {
        let mut files = vec![];
        let mut dir_listing = fs::read_dir(path).await?;
        while let Some(entry) = dir_listing.next_entry().await? {
            if entry.file_type().await?.is_file() {
                files.push(entry.path())
            }
        }
        
        Ok(Zipper::from_iter(files.into_iter()))
    }
}

#[cfg(test)]
mod tests {

    use std::io::{Cursor, Read, Write};
    use crate::error::Result;
    use super::Zipper;
    use tokio::io::AsyncReadExt;
    use zip::ZipArchive;
    #[tokio::test]
    async fn test_zip_stream() -> Result<()>{
        let zipper = Zipper::from_directory("src").await?;
        let mut stream = zipper.zipped_stream();
        let mut f = Cursor::new(Vec::<u8>::new());
        while let Some(chunk) = stream.recv().await {
            f.write_all(&(chunk?)).unwrap();
        }

        assert!(f.get_ref().len()>1000);

        f.set_position(0);

        let mut zip = ZipArchive::new(f).expect("cannot open archive");
        assert_eq!(zip.len(), 4);
        for i in 0..zip.len() {
            let mut file = zip.by_index(i).expect("entry error");
            println!("Filename: {} {} {:?}", file.name(), file.size(), file.last_modified());
            let mut content = vec![];
            file.read_to_end(&mut content).expect("read content error");

            let mut tf = tokio::fs::File::open(file.name()).await.expect("cannot open file");
            let meta = tf.metadata().await.expect("cannot get metadata");

            assert_eq!(meta.len(), file.size());
            let mut tc = vec![];
            tf.read_to_end(&mut tc).await.expect("cannot read file");
            assert_eq!(tc, content);
;        }


        Ok(())
    }
}
