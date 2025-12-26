use std::io;

#[derive(Debug)]
pub enum Error {
    InvalidDbLocation,
    FileDirectoryCreation,
    Serialization(postcard::Error),
    IOError(io::Error),
}

impl From<postcard::Error> for Error {
    fn from(error: postcard::Error) -> Self {
        Error::Serialization(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IOError(error)
    }
}
