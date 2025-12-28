use std::io;

use crate::serialization::SerializationError;

#[derive(Debug)]
pub enum Error {
    InvalidDbLocation,
    FileDirectoryCreation,
    Serialization(SerializationError),
    IO(io::Error),
    TooBig,
}

impl From<SerializationError> for Error {
    fn from(error: SerializationError) -> Self {
        Error::Serialization(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IO(error)
    }
}
