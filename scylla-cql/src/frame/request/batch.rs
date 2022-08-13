use crate::frame::{frame_errors::ParseError, value::ValueList};
use bytes::{BufMut, Bytes};
use std::convert::TryInto;

use crate::frame::{
    request::{Request, RequestOpcode},
    types,
    value::BatchValues,
};

// Batch flags
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;

pub struct Batch<'a, StatementsIter, Values>
where
    // It is expected that both of these iterators have the same size
    StatementsIter: Iterator<Item = BatchStatement<'a>> + Clone,
    Values: for<'v> BatchValues<'v>,
{
    pub statements: StatementsIter,
    pub statements_count: usize,
    pub batch_type: BatchType,
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::SerialConsistency>,
    pub timestamp: Option<i64>,
    pub values: Values,
}

/// The type of a batch.
#[derive(Clone, Copy)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum BatchStatement<'a> {
    Query { text: &'a str },
    Prepared { id: &'a Bytes },
}

impl<'a, StatementsIter, Values> Request for Batch<'a, StatementsIter, Values>
where
    StatementsIter: ExactSizeIterator<Item = BatchStatement<'a>> + Clone,
    Values: for<'v> BatchValues<'v>,
    // boilerplate trait bounds that the compiler can't infer
    for<'v> <<Values as BatchValues<'v>>::ValuesIter as Iterator>::Item: ValueList,
{
    const OPCODE: RequestOpcode = RequestOpcode::Batch;

    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        // Serializing type of batch
        buf.put_u8(self.batch_type as u8);

        // Serializing queries
        types::write_short(self.statements_count.try_into()?, buf);

        let mut n_serialized_statements = 0usize;
        for (statement, value_list) in self.statements.clone().zip(self.values.values_iter()) {
            statement.serialize(buf)?;
            value_list.write_to_request(buf)?;
            n_serialized_statements += 1;
        }
        if n_serialized_statements != self.statements_count {
            return Err(ParseError::BadDataToSerialize(
                "Mismatch between statement counts for batch query".to_owned(),
            ));
        }

        // Serializing consistency
        types::write_consistency(self.consistency, buf);

        // Serializing flags
        let mut flags = 0;
        if self.serial_consistency.is_some() {
            flags |= FLAG_WITH_SERIAL_CONSISTENCY;
        }
        if self.timestamp.is_some() {
            flags |= FLAG_WITH_DEFAULT_TIMESTAMP;
        }

        buf.put_u8(flags);

        if let Some(serial_consistency) = self.serial_consistency {
            types::write_serial_consistency(serial_consistency, buf);
        }
        if let Some(timestamp) = self.timestamp {
            types::write_long(timestamp, buf);
        }

        Ok(())
    }
}

impl BatchStatement<'_> {
    fn serialize(&self, buf: &mut impl BufMut) -> Result<(), ParseError> {
        match self {
            BatchStatement::Query { text } => {
                buf.put_u8(0);
                types::write_long_string(text, buf)?;
            }
            BatchStatement::Prepared { id } => {
                buf.put_u8(1);
                types::write_short_bytes(&id[..], buf)?;
            }
        }

        Ok(())
    }
}
