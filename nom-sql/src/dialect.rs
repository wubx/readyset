use std::fmt;
use std::str::{self, FromStr};

use bit_vec::BitVec;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take, take_while1};
use nom::character::complete::char;
use nom::character::is_alphanumeric;
use nom::combinator::{map, map_res, not, opt, peek};
use nom::error::ErrorKind;
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom_locate::LocatedSpan;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::keywords::{sql_keyword, sql_keyword_or_builtin_function, POSTGRES_NOT_RESERVED};
use crate::literal::{raw_string_literal, QuotingStyle};
use crate::select::LimitClause;
use crate::whitespace::whitespace0;
use crate::{literal, NomSqlError, NomSqlResult, SqlIdentifier};

#[inline]
pub(crate) fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

/// Byte array literal value (PostgreSQL)
fn raw_hex_bytes_psql(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    delimited(tag("E'\\\\x"), hex_bytes, tag("'::bytea"))(input)
}

/// Blob literal value (MySQL)
fn raw_hex_bytes_mysql(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    delimited(tag("X'"), hex_bytes, tag("'"))(input)
}

fn hex_bytes(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    fold_many0(
        map_res(take(2_usize), |i: LocatedSpan<&[u8]>| hex::decode(*i)),
        Vec::new,
        |mut acc: Vec<u8>, bytes: Vec<u8>| {
            acc.extend(bytes);
            acc
        },
    )(input)
}

/// Bit vector literal value (PostgreSQL)
fn raw_bit_vector_psql(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], BitVec> {
    delimited(tag_no_case("b'"), bits, tag("'"))(input)
}

fn bits(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], BitVec> {
    fold_many0(
        map(alt((char('0'), char('1'))), |i: char| i == '1'),
        BitVec::new,
        |mut acc: BitVec, bit: bool| {
            acc.push(bit);
            acc
        },
    )(input)
}

/// Specification for a SQL dialect to use when parsing
///
/// Currently, Dialect controls the escape characters used for identifiers, and the quotes used to
/// surround string literals, but may be extended to cover more dialect differences in the future
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum Dialect {
    /// The SQL dialect used by PostgreSQL.
    ///
    /// Identifiers are escaped with double quotes (`"`) and strings use only single quotes (`'`)
    PostgreSQL,

    /// The SQL dialect used by MySQL.
    ///
    /// Identifiers are escaped with backticks (`\``) or square brackets (`[` and `]`) and strings
    /// use either single quotes (`'`) or double quotes (`"`)
    MySQL,
}

#[derive(Debug, PartialEq, Eq, Clone, Error)]
#[error("Unknown dialect `{0}`, expected one of mysql or postgresql")]
pub struct UnknownDialect(String);

impl FromStr for Dialect {
    type Err = UnknownDialect;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" => Ok(Dialect::MySQL),
            "postgresql" => Ok(Dialect::PostgreSQL),
            _ => Err(UnknownDialect(s.to_owned())),
        }
    }
}

impl Dialect {
    /// All SQL dialects.
    pub const ALL: &[Self] = &[Self::MySQL, Self::PostgreSQL];

    /// Parse a SQL identifier using this Dialect
    pub fn identifier(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlIdentifier> {
        move |i| match self {
            Dialect::MySQL => map_res(
                alt((
                    preceded(
                        not(peek(sql_keyword_or_builtin_function)),
                        take_while1(is_sql_identifier),
                    ),
                    delimited(tag("`"), take_while1(|c| c != 0 && c != b'`'), tag("`")),
                    delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
                )),
                |v| str::from_utf8(&v).map(Into::into),
            )(i),
            Dialect::PostgreSQL => alt((
                map_res(
                    preceded(
                        not(map_res(peek(sql_keyword_or_builtin_function), |i| {
                            if POSTGRES_NOT_RESERVED.contains(&i.to_ascii_uppercase()[..]) {
                                Err(())
                            } else {
                                Ok(i)
                            }
                        })),
                        take_while1(is_sql_identifier),
                    ),
                    |v| {
                        str::from_utf8(&v)
                            .map(str::to_ascii_lowercase)
                            .map(Into::into)
                    },
                ),
                map_res(
                    delimited(tag("\""), take_while1(|c| c != 0 && c != b'"'), tag("\"")),
                    |v: LocatedSpan<&[u8]>| str::from_utf8(&v).map(Into::into),
                ),
            ))(i),
        }
    }

    /// Parse a SQL function identifier using this Dialect
    pub fn function_identifier(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], &str> {
        move |i| match self {
            Dialect::MySQL => map_res(
                alt((
                    preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
                    delimited(tag("`"), take_while1(is_sql_identifier), tag("`")),
                    delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
                )),
                |i| str::from_utf8(&i),
            )(i),
            Dialect::PostgreSQL => map_res(
                alt((
                    preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
                    delimited(tag("\""), take_while1(is_sql_identifier), tag("\"")),
                )),
                |i| str::from_utf8(&i),
            )(i),
        }
    }

    /// Returns the [`QuotingStyle`] for this dialect
    pub fn quoting_style(self) -> QuotingStyle {
        match self {
            Dialect::PostgreSQL => QuotingStyle::Single,
            Dialect::MySQL => QuotingStyle::SingleOrDouble,
        }
    }

    /// Returns the table/column identifier quoting character for this dialect.
    pub fn quote_identifier_char(self) -> char {
        match self {
            Self::PostgreSQL => '"',
            Self::MySQL => '`',
        }
    }

    /// Quotes the table/column identifier appropriately for this dialect.
    pub fn quote_identifier(self, ident: impl fmt::Display) -> impl fmt::Display {
        let quote = self.quote_identifier_char();
        fmty::fmt_args!("{quote}{ident}{quote}")
    }

    /// Parse the raw (byte) content of a string literal using this Dialect
    pub fn string_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
        move |i| match self {
            // Currently we allow escape sequences in all string constants. If we support postgres'
            // standard_conforming_strings setting, then the below should be changed to check for
            // the presence of a preceding 'E' instead of matching and discarding the match result.
            Dialect::PostgreSQL => preceded(
                opt(tag_no_case("E")),
                raw_string_literal(self.quoting_style()),
            )(i),
            Dialect::MySQL => preceded(
                opt(alt((tag("_utf8mb4"), tag("_utf8"), tag("_binary")))),
                raw_string_literal(self.quoting_style()),
            )(i),
        }
    }

    pub fn utf8_string_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String> {
        move |i| map_res(self.string_literal(), String::from_utf8)(i)
    }

    /// Parse the raw (byte) content of a bytes literal using this Dialect.
    // TODO(fran): Improve this. This is very naive, and for Postgres specifically, it only
    //  parses the hex-formatted byte array. We need to also add support for the escaped format.
    pub fn bytes_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
        move |i| match self {
            Dialect::PostgreSQL => raw_hex_bytes_psql(i),
            Dialect::MySQL => raw_hex_bytes_mysql(i),
        }
    }

    /// Parse the raw (byte) content of a bit vector literal using this Dialect.
    pub fn bitvec_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], BitVec> {
        move |input| match self {
            Dialect::PostgreSQL => raw_bit_vector_psql(input),
            Dialect::MySQL => Err(nom::Err::Error(NomSqlError {
                input,
                kind: nom::error::ErrorKind::Many0,
            })),
        }
    }

    /// Parses the MySQL specific `{offset}, {limit}` part in a `LIMIT` clause
    pub fn offset_limit(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitClause> {
        move |i| {
            if self == Dialect::PostgreSQL {
                return Err(nom::Err::Error(NomSqlError {
                    input: i,
                    kind: ErrorKind::Fail,
                }));
            }

            let (i, _) = whitespace0(i)?;
            let (i, offset) = literal(self)(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag_no_case(",")(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, limit) = literal(self)(i)?;

            Ok((i, LimitClause::OffsetCommaLimit { offset, limit }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod mysql {
        use super::*;
        use crate::to_nom_result;

        #[test]
        fn sql_identifiers() {
            let id1 = b"foo";
            let id2 = b"f_o_o";
            let id3 = b"foo12";
            let id4 = b":fo oo";
            let id5 = b"primary ";
            let id6 = b"`primary`";
            let id7 = b"`state-province`";
            let id8 = b"`state\0province`";

            Dialect::MySQL.identifier()(LocatedSpan::new(id1)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id2)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id3)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id4)).unwrap_err();
            Dialect::MySQL.identifier()(LocatedSpan::new(id5)).unwrap_err();
            Dialect::MySQL.identifier()(LocatedSpan::new(id6)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id7)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id8)).unwrap_err();
        }

        #[test]
        fn literal_string_single_backslash_escape() {
            let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
            for quote in [&b"'"[..], &b"\""[..]].iter() {
                let quoted = &[quote, &all_escaped[..], quote].concat();
                let res = to_nom_result(Dialect::MySQL.string_literal()(LocatedSpan::new(quoted)));
                let expected = "\0\'\"\x7F\n\r\t\x1a\\%_".as_bytes().to_vec();
                assert_eq!(res, Ok((&b""[..], expected)));
            }
        }

        #[test]
        fn literal_string_charset() {
            let res = to_nom_result(Dialect::MySQL.string_literal()(LocatedSpan::new(
                b"_utf8mb4'noria'",
            )));
            let expected = b"noria".to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn literal_string_double_quote() {
            let res = to_nom_result(Dialect::MySQL.string_literal()(LocatedSpan::new(
                br#""a""b""#,
            )));
            let expected = r#"a"b"#.as_bytes().to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn bytes_parsing() {
            let res = to_nom_result(Dialect::MySQL.bytes_literal()(LocatedSpan::new(
                b"X'0008275c6480'",
            )));
            let expected = vec![0, 8, 39, 92, 100, 128];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Empty
            let res = to_nom_result(Dialect::MySQL.bytes_literal()(LocatedSpan::new(b"X''")));
            let expected = vec![];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Malformed string
            let res = Dialect::MySQL.bytes_literal()(LocatedSpan::new(b"''"));
            res.unwrap_err();
        }
    }

    mod postgres {
        use super::*;
        use crate::to_nom_result;

        #[test]
        fn sql_identifiers() {
            let id1 = b"foo";
            let id2 = b"f_o_o";
            let id3 = b"foo12";
            let id4 = b":fo oo";
            let id5 = b"primary ";
            let id6 = b"\"primary\"";
            let id7 = b"\"state-province\"";

            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id1)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id2)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id3)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id4)).unwrap_err();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id5)).unwrap_err();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id6)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id7)).unwrap();

            Dialect::PostgreSQL.identifier()(LocatedSpan::new(b"groups")).unwrap();
        }

        #[test]
        fn sql_identifiers_case() {
            let id1 = b"FoO";
            let id2 = b"foO";
            let id3 = br#""foO""#;

            assert_eq!(
                Dialect::PostgreSQL.identifier()(LocatedSpan::new(id1))
                    .unwrap()
                    .1,
                "foo"
            );
            assert_eq!(
                Dialect::PostgreSQL.identifier()(LocatedSpan::new(id2))
                    .unwrap()
                    .1,
                "foo"
            );
            assert_eq!(
                Dialect::PostgreSQL.identifier()(LocatedSpan::new(id3))
                    .unwrap()
                    .1,
                "foO"
            );
        }

        #[test]
        fn literal_string_single_backslash_escape() {
            let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
            let quote = &b"'"[..];
            let quoted = &[quote, &all_escaped[..], quote].concat();
            let res = to_nom_result(Dialect::PostgreSQL.string_literal()(LocatedSpan::new(
                quoted,
            )));
            let expected = "\0\'\"\x7F\n\r\t\x1a\\%_".as_bytes().to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn literal_string_with_escape_character() {
            let lit = b"E'string'";
            assert_eq!(
                Dialect::PostgreSQL.string_literal()(LocatedSpan::new(lit))
                    .unwrap()
                    .1,
                b"string"
            );
        }

        #[test]
        fn bytes_parsing() {
            let res = to_nom_result(Dialect::PostgreSQL.bytes_literal()(LocatedSpan::new(
                b"E'\\\\x0008275c6480'::bytea",
            )));
            let expected = vec![0, 8, 39, 92, 100, 128];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Empty
            let res = to_nom_result(Dialect::PostgreSQL.bytes_literal()(LocatedSpan::new(
                b"E'\\\\x'::bytea",
            )));
            let expected = vec![];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Malformed string
            let res = Dialect::PostgreSQL.bytes_literal()(LocatedSpan::new(b"E'\\\\'::btea"));
            res.unwrap_err();
        }
    }
}
