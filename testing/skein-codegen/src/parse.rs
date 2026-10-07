//! Tokens, grammar and line-numbered errors for codec.md, section 3.

use core::fmt;

use crate::{Declaration, Enumeration, Field, Record, Schema, Type, Variant, check};

/// A schema refusal with its source line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub line: usize,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "line {}: {}", self.line, self.message)
    }
}

impl core::error::Error for Error {}

#[derive(Clone, Debug)]
struct Token {
    text: String,
    line: usize,
}

fn lex(source: &str) -> Result<Vec<Token>, Error> {
    let mut tokens = Vec::new();
    for (offset, line) in source.lines().enumerate() {
        let line_number = offset.checked_add(1).expect("schema line fits usize");
        let mut word = String::new();
        for character in line.chars() {
            if character == '#' {
                break;
            }
            if character.is_ascii_whitespace() || matches!(character, '{' | '}' | ':' | ',') {
                if !word.is_empty() {
                    tokens.push(Token { text: core::mem::take(&mut word), line: line_number });
                }
                if matches!(character, '{' | '}' | ':' | ',') {
                    tokens.push(Token { text: character.to_string(), line: line_number });
                }
            } else if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                word.push(character);
            } else {
                return Err(Error { line: line_number, message: format!("unexpected character {character:?}") });
            }
        }
        if !word.is_empty() {
            tokens.push(Token { text: word, line: line_number });
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    offset: usize,
    final_line: usize,
}

impl Parser {
    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.offset).map(|token| token.text.as_str())
    }

    fn line(&self) -> usize {
        self.tokens.get(self.offset).map_or(self.final_line, |token| token.line)
    }

    fn take(&mut self) -> Result<String, Error> {
        let token = self
            .tokens
            .get(self.offset)
            .ok_or_else(|| Error { line: self.final_line, message: "unexpected end of schema".into() })?;
        self.offset = self.offset.checked_add(1).expect("token offset fits usize");
        Ok(token.text.clone())
    }

    fn expect(&mut self, wanted: &str) -> Result<(), Error> {
        let line = self.line();
        let found = self.take()?;
        if found == wanted { Ok(()) } else { Err(Error { line, message: format!("expected {wanted}, found {found}") }) }
    }

    fn name(&mut self) -> Result<String, Error> {
        let line = self.line();
        let name = self.take()?;
        let mut characters = name.chars();
        let first = characters.next();
        if first.is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
            && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            Ok(name)
        } else {
            Err(Error { line, message: format!("invalid Rust name {name}") })
        }
    }

    fn bound(&mut self) -> Result<u32, Error> {
        let line = self.line();
        let value = self.take()?;
        value.parse::<u32>().map_err(|_| Error { line, message: format!("invalid bound {value}") })
    }

    fn ty(&mut self) -> Result<Type, Error> {
        let line = self.line();
        let kind = self.take()?;
        match kind.as_str() {
            "u8" => Ok(Type::U8),
            "u16" => Ok(Type::U16),
            "u32" => Ok(Type::U32),
            "u64" => Ok(Type::U64),
            "bool" => Ok(Type::Bool),
            "duration" => Ok(Type::Duration),
            "fixed" => Ok(Type::Fixed(self.bound()?)),
            "bytes" => Ok(Type::Bytes(self.bound()?)),
            "text" => Ok(Type::Text(self.bound()?)),
            "list" => {
                let count = self.bound()?;
                let item = self.ty()?;
                Ok(Type::List(count, Box::new(item)))
            }
            "option" => Ok(Type::Option(Box::new(self.ty()?))),
            "{" | "}" | ":" | "," => Err(Error { line, message: "expected a type".into() }),
            _ => Ok(Type::Named(kind)),
        }
    }

    fn record(&mut self, versioned: bool) -> Result<Declaration, Error> {
        let line = self.line();
        let name = self.name()?;
        self.expect("{")?;
        let mut fields = Vec::new();
        while self.peek() != Some("}") {
            let field_line = self.line();
            let field = self.name()?;
            self.expect(":")?;
            let ty = self.ty()?;
            fields.push(Field { line: field_line, name: field, ty });
            if self.peek() == Some(",") {
                self.take()?;
            }
        }
        self.expect("}")?;
        Ok(Declaration::Record(Record { line, name, versioned, fields }))
    }

    fn enumeration(&mut self) -> Result<Declaration, Error> {
        let line = self.line();
        let name = self.name()?;
        self.expect("{")?;
        let mut variants = Vec::new();
        while self.peek() != Some("}") {
            let variant_line = self.line();
            let variant = self.name()?;
            let record = if self.peek() == Some(":") {
                self.take()?;
                Some(self.name()?)
            } else {
                None
            };
            variants.push(Variant { line: variant_line, name: variant, record });
            if self.peek() == Some(",") {
                self.take()?;
            }
        }
        self.expect("}")?;
        Ok(Declaration::Enum(Enumeration { line, name, variants }))
    }
}

/// Parses and validates one version of a codec family.
pub fn parse(source: &str) -> Result<Schema, Error> {
    let tokens = lex(source)?;
    let mut parser = Parser { tokens, offset: 0, final_line: source.lines().count().max(1) };
    parser.expect("family")?;
    let family = parser.take()?;
    if family.is_empty()
        || !family.chars().all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(Error { line: parser.line(), message: "invalid family name".into() });
    }
    let version = {
        let line = parser.line();
        let raw = parser.take()?;
        raw.parse::<u16>().map_err(|_| Error { line, message: format!("invalid family version {raw}") })?
    };
    let mut declarations = Vec::new();
    while parser.peek().is_some() {
        let line = parser.line();
        let kind = parser.take()?;
        let declaration = match kind.as_str() {
            "record" => parser.record(false)?,
            "enum" => parser.enumeration()?,
            "versioned" => {
                parser.expect("record")?;
                parser.record(true)?
            }
            _ => {
                return Err(Error {
                    line,
                    message: format!("expected record, versioned record or enum, found {kind}"),
                });
            }
        };
        declarations.push(declaration);
    }
    let schema = Schema { family, version, declarations };
    check::check(&schema)?;
    Ok(schema)
}
