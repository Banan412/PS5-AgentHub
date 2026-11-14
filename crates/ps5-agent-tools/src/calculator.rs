use ps5_agent_core::{AgentError, Result};

const MAX_EXPR_LEN: usize = 4096;
const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    LParen,
    RParen,
    Comma,
}

pub fn eval_expression(expr: &str) -> Result<f64> {
    if expr.len() > MAX_EXPR_LEN {
        return Err(AgentError::InvalidInput(format!(
            "expression exceeds maximum length ({MAX_EXPR_LEN} bytes)"
        )));
    }
    let tokens = tokenize(expr)?;
    if tokens.is_empty() {
        return Err(AgentError::InvalidInput("empty arithmetic expression".into()));
    }
    let mut parser = Parser {
        tokens: &tokens,
        pos: 0,
        depth: 0,
    };
    let val = parser.parse_expr()?;
    if parser.pos != tokens.len() {
        return Err(AgentError::InvalidInput(format!(
            "unexpected token near position {}",
            parser.pos
        )));
    }
    if !val.is_finite() {
        return Err(AgentError::InvalidInput(
            "expression evaluated to non-finite number".into(),
        ));
    }
    Ok(val)
}

fn tokenize(expr: &str) -> Result<Vec<Token>> {
    let bytes = expr.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        if ch.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        match ch {
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '%' => {
                tokens.push(Token::Percent);
                i += 1;
            }
            '^' => {
                tokens.push(Token::Caret);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            c if c.is_ascii_digit() || c == '.' => {
                let start = i;
                let mut saw_dot = c == '.';
                let mut saw_exp = false;
                i += 1;
                while i < bytes.len() {
                    let b = bytes[i] as char;
                    if b.is_ascii_digit() {
                        i += 1;
                    } else if b == '.' && !saw_dot && !saw_exp {
                        saw_dot = true;
                        i += 1;
                    } else if (b == 'e' || b == 'E') && !saw_exp {
                        saw_exp = true;
                        i += 1;
                        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                            i += 1;
                        }
                    } else {
                        break;
                    }
                }
                let slice = &expr[start..i];
                let num: f64 = slice.parse().map_err(|_| {
                    AgentError::InvalidInput(format!("invalid numeric literal '{slice}'"))
                })?;
                tokens.push(Token::Number(num));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    let b = bytes[i] as char;
                    if b.is_ascii_alphanumeric() || b == '_' {
                        i += 1;
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Ident(expr[start..i].to_ascii_lowercase()));
            }
            other => {
                return Err(AgentError::InvalidInput(format!(
                    "unsupported character '{other}' in expression"
                )));
            }
        }
    }

    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn enter(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(AgentError::InvalidInput(
                "arithmetic expression nesting depth exceeded".into(),
            ));
        }
        Ok(())
    }

    fn exit(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    // expr := term (('+' | '-') term)*
    fn parse_expr(&mut self) -> Result<f64> {
        self.enter()?;
        let mut lhs = self.parse_term()?;
        while let Some(tok) = self.peek() {
            match tok {
                Token::Plus => {
                    self.next();
                    lhs += self.parse_term()?;
                }
                Token::Minus => {
                    self.next();
                    lhs -= self.parse_term()?;
                }
                _ => break,
            }
        }
        self.exit();
        Ok(lhs)
    }

    // term := power (('*' | '/' | '%') power)*
    fn parse_term(&mut self) -> Result<f64> {
        self.enter()?;
        let mut lhs = self.parse_power()?;
        while let Some(tok) = self.peek() {
            match tok {
                Token::Star => {
                    self.next();
                    lhs *= self.parse_power()?;
                }
                Token::Slash => {
                    self.next();
                    let rhs = self.parse_power()?;
                    if rhs == 0.0 {
                        self.exit();
                        return Err(AgentError::InvalidInput("division by zero".into()));
                    }
                    lhs /= rhs;
                }
                Token::Percent => {
                    self.next();
                    let rhs = self.parse_power()?;
                    if rhs == 0.0 {
                        self.exit();
                        return Err(AgentError::InvalidInput("modulo by zero".into()));
                    }
                    lhs %= rhs;
                }
                _ => break,
            }
        }
        self.exit();
        Ok(lhs)
    }

    // power := unary ('^' power)?  (right-associative)
    fn parse_power(&mut self) -> Result<f64> {
        self.enter()?;
        let base = self.parse_unary()?;
        let res = if matches!(self.peek(), Some(Token::Caret)) {
            self.next();
            let exp = self.parse_power()?;
            base.powf(exp)
        } else {
            base
        };
        self.exit();
        Ok(res)
    }

    // unary := ('+' | '-') unary | primary
    fn parse_unary(&mut self) -> Result<f64> {
        self.enter()?;
        let res = match self.peek() {
            Some(Token::Plus) => {
                self.next();
                self.parse_unary()?
            }
            Some(Token::Minus) => {
                self.next();
                -self.parse_unary()?
            }
            _ => self.parse_primary()?,
        };
        self.exit();
        Ok(res)
    }

    fn parse_primary(&mut self) -> Result<f64> {
        match self.next().cloned() {
            Some(Token::Number(n)) => Ok(n),
            Some(Token::LParen) => {
                let v = self.parse_expr()?;
                match self.next() {
                    Some(Token::RParen) => Ok(v),
                    _ => Err(AgentError::InvalidInput("missing closing ')'".into())),
                }
            }
            Some(Token::Ident(name)) => {
                if matches!(self.peek(), Some(Token::LParen)) {
                    self.next(); // '('
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Some(Token::RParen)) {
                        loop {
                            args.push(self.parse_expr()?);
                            match self.peek() {
                                Some(Token::Comma) => {
                                    self.next();
                                }
                                Some(Token::RParen) => break,
                                _ => {
                                    return Err(AgentError::InvalidInput(format!(
                                        "malformed argument list for function '{name}'"
                                    )))
                                }
                            }
                        }
                    }
                    self.next(); // ')'
                    eval_func(&name, &args)
                } else {
                    match name.as_str() {
                        "pi" => Ok(std::f64::consts::PI),
                        "e" => Ok(std::f64::consts::E),
                        _ => Err(AgentError::InvalidInput(format!(
                            "unknown identifier '{name}'"
                        ))),
                    }
                }
            }
            Some(tok) => Err(AgentError::InvalidInput(format!(
                "unexpected token {tok:?} in expression"
            ))),
            None => Err(AgentError::InvalidInput(
                "unexpected end of expression".into(),
            )),
        }
    }
}

fn eval_func(name: &str, args: &[f64]) -> Result<f64> {
    let one = || -> Result<f64> {
        if args.len() != 1 {
            Err(AgentError::InvalidInput(format!(
                "function '{name}' expects 1 argument"
            )))
        } else {
            Ok(args[0])
        }
    };

    match name {
        "sqrt" => {
            let x = one()?;
            if x < 0.0 {
                Err(AgentError::InvalidInput("sqrt of negative number".into()))
            } else {
                Ok(x.sqrt())
            }
        }
        "abs" => Ok(one()?.abs()),
        "sin" => Ok(one()?.sin()),
        "cos" => Ok(one()?.cos()),
        "tan" => Ok(one()?.tan()),
        "ln" => {
            let x = one()?;
            if x <= 0.0 {
                Err(AgentError::InvalidInput("ln of non-positive number".into()))
            } else {
                Ok(x.ln())
            }
        }
        "log10" => {
            let x = one()?;
            if x <= 0.0 {
                Err(AgentError::InvalidInput("log10 of non-positive number".into()))
            } else {
                Ok(x.log10())
            }
        }
        "floor" => Ok(one()?.floor()),
        "ceil" => Ok(one()?.ceil()),
        "round" => Ok(one()?.round()),
        "min" => {
            if args.is_empty() {
                return Err(AgentError::InvalidInput("min expects >= 1 argument".into()));
            }
            Ok(args.iter().copied().fold(f64::INFINITY, f64::min))
        }
        "max" => {
            if args.is_empty() {
                return Err(AgentError::InvalidInput("max expects >= 1 argument".into()));
            }
            Ok(args.iter().copied().fold(f64::NEG_INFINITY, f64::max))
        }
        _ => Err(AgentError::InvalidInput(format!(
            "unsupported math function '{name}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluates_precedence_and_functions() {
        assert_eq!(eval_expression("2 + 3 * 4").unwrap(), 14.0);
        assert_eq!(eval_expression("(2 + 3) * 4").unwrap(), 20.0);
        assert_eq!(eval_expression("2 ^ 3 ^ 2").unwrap(), 512.0);
        assert_eq!(eval_expression("-5 + abs(-3)").unwrap(), -2.0);
        assert_eq!(eval_expression("sqrt(16) + max(1, 5, 3)").unwrap(), 9.0);
    }

    #[test]
    fn rejects_div_zero_and_deep_nesting() {
        assert!(eval_expression("1 / 0").is_err());
        let deep = "(".repeat(100) + "1" + &")".repeat(100);
        assert!(eval_expression(&deep).is_err());
    }
}
