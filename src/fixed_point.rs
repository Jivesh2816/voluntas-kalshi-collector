//! Exact conversion from exchange decimal strings to the schema's fixed-point
//! integers.
//!
//! Both `price_level` and `quantity` in `challenge_orderbook.yaml` use the same
//! scale: `1_000_000` = 1 USDC or 1 contract. Exchanges send these values as
//! decimal strings (Kalshi: `"0.9600"`, `"-54.00"`), so we parse the digits
//! directly instead of going through `f64`, which cannot represent most
//! decimal fractions exactly.

/// Fixed-point scale used by every price and quantity in the output schema.
pub const SCALE: i64 = 1_000_000;
const SCALE_DIGITS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFixedError {
    pub input: String,
    pub reason: &'static str,
}

impl std::fmt::Display for ParseFixedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid fixed-point value {:?}: {}",
            self.input, self.reason
        )
    }
}

impl std::error::Error for ParseFixedError {}

/// Parses a plain decimal string (optional sign, digits, optional fraction)
/// into an integer scaled by [`SCALE`].
///
/// Fractional digits beyond the sixth are accepted only if they are zero, so
/// a value is never silently rounded. Exponents, whitespace and empty inputs
/// are rejected.
pub fn parse_fixed(input: &str) -> Result<i64, ParseFixedError> {
    let err = |reason| ParseFixedError {
        input: input.to_owned(),
        reason,
    };

    let (negative, unsigned) = match input.as_bytes().first() {
        Some(b'-') => (true, &input[1..]),
        Some(b'+') => (false, &input[1..]),
        _ => (false, input),
    };
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (unsigned, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(err("no digits"));
    }
    if !int_part
        .bytes()
        .chain(frac_part.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return Err(err("expected only digits and one decimal point"));
    }

    let (kept, dropped) = frac_part.split_at(frac_part.len().min(SCALE_DIGITS));
    if dropped.bytes().any(|b| b != b'0') {
        return Err(err("more than six significant decimal places"));
    }

    let mut value: i64 = 0;
    for b in int_part.bytes().chain(kept.bytes()) {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(b - b'0')))
            .ok_or_else(|| err("out of range"))?;
    }
    for _ in kept.len()..SCALE_DIGITS {
        value = value.checked_mul(10).ok_or_else(|| err("out of range"))?;
    }

    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kalshi_prices() {
        assert_eq!(parse_fixed("0.9600"), Ok(960_000));
        assert_eq!(parse_fixed("0.0800"), Ok(80_000));
        assert_eq!(parse_fixed("0.0001"), Ok(100));
        assert_eq!(parse_fixed("1.0000"), Ok(1_000_000));
        assert_eq!(parse_fixed("0"), Ok(0));
    }

    #[test]
    fn parses_kalshi_quantities_and_deltas() {
        assert_eq!(parse_fixed("300.00"), Ok(300_000_000));
        assert_eq!(parse_fixed("-54.00"), Ok(-54_000_000));
        assert_eq!(parse_fixed("+1.50"), Ok(1_500_000));
        assert_eq!(parse_fixed("0.01"), Ok(10_000));
        assert_eq!(parse_fixed("-0.00"), Ok(0));
    }

    #[test]
    fn accepts_partial_forms() {
        assert_eq!(parse_fixed("12"), Ok(12_000_000));
        assert_eq!(parse_fixed("12."), Ok(12_000_000));
        assert_eq!(parse_fixed(".5"), Ok(500_000));
        assert_eq!(parse_fixed("0.123456"), Ok(123_456));
        assert_eq!(parse_fixed("0.12345600"), Ok(123_456));
    }

    #[test]
    fn rejects_precision_loss() {
        assert!(parse_fixed("0.1234567").is_err());
        assert!(parse_fixed("0.00000001").is_err());
    }

    #[test]
    fn rejects_malformed_input() {
        for input in [
            "", "-", "+", ".", "-.", "1.2.3", "1e3", " 1", "1 ", "abc", "--1", "0x10", "1,000",
        ] {
            assert!(parse_fixed(input).is_err(), "{input:?} should be rejected");
        }
    }

    #[test]
    fn rejects_overflow() {
        // i64::MAX / 1_000_000 is 9_223_372_036_854.775807.
        assert_eq!(
            parse_fixed("9223372036854.775807"),
            Ok(i64::MAX),
            "largest representable value"
        );
        assert!(parse_fixed("9223372036854.775808").is_err());
        assert!(parse_fixed("99999999999999999999").is_err());
    }
}
